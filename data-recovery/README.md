# data-recovery track

Benchmarks Serverless **data recovery** restores.

## Goals

* Measure restore duration at normal size, and later at platform limits.
* Later: verify that restores change cluster state no differently from normal index creation (linear growth).

## Challenges

| Challenge | Purpose |
| --- | --- |
| `restore-baseline` | Load a Rally corpus into a source index, obtain a recovery point, restore it (`mode: rename`) and validate the restored index against the source. |

Challenges for the platform limits and for cluster state growth will be added once they are agreed.

## `restore-baseline`

1. `preflight`: checks `GET /_data_recovery/points` answers, and that the source index name and `restore_prefix` are plain names (so a bad value fails before anything is indexed).
2. `delete-source`, `create-source`, `index-source`, `refresh-source`: Rally's own `delete-index`, `create-index`, `bulk` and `refresh` operations. They (re)create the source index with the corpus mapping and load `ingest_percentage` of the corpus with `bulk_indexing_clients` clients.
3. `record-source`: reads the doc count back from the source (so it stays right with `ingest_percentage` and with bulk items Rally reported as errors) and, unless `validate_restore=false`, the sum of the corpus's checksum field.
4. `wait-for-recovery-point`: finds the earliest recovery point that started after indexing finished and has the source `complete`.
   * `recovery_point_mode=wait` (default, real deployments): points are created automatically (about every 30 minutes), so this can wait that long.
   * `recovery_point_mode=snapshot` (local deployments): creates the point by taking a snapshot into `recovery_repo`.
5. `restore-recovery`: `POST /_data_recovery/recoveries` with `mode: rename` and a unique prefix, then polls the restored index until it holds every source doc with no failed shards (404/503 and failed shards are treated as "not ready yet"; this poll is temporary until the recovery status API is deployed to QA). Starting and waiting are one task on purpose: Rally leaves a gap between tasks, so Rally's service time for this task is the restore duration. Set `use_status_api=true` to also wait on `GET /_data_recovery/recoveries/{id}`, which is still under development.
6. `validate-restore`: checks the restored index against the source: the same doc count and, for a corpus with a checksum field, the same sum of that field. A mismatch fails the race with a message saying which check failed. It is a separate task so `restore-recovery` timing stays the pure restore. With `validate_restore=false` the task is left out of the schedule and the checksum is not recorded (for data too large to aggregate over).
7. `cleanup-recovery`: deletes the restored and source indices. It never deletes snapshots: a snapshot created in `snapshot` mode (named `rally-<8 hex>`) is left in `recovery_repo`. It refuses wildcard, list and other non-plain index names. With `cleanup=false` the task is left out of the schedule (Rally fails a task whose runner makes no Elasticsearch request), so the indices stay for inspection.

Results: Rally's own timing for each task (`race.json`), where `restore-recovery` service time is the restore duration. `restore-recovery` also writes a one-line summary to the Rally log (it does not reach the console, since runners execute in a worker process): restored vs source docs, restore duration, primary shards, and the recovery point wait (reported separately, not part of the restore duration). The values the runner returns (`restore_duration_secs`, `source_docs`, `restored_docs`, `restored_primary_shards`, `recovery_point_wait_secs`) are only kept by a non-in-memory metrics store.

All recovery API calls go through `DataRecoveryClient` in `track.py` and use the Elasticsearch-side `_data_recovery` APIs. The runners share state through a module-level object, so each of the track's own operations must run with a single client (Rally's `bulk` task can use several).

## Corpora

The source data is a Rally corpus picked per run with the `corpus` track param, for example `--track-params="corpus:nyc_taxis,ingest_percentage:25"`.

| Corpus | Source index | Docs | Download | Uncompressed | Checksum field |
| --- | --- | --- | --- | --- | --- |
| `geonames` (default) | `geonames` | 11,396,503 | 0.3 GB | 3.5 GB | `population` |
| `nyc_taxis` | `nyc_taxis` | 165,346,692 | 4.8 GB | 80 GB | `passenger_count` |

`geonames` is the default so a plain run (for example against a local deployment) is quick; benchmark runs set `corpus:nyc_taxis`. `ingest_percentage` scales either down. An unknown corpus fails to load the track (the Rally log names it).

The corpus entries (`corpora/<name>.json`) and index mappings (`mappings/<name>.json`) are copied from [elastic/rally-tracks](https://github.com/elastic/rally-tracks) at `02806f0` (`geonames/` and `nyc_taxis/`). The mappings keep their Serverless handling, so the shard count is the platform default there.

To add a corpus:

1. Copy its corpus entry into `corpora/<name>.json`, with `target-index` set on every document, and its index body into `mappings/<name>.json`.
2. Add a row to `corpus_config` at the top of `track.json` with its `target` index and, optionally, a `checksum_field`. The checksum is compared exactly, so it must be an integer field (a sum of floating point values can change with summation order). Without one, validation compares doc counts only.
3. Add it to the table above. A unit test renders the track for every corpus and checks the files, names and checksum field type.

## Track parameters

| Setting | Default | Description |
| --- | --- | --- |
| `corpus` | `geonames` | Corpus to load into the source index, see [Corpora](#corpora). The source index is deleted and recreated. |
| `ingest_percentage` | `100` | Percentage of the corpus to load. |
| `bulk_size` | `5000` | Docs per bulk request. |
| `bulk_indexing_clients` | `8` | Clients loading the corpus. |
| `recovery_point_mode` | `wait` | `wait` for automatic points, `snapshot` to create one via a snapshot (local only). |
| `recovery_repo` | `data-recovery` | Snapshot repository used in `snapshot` mode (the repository name on QA). A local cluster may use a different name, e.g. `recovery_repo:recovery-repo`. |
| `recovery_point_timeout_secs` | `2400` | How long to wait for a usable recovery point. |
| `poll_interval_secs` | `10` | Poll interval while waiting for a recovery point. |
| `restore_prefix` | `rally-restore-` | Prefix for restored names; a random suffix is added per run. |
| `recovery_timeout_secs` | `600` | How long to wait for the restore to finish. |
| `restore_poll_interval_secs` | `2` | Poll interval while waiting for the restore. |
| `use_status_api` | `false` | Also wait on the recovery status API (under development). |
| `validate_restore` | `true` | Compare the restored index with the source (doc count and checksum). |
| `cleanup` | `true` | Delete the restored and source indices at the end. Snapshots are never deleted. |

The defaults are defined in `track.json` (`corpus`) and `challenges/restore-baseline.json`. The runners require their params and do not repeat the defaults, and a unit test checks that this table matches the rendered track.
