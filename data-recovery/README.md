# data-recovery track

Benchmarks Serverless **data recovery** restores.

## Goals

* Measure restore duration at normal size, and later at platform limits.
* Later: verify that restores change cluster state no differently from normal index creation (linear growth).

## Challenges

| Challenge | Purpose |
| --- | --- |
| `restore-baseline` | Load a Rally corpus into a source index or data stream, obtain a recovery point, restore it (`mode: rename`) and validate the restored index against the source. |

Challenges for the platform limits and for cluster state growth will be added once they are agreed.

## `restore-baseline`

1. `preflight`: checks `GET /_data_recovery/points` answers, and that the source name and `restore_prefix` are plain names (so a bad value fails before anything is indexed).
2. `delete-source`, `create-source`, `index-source`, `refresh-source`: Rally's own `delete-index` and `create-index` (or `delete-data-stream` and `create-data-stream`), `bulk` and `refresh` operations. They (re)create the source with the corpus mapping (an index) or the cluster's matching templates (a data stream) and load `ingest_percentage` of the corpus with `bulk_indexing_clients` clients.
3. `record-source`: reads the doc count back from the source (so it stays right with `ingest_percentage` and with bulk items Rally reported as errors) and, unless `validate_restore=false`, the sum of the corpus's checksum field. It fails if that field is not mapped as an integer type.
4. `wait-for-recovery-point`: finds the earliest recovery point that started after indexing finished and has the source `complete`.
   * `recovery_point_mode=wait` (default, real deployments): points are created automatically (about every 30 minutes), so this can wait that long.
   * `recovery_point_mode=snapshot` (local deployments): creates the point by taking a snapshot into `recovery_repo`, and uses the point that starts with that snapshot (by the cluster's clock, so a load driver whose clock differs from the cluster's does not skip it).
5. `restore-recovery`: `POST /_data_recovery/recoveries` with `mode: rename` and a unique name: `restore_prefix` plus a random part, as a prefix for an index and as a suffix for a data stream (see [Data streams](#data-streams)). It then polls the restored copy until it holds every source doc with no failed shards (404/503 and failed shards are treated as "not ready yet"; this poll is temporary until the recovery status API is deployed to QA). Starting and waiting are one task on purpose: Rally leaves a gap between tasks, so Rally's service time for this task is the restore duration. Set `use_status_api=true` to also wait on `GET /_data_recovery/recoveries/{id}`, which is still under development.
6. `validate-restore`: checks the restored copy against the source: the same doc count and, for a corpus with a checksum field, the same sum of that field. A mismatch fails the race with a message saying which check failed. It is a separate task so `restore-recovery` timing stays the pure restore. With `validate_restore=false` the task is left out of the schedule and the checksum is not recorded (for data too large to aggregate over).
7. `cleanup-recovery`: deletes the restored copy and the source (with `DELETE /_data_stream/...` for data streams). It never deletes snapshots: a snapshot created in `snapshot` mode (named `rally-<8 hex>`) is left in `recovery_repo`. It refuses wildcard, list and other non-plain index names. With `cleanup=false` the task is left out of the schedule (Rally fails a task whose runner makes no Elasticsearch request), so the indices stay for inspection.

Results: Rally's own timing for each task (`race.json`), where `restore-recovery` service time is the restore duration. `restore-recovery` also writes a one-line summary to the Rally log (it does not reach the console, since runners execute in a worker process): restored vs source docs, restore duration, primary shards, and the recovery point wait (reported separately, not part of the restore duration). The values the runner returns (`restore_duration_secs`, `source_docs`, `restored_docs`, `restored_primary_shards`, `recovery_point_wait_secs`) are only kept by a non-in-memory metrics store.

All recovery API calls go through `DataRecoveryClient` in `track.py` and use the Elasticsearch-side `_data_recovery` APIs. The runners share state through a module-level object, so each of the track's own operations must run with a single client (Rally's `bulk` task can use several).

## Corpora

The source data is a Rally corpus picked per run with the `corpus` track param, for example `--track-params="corpus:nyc_taxis,ingest_percentage:25"`. A corpus loads into an index or a data stream.

| Corpus | Source | Docs | Download | Uncompressed | Checksum field |
| --- | --- | --- | --- | --- | --- |
| `geonames` (default) | index `geonames` | 11,396,503 | 0.3 GB | 3.5 GB | `population` |
| `nyc_taxis` | index `nyc_taxis` | 165,346,692 | 4.8 GB | 80 GB | `passenger_count` |
| `big5` | data stream `logs-benchmark-dev` | 1,171,840,430 | 60 GB | 1.08 TB | `metrics.size` |
| `endpoint_file` | data stream `logs-endpoint.events.file-default` | 4,315,173 | 0.35 GB | ~11.5 GB (estimated) | `event.sequence` |

`geonames` is the default so a plain run (for example against a local deployment) is quick; benchmark runs set `corpus:nyc_taxis` or `corpus:big5`. `endpoint_file` is a small data stream corpus for local runs. `ingest_percentage` scales how much is indexed, but not what is downloaded and decompressed: Rally always prepares every file of the corpus in full, so `big5` needs about 1.1 TB of disk on the load driver whatever the percentage. An unknown corpus fails to load the track (the Rally log names it).

The corpus entries (`corpora/<name>.json`) and index mappings (`mappings/<name>.json`) are copied from [elastic/rally-tracks](https://github.com/elastic/rally-tracks) at `02806f0` (`geonames/`, `nyc_taxis/`, `big5/`, and the `endpoint-events-file` corpus of `elastic/security/`, whose entry lists no uncompressed size). The mappings keep their Serverless handling, so the shard count is the platform default there.

### Data streams

A data stream corpus has no mapping file: its data stream is created from the cluster's index templates that match its name. `big5` loads into `logs-benchmark-dev` and `endpoint_file` into `logs-endpoint.events.file-default`, which both match the built-in `logs-*-*` template (logsdb, the platform's lifecycle). The `big5` track's own template is not used, since it sets ILM, shard counts and other settings that Serverless does not accept. So the mapping, and the size on disk, differ from a `big5` race on a stateful cluster.

A restored data stream needs an index template that matches its new name, or the restore is refused. The restore therefore adds `restore_prefix` and a random part as a **suffix** for a data stream (e.g. `logs-benchmark-dev-rally-restore-1a2b3c`), which still matches `logs-*-*`, and as a prefix for an index. A large load may roll the data stream over, so the restore covers every backing index.

### Adding a corpus

1. Copy its corpus entry into `corpora/<name>.json`, with `target-index` (or `target-data-stream`) set on every document. For an index, copy its index body into `mappings/<name>.json`.
2. Add a row to `corpus_config` at the top of `track.json` with its `target`, its `target_type` (`index` or `data_stream`) and, optionally, a `checksum_field`. The checksum is compared exactly, so it must be an integer field (a sum of floating point values can change with summation order); `record-source` checks the mapped type. Without one, validation compares doc counts only.
3. Add it to the table above. A unit test renders the track for every corpus and checks the files, names, setup operations and, for an index, the checksum field type in the mapping.

## Track parameters

| Setting | Default | Description |
| --- | --- | --- |
| `corpus` | `geonames` | Corpus to load into the source index or data stream, see [Corpora](#corpora). The source is deleted and recreated. |
| `ingest_percentage` | `100` | Percentage of the corpus to load. |
| `bulk_size` | `5000` | Docs per bulk request. |
| `bulk_indexing_clients` | `8` | Clients loading the corpus. |
| `recovery_point_mode` | `wait` | `wait` for automatic points, `snapshot` to create one via a snapshot (local only). |
| `recovery_repo` | `data-recovery` | Snapshot repository used in `snapshot` mode (the repository name on QA). A local cluster may use a different name, e.g. `recovery_repo:recovery-repo`. |
| `recovery_point_timeout_secs` | `2400` | How long to wait for a usable recovery point. |
| `poll_interval_secs` | `10` | Poll interval while waiting for a recovery point. |
| `restore_prefix` | `rally-restore-` | Tag for restored names, with a random part added per run: a prefix for an index, a suffix for a data stream. |
| `recovery_timeout_secs` | `600` | How long to wait for the restore to finish. |
| `restore_poll_interval_secs` | `2` | Poll interval while waiting for the restore. |
| `use_status_api` | `false` | Also wait on the recovery status API (under development). |
| `validate_restore` | `true` | Compare the restored copy with the source (doc count and checksum). |
| `cleanup` | `true` | Delete the restored copy and the source at the end. Snapshots are never deleted. |

The defaults are defined in `track.json` (`corpus`) and `challenges/restore-baseline.json`. The runners require their params and do not repeat the defaults, and a unit test checks that this table matches the rendered track.
