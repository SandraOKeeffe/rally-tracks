# snapshot-recovery track

Benchmarks Serverless **data recovery** restores and checks how restores affect cluster state.

Status: `restore-baseline` is implemented (MVP). The other challenges and `measure-cluster-state` are still stubs.

## Goals

* Measure restore duration at normal size and at platform limits.
* Verify that restores change cluster state no differently from normal index creation (linear growth).

## Challenges

| Challenge | Status | Purpose |
| --- | --- | --- |
| `restore-baseline` | implemented | Index a small source index, obtain a recovery point, restore it (`mode: rename`) and verify the restored doc count. |
| `max-shard-size` | stub | Restore sources whose shards are at the platform maximum size. |
| `max-shards-per-node` | stub | Restore enough shards to reach the per-node shard limit. |
| `max-concurrent-restores` | stub | Run restores in parallel up to the concurrent-restore limit. |
| `max-concurrent-restores-many-shards` | stub | Maximum concurrent restores with many shards each; exercises the cluster state limit. |
| `restore-cluster-state` | stub | Stepped restore comparing cluster state growth against creating equivalent indices directly. |

## `restore-baseline`

1. `data-recovery-preflight`: checks `GET /_data_recovery/points` answers.
2. `index-source-docs`: (re)creates the source index and bulk indexes `doc_count` generated docs. It then reads the doc count back from the source and fails if it differs from `doc_count`, and (unless `validate_restore=false`) records a content checksum: the sum of the generated `n` field.
3. `wait-for-recovery-point`: finds the earliest recovery point that started after indexing finished and has the source `complete`.
   * `recovery_point_mode=wait` (default, real deployments): points are created automatically (about every 30 minutes), so this can wait that long.
   * `recovery_point_mode=snapshot` (local deployments): creates the point by taking a snapshot into `recovery_repo`.
4. `restore-recovery`: `POST /_data_recovery/recoveries` with `mode: rename` and a unique prefix, then polls the restored index until it holds every source doc (404/503 are treated as "not ready yet"). Starting and waiting are one task on purpose: Rally leaves a gap between tasks, so Rally's service time for this task is the restore duration. Set `use_status_api=true` to also wait on `GET /_data_recovery/recoveries/{id}`, which is still under development.
5. `validate-restore`: checks the restored index against the source: the same doc count and the same content checksum. A mismatch fails the race with a message saying which check failed. It is a separate task so `restore-recovery` timing stays the pure restore. With `validate_restore=false` the task is left out of the schedule and the checksum is not recorded (for data too large to aggregate over).
6. `cleanup-recovery`: deletes the restored and source indices. It never deletes snapshots: a snapshot created in `snapshot` mode (named `rally-<8 hex>`) is left in `recovery_repo`. It refuses wildcard, list and other non-plain index names. With `cleanup=false` the task is left out of the schedule (Rally fails a task whose runner makes no Elasticsearch request), so the indices stay for inspection.

Results: Rally's own timing for each task (`race.json`), where `restore-recovery` service time is the restore duration. `restore-recovery` also writes a one-line summary to the Rally log (it does not reach the console, since runners execute in a worker process): restored vs source docs, restore duration, primary shards, and the recovery point wait (reported separately, not part of the restore duration). The values the runner returns (`restore_duration_secs`, `source_docs`, `restored_docs`, `restored_primary_shards`, `recovery_point_wait_secs`) are only kept by a non-in-memory metrics store.

All recovery API calls go through `DataRecoveryClient` in `track.py` and use the Elasticsearch-side `_data_recovery` APIs. The runners share state through a module-level object, so each operation must run with a single client.

## Running locally

Point Rally at a local Serverless Elasticsearch with `_data_recovery` enabled, with operator credentials:

```
esrally race --track-path=<path>/snapshot-recovery --challenge=restore-baseline \
  --pipeline=benchmark-only --target-hosts=localhost:9200 \
  --client-options=<client-options-file> --track-params="recovery_point_mode:snapshot"
```

## Track parameters

| Setting | Default | Description |
| --- | --- | --- |
| `source_index` | `recovery-source` | Name of the source index. It is deleted and recreated. |
| `doc_count` | `100` | Number of generated docs to index. |
| `bulk_size` | `1000` | Bulk request size. |
| `recovery_point_mode` | `wait` | `wait` for automatic points, `snapshot` to create one via a snapshot (local only). |
| `recovery_repo` | `data-recovery` | Snapshot repository used in `snapshot` mode (the repository name on QA). A local cluster may use a different name, e.g. `recovery_repo:recovery-repo`. |
| `recovery_point_timeout_secs` | `2400` | How long to wait for a usable recovery point. |
| `poll_interval_secs` | `10` | Poll interval while waiting for a recovery point. |
| `restore_prefix` | `rally-restore-` | Prefix for restored names; a random suffix is added per run. |
| `recovery_timeout_secs` | `600` | How long to wait for the restore to finish. |
| `restore_poll_interval_secs` | `2` | Poll interval while waiting for the restore. |
| `use_status_api` | `false` | Also wait on the recovery status API (under development). |
| `validate_restore` | `true` | Run the content checksum comparison between the source and the restored index. |
| `cleanup` | `true` | Delete the restored and source indices at the end. Snapshots are never deleted. |
| `fail_on_recovery_failure` | `true` | Reserved for the other challenges. |
| `state_tolerance` | `0.1` | Reserved for `restore-cluster-state`. |
| `step_size` | `10` | Reserved for `restore-cluster-state`. |
| `max_sources` | `100` | Reserved for `restore-cluster-state`. |
| `recovery_point_max_age_minutes` | `60` | Reserved, currently unused. |
