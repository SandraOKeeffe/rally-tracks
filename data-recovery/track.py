"""Serverless data recovery restore track.

The ``restore-baseline`` challenge loads a Rally corpus (picked with the ``corpus`` track param) into a
source index or data stream with Rally's own bulk operation, obtains a recovery point (created via a
snapshot locally, or waited for on a real deployment), restores it with ``mode: rename`` and validates
the restored copy against the source.

Defaults live in the challenge definition only: the runners require their params (see
``required_params``) and fail with a clear message if one is missing.

State is shared between the runners of a race through ``STATE``, so the track assumes a single
client per operation (the default for the tasks in ``restore-baseline``). A runner whose inputs
were not set by an earlier task fails with a clear message.
"""

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

from esrally.utils import console

logger = logging.getLogger(__name__)

# Statuses returned while a restored index is still being created / allocated.
TRANSIENT_STATUSES = (404, 503)

# What a corpus loads into, as named in ``corpus_config`` in track.json and in ``resolved_sources``.
TARGET_TYPES = ("index", "data_stream")

# Field types whose sum is exact, so it can be compared between the source and the restored copy.
INTEGER_FIELD_TYPES = ("long", "integer", "short", "byte")


def _unwrap(response):
    """Return the parsed JSON body of a response, whether it is an ApiResponse or a plain dict."""
    body = getattr(response, "body", response)
    if isinstance(body, (bytes, str)):
        body = json.loads(body) if body else {}
    return body


def _status(exc):
    return getattr(exc, "status_code", None) or getattr(exc, "status", None)


RESTORING_ERROR_TYPE = "shard_restoring_exception"


def _is_restoring_error(exc):
    """True if the error says the index is currently being restored from a snapshot.

    The error type has to be read from the structured response body: ``str(ApiError)`` only contains
    the root cause *reason*, never its *type*, so matching ``shard_restoring_exception`` in ``str(exc)``
    never matches a real error. The reason text is checked as a fallback.
    """
    body = getattr(exc, "body", None)
    if isinstance(body, (bytes, str)):
        try:
            body = json.loads(body)
        except ValueError:
            body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        causes = [error, *(error.get("root_cause") or [])]
        causes += [failed.get("reason") for failed in error.get("failed_shards") or [] if isinstance(failed, dict)]
        if any(isinstance(cause, dict) and cause.get("type") == RESTORING_ERROR_TYPE for cause in causes):
            return True
    return "currently being restored from a snapshot" in str(exc)


def _is_not_ready(exc):
    """True while a restored index is still being created / allocated.

    That is a 404 or 503, or a 403 ``shard_restoring_exception`` ("currently being restored from a
    snapshot and is temporarily unavailable"). Other 403s are real permission errors and are not retried.
    """
    return _status(exc) in TRANSIENT_STATUSES or _is_restoring_error(exc)


def _parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utcnow():
    return datetime.now(timezone.utc)


def _report(message):
    """Report a result in the Rally log (not kept by the in-memory metrics store).

    Runners execute in a worker process, so the console output does not reach the terminal there.
    """
    console.info(message)
    logger.info(message)


# Names are interpolated into request paths, so only accept characters that cannot change which resource a
# request addresses: no wildcards, lists, path or URL metacharacters (``% # ? / ,``) and no leading ``_ - + .``
# (which also keeps out ``_all`` and system indices).
_PLAIN_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _require_plain_name(name, kind):
    """Refuse a name that could change which resource a request addresses (see ``_PLAIN_NAME``)."""
    if not isinstance(name, str) or not _PLAIN_NAME.fullmatch(name):
        raise ValueError(
            f"[{name}] is not a plain {kind} name: it must start with a letter or digit and only contain letters, digits, . _ -"
        )
    return name


def _require_plain_index_name(name):
    """Refuse anything that could make a DELETE hit more than the single index this track created."""
    return _require_plain_name(name, "index")


class DataRecoveryClient:
    """Single place for all data recovery API calls.

    Calls the Elasticsearch ``_data_recovery`` APIs directly (``points``, ``sources``,
    ``recoveries``) with the credentials Rally is configured with. Callers must only go
    through this class so the backing API can change later.
    """

    def __init__(self, es):
        self._es = es

    async def _request(self, method, path, params=None, body=None, headers=None):
        response = await self._es.perform_request(method=method, path=path, params=params, body=body, headers=headers)
        return _unwrap(response)

    async def list_recovery_points(self, size=None):
        """Recovery points, newest first."""
        params = {"size": size} if size else None
        body = await self._request("GET", "/_data_recovery/points", params=params)
        return body.get("recovery_points", [])

    async def list_sources(self, recovery_point_end_time, expression="*"):
        # Does not follow ``has_more``: callers pass an exact name or a small expression.
        params = {"expression": expression, "recovery_point_end_time": recovery_point_end_time}
        return await self._request("GET", "/_data_recovery/sources", params=params)

    async def start_recovery(self, recovery_point_end_time, sources, mode="rename", prefix=None, suffix=None):
        body = {"recovery_point_end_time": recovery_point_end_time, "sources": list(sources), "mode": mode}
        strategy = {}
        if prefix:
            strategy["prefix"] = prefix
        if suffix:
            strategy["suffix"] = suffix
        if strategy:
            body["rename_strategy"] = strategy
        return await self._request("POST", "/_data_recovery/recoveries", body=body)

    async def get_recovery(self, recovery_id):
        # The status endpoint is under development and rejected ids on the local build.
        return await self._request("GET", f"/_data_recovery/recoveries/{recovery_id}")

    # Local deployments only: a recovery point is created by taking a snapshot into the recovery repository.
    async def create_snapshot(self, repository, name, indices):
        body = {"indices": indices, "include_global_state": False}
        return await self._request("PUT", f"/_snapshot/{repository}/{name}", params={"wait_for_completion": "true"}, body=body)

    async def count(self, index):
        body = await self._request("GET", f"/{index}/_count")
        return body["count"]

    # TEMPORARY, with ``RestoreRecovery._wait_for_docs``: remove both once the recovery status API is deployed to QA.
    async def count_with_shards(self, index):
        """Doc count plus the number of shards that answered and the number that failed to."""
        body = await self._request("GET", f"/{index}/_count")
        return body["count"], body["_shards"]["total"], body["_shards"]["failed"]

    async def field_types(self, target, field):
        """Mapped types of a field across the indices behind ``target`` (an index or a data stream).

        Uses field capabilities, since the field mapping API is not available on Serverless.
        """
        body = await self._request("GET", f"/{target}/_field_caps", params={"fields": field})
        return set((body.get("fields") or {}).get(field, {}))

    async def sum_field(self, index, field):
        """Sum of a numeric field, used as a cheap content checksum."""
        body = {"size": 0, "aggs": {"total": {"sum": {"field": field}}}}
        result = await self._request("POST", f"/{index}/_search", body=body)
        return result["aggregations"]["total"]["value"]


class _RunState:
    """State handed from one runner to the next within a race."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.source_index = None
        self.expected_docs = None
        self.source_type = None
        self.checksum_field = None
        self.expected_checksum = None
        self.indexed_at = None
        self.recovery_point_end_time = None
        self.recovery_point_wait_secs = None
        self.recovery_id = None
        self.destination = None


STATE = _RunState()


def _require_state(runner, *fields):
    """Fail clearly if an earlier task has not set the values this runner needs from ``STATE``."""
    missing = [field for field in fields if getattr(STATE, field) is None]
    if missing:
        raise RuntimeError(
            f"[{runner}] needs {', '.join(missing)} from an earlier task. "
            "Run the restore-baseline tasks in order, each with a single client."
        )


class _Runner:
    name = None
    # Params the challenge definition must provide; it is the single source of defaults.
    required_params = ()

    def _require_params(self, params):
        missing = [key for key in self.required_params if key not in params]
        if missing:
            raise KeyError(f"missing required param(s) {missing}; they are set in the challenge definition")

    def __repr__(self, *args, **kwargs):
        return self.name


class DataRecoveryPreflight(_Runner):
    name = "data-recovery-preflight"
    required_params = ("source_index", "restore_prefix")

    async def __call__(self, es, params):
        self._require_params(params)
        # Fail on bad names now, not after indexing and waiting for a recovery point.
        _require_plain_index_name(params["source_index"])
        _require_plain_name(params["restore_prefix"], "restore prefix")
        STATE.reset()
        points = await DataRecoveryClient(es).list_recovery_points(size=1)
        return {"weight": 1, "unit": "ops", "recovery_points_available": len(points)}


class RecordSource(_Runner):
    """Record what the restore is checked against, once Rally's bulk task has loaded the source.

    The source is an index or a data stream (``target_type``). The expected doc count is read back from
    it rather than taken from the corpus, so it stays right with ``ingest_percentage`` and with bulk
    items Rally reported as errors. ``checksum_field`` is the corpus's integer field whose sum is
    compared after the restore (a sum over floating point values can differ with summation order); its
    mapped type is checked here, since a data stream corpus gets its mapping from the cluster's
    templates. Without one only the doc count is validated.
    """

    name = "record-source"
    required_params = ("index", "target_type", "checksum_field")

    async def __call__(self, es, params):
        self._require_params(params)
        index = _require_plain_index_name(params["index"])
        target_type = params["target_type"]
        if target_type not in TARGET_TYPES:
            raise ValueError(f"target_type must be one of {list(TARGET_TYPES)}, got [{target_type}]")
        checksum_field = params["checksum_field"]
        if checksum_field is not None:
            _require_plain_name(checksum_field, "field")
        client = DataRecoveryClient(es)

        docs = await client.count(index)
        if docs < 1:
            raise RuntimeError(f"[{index}] holds no docs after loading the corpus")
        expected_checksum = None
        if checksum_field:
            types = await client.field_types(index, checksum_field)
            if not types or not types <= set(INTEGER_FIELD_TYPES):
                raise RuntimeError(
                    f"checksum field [{checksum_field}] of [{index}] is mapped as {sorted(types) or 'nothing'}, "
                    f"expected one of {list(INTEGER_FIELD_TYPES)}"
                )
            expected_checksum = await client.sum_field(index, checksum_field)

        STATE.source_index = index
        STATE.source_type = target_type
        STATE.expected_docs = docs
        STATE.checksum_field = checksum_field
        STATE.expected_checksum = expected_checksum
        # Recovery points must start after this to contain every doc.
        STATE.indexed_at = _utcnow()
        _report(
            f"Source {target_type} [{index}] holds {docs} docs"
            + (f", checksum of [{checksum_field}] {expected_checksum}" if checksum_field else "")
        )
        return {"weight": 1, "unit": "ops", "source_index": index, "source_type": target_type, "source_docs": docs}


class WaitForRecoveryPoint(_Runner):
    name = "wait-for-recovery-point"
    required_params = ("recovery_point_mode", "recovery_repo", "timeout_secs", "poll_interval_secs")

    async def __call__(self, es, params):
        self._require_params(params)
        _require_state(self.name, "source_index", "indexed_at")
        mode = params["recovery_point_mode"]
        timeout = float(params["timeout_secs"])
        interval = float(params["poll_interval_secs"])
        client = DataRecoveryClient(es)
        started = time.monotonic()

        # Snapshots are left in place: the track never deletes them.
        snapshot_name = None
        # Earliest start time a usable point can have. In wait mode that is when the source was recorded, by
        # this host's clock. In snapshot mode it is the start of the snapshot taken here, by the cluster's
        # clock: it can be only milliseconds after recording, less than the two clocks can differ.
        not_before = STATE.indexed_at
        if mode == "snapshot":
            repository = _require_plain_name(params["recovery_repo"], "repository")
            snapshot_name = f"rally-{uuid.uuid4().hex[:8]}"
            body = await client.create_snapshot(repository, snapshot_name, STATE.source_index)
            not_before = datetime.fromtimestamp(body["snapshot"]["start_time_in_millis"] / 1000, timezone.utc)
        elif mode != "wait":
            raise ValueError(f"recovery_point_mode must be 'wait' or 'snapshot', got [{mode}]")

        while True:
            point = await self._usable_point(client, not_before)
            if point is not None:
                break
            if time.monotonic() - started > timeout:
                raise RuntimeError(f"no recovery point covering [{STATE.source_index}] after {timeout}s")
            await asyncio.sleep(interval)

        STATE.recovery_point_end_time = point["end_time"]
        STATE.recovery_point_wait_secs = time.monotonic() - started
        return {
            "weight": 1,
            "unit": "ops",
            "recovery_point_end_time": point["end_time"],
            "recovery_point_wait_secs": STATE.recovery_point_wait_secs,
            "snapshot": snapshot_name,
        }

    @staticmethod
    async def _usable_point(client, not_before):
        """Earliest recovery point that started at or after ``not_before`` and has the source complete."""
        points = await client.list_recovery_points()
        candidates = [p for p in points if _parse_ts(p["start_time"]) >= not_before]
        for point in sorted(candidates, key=lambda p: _parse_ts(p["start_time"])):
            body = await client.list_sources(point["end_time"], STATE.source_index)
            sources = body.get("sources", [])
            if sources and all(s.get("complete") for s in sources):
                return point
        return None


class RestoreRecovery(_Runner):
    """Start the restore and wait for it to finish in one task.

    Rally adds a gap between tasks, so doing both in one runner makes Rally's service time for this
    task the restore duration (from the start request until the restored index holds every doc).
    """

    name = "restore-recovery"
    required_params = ("restore_prefix", "timeout_secs", "poll_interval_secs", "use_status_api")

    async def __call__(self, es, params):
        self._require_params(params)
        _require_state(self.name, "source_index", "source_type", "expected_docs", "recovery_point_end_time", "recovery_point_wait_secs")
        timeout = float(params["timeout_secs"])
        interval = float(params["poll_interval_secs"])
        use_status_api = bool(params["use_status_api"])
        # Unique per run so restores never collide and cleanup can target them.
        tag = f"{params['restore_prefix']}{uuid.uuid4().hex[:6]}"
        prefix, suffix = self._rename(tag)
        # Cleanup refuses names that are not plain, so check the name the restore will create before starting it.
        _require_plain_index_name(f"{prefix or ''}{STATE.source_index}{suffix or ''}")
        client = DataRecoveryClient(es)

        started = time.monotonic()
        body = await client.start_recovery(STATE.recovery_point_end_time, [STATE.source_index], mode="rename", prefix=prefix, suffix=suffix)
        STATE.recovery_id = body["recovery_id"]
        resolved = body["resolved_sources"][0]
        if resolved.get("type", STATE.source_type) != STATE.source_type:
            raise RuntimeError(f"[{STATE.source_index}] was resolved as a [{resolved['type']}], expected a [{STATE.source_type}]")
        STATE.destination = _require_plain_index_name(resolved["destination"])

        # One timeout covers the whole restore, whichever way completion is detected.
        if use_status_api:
            await self._wait_with_status_api(client, started, timeout, interval)
        # The last request of the timed restore is the successful poll, so nothing else adds to the duration.
        restored_docs, primary_shards = await self._wait_for_docs(client, started, timeout, interval)
        restore_duration = time.monotonic() - started

        _report(
            f"Restore of [{STATE.source_index}] to [{STATE.destination}] complete: "
            f"{restored_docs}/{STATE.expected_docs} docs in {restore_duration:.2f}s, {primary_shards} primary shards, "
            f"recovery point wait {STATE.recovery_point_wait_secs:.2f}s"
        )
        return {
            "weight": 1,
            "unit": "ops",
            "recovery_id": STATE.recovery_id,
            "destination": STATE.destination,
            "restore_duration_secs": restore_duration,
            "source_docs": STATE.expected_docs,
            "restored_docs": restored_docs,
            "restored_primary_shards": primary_shards,
            "recovery_point_wait_secs": STATE.recovery_point_wait_secs,
        }

    @staticmethod
    def _rename(tag):
        """``(prefix, suffix)`` that give the restored copy a unique name.

        An index gets the tag as a prefix. A restored data stream needs an index template that matches
        its new name (the restore is refused otherwise), so it gets the tag as a suffix instead: that
        keeps it matching the same templates as the source, e.g. ``logs-*-*``.
        """
        if STATE.source_type == "data_stream":
            return None, f"-{tag}"
        return f"{tag}-", None

    @staticmethod
    async def _wait_with_status_api(client, started, timeout, interval):
        deadline = started + timeout
        while True:
            status = (await client.get_recovery(STATE.recovery_id)).get("status")
            if status == "completed":
                return
            if status == "failed":
                raise RuntimeError(f"recovery [{STATE.recovery_id}] failed")
            if time.monotonic() > deadline:
                raise RuntimeError(f"recovery [{STATE.recovery_id}] still [{status}] after {timeout}s")
            await asyncio.sleep(interval)

    @staticmethod
    async def _wait_for_docs(client, started, timeout, interval):
        """Poll the restored index until it holds every source doc and no shard failed.

        Returns ``(doc count, primary shard count)``.

        TEMPORARY: this poll stands in for the recovery status API (see ``use_status_api``) until that API
        is deployed to QA. Once it is, completion should come from the status API and this poll, together
        with ``DataRecoveryClient.count_with_shards``, can go.

        A 404, a 503 or a ``shard_restoring_exception`` 403 mean it is not ready yet (see ``_is_not_ready``).
        """
        deadline = started + timeout
        last = None
        failed = None
        while True:
            try:
                last, shards, failed = await client.count_with_shards(STATE.destination)
            except Exception as e:
                if not _is_not_ready(e):
                    raise
            else:
                # Some failed shards are expected while the restore is still in progress, so they mean
                # "not ready, keep polling", not an error. They are only reported if the deadline passes.
                if last == STATE.expected_docs and failed == 0:
                    return last, shards
                if failed:
                    logger.debug("[%s] has %s docs and %s of %s shards failed, still polling", STATE.destination, last, failed, shards)
                if last > STATE.expected_docs:
                    raise RuntimeError(f"[{STATE.destination}] has {last} docs, expected {STATE.expected_docs}")
            if time.monotonic() > deadline:
                if last is None:
                    raise RuntimeError(f"[{STATE.destination}] was not queryable after {timeout}s")
                raise RuntimeError(
                    f"[{STATE.destination}] has {last} docs and {failed} failed shards after {timeout}s, "
                    f"expected {STATE.expected_docs} docs and 0 failed shards"
                )
            await asyncio.sleep(interval)


class ValidateRestore(_Runner):
    """Check the restored index against the source: same doc count and, when the corpus has a
    checksum field, the same sum of that field.

    Left out of the schedule with ``validate_restore=false`` (for example for data too large to
    aggregate over). It must not be scheduled as a no-op: Rally times a task from the Elasticsearch
    requests it makes, so a runner that makes none fails the task. Kept out of ``restore-recovery``
    so that task's timing is only the restore itself.
    """

    name = "validate-restore"

    async def __call__(self, es, params):
        _require_state(self.name, "source_index", "destination", "expected_docs")
        client = DataRecoveryClient(es)
        restored_docs = await client.count(STATE.destination)
        if restored_docs != STATE.expected_docs:
            raise RuntimeError(
                f"validation failed: [{STATE.destination}] has {restored_docs} docs, source [{STATE.source_index}] had {STATE.expected_docs}"
            )
        if STATE.checksum_field is None:
            _report(f"Validation passed: [{STATE.destination}] matches [{STATE.source_index}] ({restored_docs} docs, no checksum field)")
            return {"weight": 1, "unit": "ops", "validated": True, "restored_docs": restored_docs, "checksum": None}
        _require_state(self.name, "expected_checksum")
        restored_checksum = await client.sum_field(STATE.destination, STATE.checksum_field)
        if restored_checksum != STATE.expected_checksum:
            raise RuntimeError(
                f"validation failed: [{STATE.destination}] content checksum {restored_checksum} differs from "
                f"source [{STATE.source_index}] checksum {STATE.expected_checksum}"
            )
        _report(
            f"Validation passed: [{STATE.destination}] matches [{STATE.source_index}] ({restored_docs} docs, checksum {restored_checksum})"
        )
        return {"weight": 1, "unit": "ops", "validated": True, "restored_docs": restored_docs, "checksum": restored_checksum}


class CleanupRecovery(_Runner):
    """Delete the restored copy and the source (indices or data streams). Snapshots are never deleted.

    Left out of the schedule with ``cleanup=false``. It must not be scheduled as a no-op: Rally
    times a task from the Elasticsearch requests it makes, so a runner that makes none fails the task.
    """

    name = "cleanup-recovery"

    async def __call__(self, es, params):
        names = [_require_plain_index_name(name) for name in (STATE.destination, STATE.source_index) if name]
        # The restored copy is the same kind as its source.
        base = "/_data_stream" if STATE.source_type == "data_stream" else ""
        for name in names:
            await self._ignore_missing(es.perform_request(method="DELETE", path=f"{base}/{name}"))
        return {"weight": 1, "unit": "ops", "cleaned_up": True}

    @staticmethod
    async def _ignore_missing(awaitable):
        try:
            await awaitable
        except Exception as e:
            if _status(e) != 404:
                raise


def register(registry):
    for runner in (
        DataRecoveryPreflight,
        RecordSource,
        WaitForRecoveryPoint,
        RestoreRecovery,
        ValidateRestore,
        CleanupRecovery,
    ):
        registry.register_runner(runner.name, runner(), async_runner=True)
