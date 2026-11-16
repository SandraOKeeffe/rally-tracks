# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Serverless data recovery restore track.

MVP: the ``restore-baseline`` challenge indexes a small source index, obtains a recovery point
(created via a snapshot locally, or waited for on a real deployment), restores it with
``mode: rename`` and verifies the restored doc count. ``measure-cluster-state`` is still a stub.

State is shared between the runners of a race through ``STATE``, so the track assumes a single
client per operation (the default for the tasks in ``restore-baseline``).
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


def _unwrap(response):
    """Return the parsed JSON body of a response, whether it is an ApiResponse or a plain dict."""
    body = getattr(response, "body", response)
    if isinstance(body, (bytes, str)):
        body = json.loads(body) if body else {}
    return body


def _status(exc):
    return getattr(exc, "status_code", None) or getattr(exc, "status", None)


def _parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utcnow():
    return datetime.now(timezone.utc)


def _report(message):
    """Show a result on the console and in the Rally log (not kept by the in-memory metrics store)."""
    console.info(message)
    logger.info(message)


_UNSAFE_NAME = re.compile(r"[*?,/\\\s]")


def _require_plain_index_name(name):
    """Refuse anything that could make a DELETE hit more than the single index this track created.

    Rejects empty names, wildcards, lists, whitespace, path characters, and names starting with
    ``_``, ``-``, ``+`` or ``.`` (which also covers ``_all`` and system indices).
    """
    if not isinstance(name, str) or not name or _UNSAFE_NAME.search(name) or name[0] in "_-+.":
        raise ValueError(
            f"[{name}] is not a plain index name: wildcards, lists, whitespace and names starting with _ - + . are not allowed"
        )
    return name


class DataRecoveryClient:
    """Single place for all data recovery API calls.

    Calls the Elasticsearch-side ``_data_recovery`` APIs directly (``points``,
    ``sources``, ``recoveries``) with the operator credentials esbench passes to
    Rally. The control-plane (project-api) equivalents are incomplete and Rally has
    no control-plane credentials, so they are not used. Callers must only go
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

    async def primary_shard_count(self, index):
        shards = await self._request("GET", f"/_cat/shards/{index}", params={"format": "json"})
        return sum(1 for shard in shards if shard.get("prirep") == "p")

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
        self.expected_checksum = None
        self.indexed_at = None
        self.recovery_point_end_time = None
        self.recovery_point_wait_secs = None
        self.recovery_id = None
        self.destination = None


STATE = _RunState()


class _Runner:
    name = None

    def __repr__(self, *args, **kwargs):
        return self.name


class DataRecoveryPreflight(_Runner):
    name = "data-recovery-preflight"

    async def __call__(self, es, params):
        STATE.reset()
        points = await DataRecoveryClient(es).list_recovery_points(size=1)
        return {"weight": 1, "unit": "ops", "recovery_points_available": len(points)}


class IndexSourceDocs(_Runner):
    name = "index-source-docs"

    async def __call__(self, es, params):
        index = _require_plain_index_name(params["index"])
        doc_count = int(params.get("doc_count", 100))
        bulk_size = int(params.get("bulk_size", 1000))

        try:
            await es.perform_request(method="DELETE", path=f"/{index}")
        except Exception as e:
            if _status(e) != 404:
                raise
        await es.perform_request(method="PUT", path=f"/{index}")

        for start in range(0, doc_count, bulk_size):
            lines = []
            for n in range(start, min(start + bulk_size, doc_count)):
                lines.append(json.dumps({"index": {}}))
                lines.append(json.dumps({"n": n, "message": f"doc {n}"}))
            response = await es.perform_request(
                method="POST",
                path=f"/{index}/_bulk",
                params={"refresh": "true"},
                body="\n".join(lines) + "\n",
                headers={"content-type": "application/x-ndjson"},
            )
            if _unwrap(response).get("errors"):
                raise RuntimeError(f"bulk indexing into [{index}] reported errors")

        STATE.source_index = index
        STATE.indexed_at = _utcnow()

        # Read the expected values back from the source instead of trusting what we asked to index.
        client = DataRecoveryClient(es)
        indexed = await client.count(index)
        if indexed != doc_count:
            raise RuntimeError(f"[{index}] holds {indexed} docs after indexing, expected {doc_count}")
        STATE.expected_docs = indexed
        STATE.expected_checksum = await client.sum_field(index, "n") if params.get("validate_restore", True) else None
        return {"weight": doc_count, "unit": "docs", "source_index": index}


class WaitForRecoveryPoint(_Runner):
    name = "wait-for-recovery-point"

    async def __call__(self, es, params):
        mode = params.get("recovery_point_mode", "wait")
        timeout = float(params.get("timeout_secs", 2400))
        interval = float(params.get("poll_interval_secs", 10))
        client = DataRecoveryClient(es)
        started = time.monotonic()

        # Snapshots are left in place: the track never deletes them.
        snapshot_name = None
        if mode == "snapshot":
            repository = params.get("recovery_repo", "data-recovery")
            snapshot_name = f"rally-{uuid.uuid4().hex[:8]}"
            await client.create_snapshot(repository, snapshot_name, STATE.source_index)
        elif mode != "wait":
            raise ValueError(f"recovery_point_mode must be 'wait' or 'snapshot', got [{mode}]")

        while True:
            point = await self._usable_point(client)
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
    async def _usable_point(client):
        """Earliest recovery point that started after indexing finished and has the source complete."""
        points = await client.list_recovery_points()
        candidates = [p for p in points if _parse_ts(p["start_time"]) > STATE.indexed_at]
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

    async def __call__(self, es, params):
        timeout = float(params.get("timeout_secs", 600))
        interval = float(params.get("poll_interval_secs", 2))
        use_status_api = bool(params.get("use_status_api", False))
        # Unique per run so restores never collide and cleanup can target them.
        prefix = f"{params.get('restore_prefix', 'rally-restore-')}{uuid.uuid4().hex[:6]}-"
        client = DataRecoveryClient(es)

        started = time.monotonic()
        body = await client.start_recovery(STATE.recovery_point_end_time, [STATE.source_index], mode="rename", prefix=prefix)
        STATE.recovery_id = body["recovery_id"]
        STATE.destination = body["resolved_sources"][0]["destination"]

        if use_status_api:
            await self._wait_with_status_api(client, timeout, interval)
        restored_docs = await self._wait_for_docs(client, timeout, interval)
        restore_duration = time.monotonic() - started
        primary_shards = await client.primary_shard_count(STATE.destination)

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
    async def _wait_with_status_api(client, timeout, interval):
        deadline = time.monotonic() + timeout
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
    async def _wait_for_docs(client, timeout, interval):
        """Poll the restored index until it holds every source doc; 404/503 mean it is not ready yet."""
        deadline = time.monotonic() + timeout
        last = None
        while True:
            try:
                last = await client.count(STATE.destination)
            except Exception as e:
                if _status(e) not in TRANSIENT_STATUSES:
                    raise
            else:
                if last == STATE.expected_docs:
                    return last
                if last > STATE.expected_docs:
                    raise RuntimeError(f"[{STATE.destination}] has {last} docs, expected {STATE.expected_docs}")
            if time.monotonic() > deadline:
                if last is None:
                    raise RuntimeError(f"[{STATE.destination}] was not queryable after {timeout}s")
                raise RuntimeError(f"[{STATE.destination}] has {last} docs after {timeout}s, expected {STATE.expected_docs}")
            await asyncio.sleep(interval)


class ValidateRestore(_Runner):
    """Check the restored index against the source: same doc count and the same content checksum.

    Left out of the schedule with ``validate_restore=false`` (for example for data too large to
    aggregate over). It must not be scheduled as a no-op: Rally times a task from the Elasticsearch
    requests it makes, so a runner that makes none fails the task. Kept out of ``restore-recovery``
    so that task's timing is only the restore itself.
    """

    name = "validate-restore"

    async def __call__(self, es, params):
        client = DataRecoveryClient(es)
        restored_docs = await client.count(STATE.destination)
        if restored_docs != STATE.expected_docs:
            raise RuntimeError(
                f"validation failed: [{STATE.destination}] has {restored_docs} docs, source [{STATE.source_index}] had {STATE.expected_docs}"
            )
        restored_checksum = await client.sum_field(STATE.destination, "n")
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
    """Delete the restored and source indices. Snapshots are never deleted.

    Left out of the schedule with ``cleanup=false``. It must not be scheduled as a no-op: Rally
    times a task from the Elasticsearch requests it makes, so a runner that makes none fails the task.
    """

    name = "cleanup-recovery"

    async def __call__(self, es, params):
        indices = [_require_plain_index_name(index) for index in (STATE.destination, STATE.source_index) if index]
        for index in indices:
            await self._ignore_missing(es.perform_request(method="DELETE", path=f"/{index}"))
        return {"weight": 1, "unit": "ops", "cleaned_up": True}

    @staticmethod
    async def _ignore_missing(awaitable):
        try:
            await awaitable
        except Exception as e:
            if _status(e) != 404:
                raise


class MeasureClusterState(_Runner):
    name = "measure-cluster-state"

    async def __call__(self, es, params):
        raise NotImplementedError(f"{self.name} is not implemented yet")


def register(registry):
    for runner in (
        DataRecoveryPreflight,
        IndexSourceDocs,
        WaitForRecoveryPoint,
        RestoreRecovery,
        ValidateRestore,
        CleanupRecovery,
        MeasureClusterState,
    ):
        registry.register_runner(runner.name, runner(), async_runner=True)
