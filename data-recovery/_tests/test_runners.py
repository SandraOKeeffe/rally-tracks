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

import importlib.util
import pathlib
from datetime import datetime, timedelta, timezone

import elasticsearch
import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig

TRACK_DIR = pathlib.Path(__file__).parents[1]

# Import the track module under a unique name to avoid colliding with other
# tracks' track.py modules when the whole repo's tests are collected together.
_spec = importlib.util.spec_from_file_location("data_recovery_track", TRACK_DIR / "track.py")
track = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(track)


class FakeApiError(Exception):
    def __init__(self, status_code, message=None):
        super().__init__(message or f"HTTP {status_code}")
        self.status_code = status_code


def api_error(status, body, message="search_phase_execution_exception"):
    """A real elasticsearch.ApiError, so str() and .body behave like the ones Rally's client raises."""
    meta = ApiResponseMeta(
        status=status, http_version="1.1", headers=HttpHeaders(), duration=0.0, node=NodeConfig("http", "localhost", 9200)
    )
    return elasticsearch.ApiError(message, meta, body)


_RESTORING_REASON = "index [x] is currently being restored from a snapshot and is temporarily unavailable"
# The error body a count on a restoring index returns (shape taken from a real QA run).
RESTORING_BODY = {
    "error": {
        "root_cause": [
            {"type": "shard_restoring_exception", "reason": _RESTORING_REASON, "index": "x", "recovery_id": "recovery-v1-default-abc"}
        ],
        "type": "search_phase_execution_exception",
        "reason": "all shards failed",
        "phase": "query",
        "grouped": True,
        "failed_shards": [
            {"shard": 0, "index": "x", "node": None, "reason": {"type": "shard_restoring_exception", "reason": _RESTORING_REASON}}
        ],
    },
    "status": 403,
}
SECURITY_BODY = {
    "error": {
        "root_cause": [{"type": "security_exception", "reason": "action is unauthorized"}],
        "type": "security_exception",
        "reason": "action is unauthorized",
    },
    "status": 403,
}


def test_str_of_a_real_api_error_does_not_contain_the_restoring_exception_type():
    # This is why the type must be read from the body: matching it in str(exc) never works.
    assert "shard_restoring_exception" not in str(api_error(403, RESTORING_BODY))


def test_is_not_ready_for_a_real_restoring_api_error():
    assert track._is_not_ready(api_error(403, RESTORING_BODY))


def test_is_not_ready_when_only_the_failed_shard_names_the_type():
    body = {"error": {"root_cause": [], "failed_shards": RESTORING_BODY["error"]["failed_shards"]}}
    assert track._is_not_ready(api_error(403, body))


def test_is_not_ready_for_404_and_503():
    assert track._is_not_ready(FakeApiError(404))
    assert track._is_not_ready(FakeApiError(503))


def test_is_not_ready_is_false_for_other_403s_and_other_errors():
    assert not track._is_not_ready(api_error(403, SECURITY_BODY))
    assert not track._is_not_ready(FakeApiError(500))
    assert not track._is_not_ready(ValueError("boom"))


class FakeResponse:
    def __init__(self, body):
        self.body = body


class FakeEs:
    """Calls ``handler(method, path, params, body)``; it returns a body or raises FakeApiError."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    async def perform_request(self, method, path, params=None, body=None, headers=None):
        self.calls.append((method, path, params, body))
        return FakeResponse(self.handler(method, path, params, body))


@pytest.fixture(autouse=True)
def fresh_state(monkeypatch):
    track.STATE.reset()

    async def no_sleep(_):
        return None

    monkeypatch.setattr(track.asyncio, "sleep", no_sleep)
    yield
    track.STATE.reset()


def _ts(offset_secs):
    base = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(seconds=offset_secs)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _point(start, end):
    return {"start_time": _ts(start), "end_time": _ts(end)}


def _set_indexed(index="src", docs=3, at=0):
    track.STATE.source_index = index
    track.STATE.expected_docs = docs
    track.STATE.indexed_at = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=at)


# The runners require every param (the challenge definition is the single source of defaults).
INDEX_PARAMS = {"index": "src", "doc_count": 5, "bulk_size": 1000, "validate_restore": True}
WAIT_PARAMS = {"recovery_point_mode": "wait", "recovery_repo": "data-recovery", "timeout_secs": 100, "poll_interval_secs": 0}
RESTORE_PARAMS = {"restore_prefix": "rally-restore-", "timeout_secs": 100, "poll_interval_secs": 0, "use_status_api": False}


@pytest.mark.asyncio
async def test_client_unwraps_plain_dict_and_api_response():
    plain = track.DataRecoveryClient(FakeEs(lambda *a: {"recovery_points": [1]}))
    assert await plain.list_recovery_points() == [1]

    class RawEs:
        async def perform_request(self, **kwargs):
            return FakeResponse('{"recovery_points": [2]}')

    assert await track.DataRecoveryClient(RawEs()).list_recovery_points() == [2]


@pytest.mark.asyncio
async def test_start_recovery_request_body():
    es = FakeEs(lambda *a: {"recovery_id": "abc"})
    await track.DataRecoveryClient(es).start_recovery("2026-11-13T12:00:01.000Z", ["src"], prefix="p-")
    method, path, _, body = es.calls[0]
    assert (method, path) == ("POST", "/_data_recovery/recoveries")
    assert body == {
        "recovery_point_end_time": "2026-11-13T12:00:01.000Z",
        "sources": ["src"],
        "mode": "rename",
        "rename_strategy": {"prefix": "p-"},
    }


@pytest.mark.asyncio
async def test_list_sources_request():
    es = FakeEs(lambda *a: {"sources": []})
    result = await track.DataRecoveryClient(es).list_sources("2026-11-13T12:00:01.000Z", "src")
    assert es.calls == [
        ("GET", "/_data_recovery/sources", {"expression": "src", "recovery_point_end_time": "2026-11-13T12:00:01.000Z"}, None)
    ]
    assert result == {"sources": []}


@pytest.mark.asyncio
async def test_get_recovery_request():
    es = FakeEs(lambda *a: {"status": "running"})
    result = await track.DataRecoveryClient(es).get_recovery("id1")
    assert es.calls == [("GET", "/_data_recovery/recoveries/id1", None, None)]
    assert result == {"status": "running"}


@pytest.mark.asyncio
async def test_wait_for_recovery_point_skips_points_before_indexing_and_incomplete_sources():
    _set_indexed(at=10)
    state = {"polls": 0}

    def handler(method, path, params, body):
        if path == "/_data_recovery/points":
            state["polls"] += 1
            points = [_point(0, 1)]  # starts before indexing finished: never usable
            if state["polls"] >= 2:
                points.insert(0, _point(20, 21))
            return {"recovery_points": points}
        complete = state["polls"] >= 3
        return {"sources": [{"name": "src", "type": "index", "complete": complete}]}

    result = await track.WaitForRecoveryPoint()(FakeEs(handler), WAIT_PARAMS)
    assert result["recovery_point_end_time"] == _ts(21)
    assert track.STATE.recovery_point_end_time == _ts(21)


@pytest.mark.asyncio
async def test_wait_for_recovery_point_snapshot_mode_creates_snapshot():
    _set_indexed(at=0)

    def handler(method, path, params, body):
        if method == "PUT":
            return {"snapshot": {}}
        if path == "/_data_recovery/points":
            return {"recovery_points": [_point(5, 6)]}
        return {"sources": [{"name": "src", "complete": True}]}

    es = FakeEs(handler)
    result = await track.WaitForRecoveryPoint()(es, {**WAIT_PARAMS, "recovery_point_mode": "snapshot", "recovery_repo": "my-repo"})
    put = next(c for c in es.calls if c[0] == "PUT")
    assert put[1].startswith("/_snapshot/my-repo/rally-")
    assert put[3] == {"indices": "src", "include_global_state": False}
    assert result["snapshot"] == put[1].rsplit("/", 1)[1]


@pytest.mark.asyncio
async def test_wait_for_recovery_point_times_out():
    _set_indexed(at=0)
    es = FakeEs(lambda *a: {"recovery_points": []})
    with pytest.raises(RuntimeError, match="no recovery point"):
        await track.WaitForRecoveryPoint()(es, {**WAIT_PARAMS, "timeout_secs": -1})


@pytest.mark.asyncio
async def test_wait_for_recovery_point_rejects_unknown_mode():
    _set_indexed()
    with pytest.raises(ValueError):
        await track.WaitForRecoveryPoint()(FakeEs(lambda *a: {}), {**WAIT_PARAMS, "recovery_point_mode": "bogus"})


@pytest.mark.asyncio
@pytest.mark.parametrize("repo", ["a/b", "a?b", "a,b", "*", "_all", ".x", "-x", "", "has space"])
async def test_wait_for_recovery_point_refuses_unsafe_repository_names_without_calling_es(repo):
    _set_indexed()
    es = FakeEs(lambda *a: {})
    with pytest.raises(ValueError, match="not a plain repository name"):
        await track.WaitForRecoveryPoint()(es, {**WAIT_PARAMS, "recovery_point_mode": "snapshot", "recovery_repo": repo})
    assert es.calls == []


def _set_recovery_point():
    _set_indexed(docs=3)
    track.STATE.recovery_point_end_time = _ts(21)
    track.STATE.recovery_point_wait_secs = 0.5


def _restore_handler(on_other):
    """Handle the restore start request; everything else goes to ``on_other``."""

    def handler(method, path, params, body):
        if method == "POST" and path == "/_data_recovery/recoveries":
            prefix = body["rename_strategy"]["prefix"]
            return {"recovery_id": "id1", "resolved_sources": [{"source": "src", "destination": prefix + "src"}]}
        return on_other(method, path, params, body)

    return handler


@pytest.mark.asyncio
async def test_restore_recovery_starts_restore_retries_transient_errors_and_reports_metrics():
    _set_recovery_point()
    errors = [FakeApiError(404), FakeApiError(503)]

    def other(method, path, params, body):
        if path.endswith("/_count"):
            if errors:
                raise errors.pop(0)
            return {"count": 3}
        return [{"prirep": "p"}, {"prirep": "r"}, {"prirep": "p"}]

    es = FakeEs(_restore_handler(other))
    result = await track.RestoreRecovery()(es, RESTORE_PARAMS)

    start = next(c for c in es.calls if c[0] == "POST")
    assert start[3]["recovery_point_end_time"] == _ts(21)
    assert start[3]["sources"] == ["src"]
    assert start[3]["mode"] == "rename"
    assert track.STATE.recovery_id == "id1"
    assert track.STATE.destination.startswith("rally-restore-")
    assert track.STATE.destination.endswith("-src")
    assert result["restored_docs"] == 3
    assert result["source_docs"] == 3
    assert result["restored_primary_shards"] == 2
    assert result["restore_duration_secs"] >= 0
    assert result["recovery_point_wait_secs"] == 0.5


@pytest.mark.asyncio
async def test_restore_recovery_retries_403_shard_restoring_exception():
    _set_recovery_point()
    errors = [api_error(403, RESTORING_BODY), api_error(403, RESTORING_BODY)]

    def other(method, path, params, body):
        if path.endswith("/_count"):
            if errors:
                raise errors.pop(0)
            return {"count": 3}
        return [{"prirep": "p"}]

    result = await track.RestoreRecovery()(FakeEs(_restore_handler(other)), RESTORE_PARAMS)
    assert result["restored_docs"] == 3
    assert errors == []


@pytest.mark.asyncio
async def test_restore_recovery_does_not_retry_other_403_errors():
    _set_recovery_point()

    def other(method, path, params, body):
        raise api_error(403, SECURITY_BODY, message="security_exception")

    with pytest.raises(elasticsearch.ApiError):
        await track.RestoreRecovery()(FakeEs(_restore_handler(other)), RESTORE_PARAMS)


@pytest.mark.asyncio
async def test_restore_recovery_reports_summary(monkeypatch):
    _set_recovery_point()
    reported = []
    monkeypatch.setattr(track, "_report", reported.append)

    def other(method, path, params, body):
        return {"count": 3} if path.endswith("/_count") else [{"prirep": "p"}]

    await track.RestoreRecovery()(FakeEs(_restore_handler(other)), RESTORE_PARAMS)
    assert len(reported) == 1
    assert "3/3 docs" in reported[0]
    assert "1 primary shards" in reported[0]


@pytest.mark.asyncio
async def test_restore_recovery_uses_unique_prefix_per_run():
    _set_recovery_point()

    def other(method, path, params, body):
        return {"count": 3} if path.endswith("/_count") else [{"prirep": "p"}]

    runner = track.RestoreRecovery()
    await runner(FakeEs(_restore_handler(other)), RESTORE_PARAMS)
    first = track.STATE.destination
    await runner(FakeEs(_restore_handler(other)), RESTORE_PARAMS)
    assert track.STATE.destination != first


@pytest.mark.asyncio
async def test_restore_recovery_fails_on_unexpected_error():
    _set_recovery_point()

    def other(method, path, params, body):
        raise FakeApiError(500)

    with pytest.raises(FakeApiError):
        await track.RestoreRecovery()(FakeEs(_restore_handler(other)), RESTORE_PARAMS)


@pytest.mark.asyncio
async def test_restore_recovery_fails_on_doc_count_mismatch_after_timeout():
    _set_recovery_point()

    def other(method, path, params, body):
        return {"count": 1}

    with pytest.raises(RuntimeError, match="expected 3"):
        await track.RestoreRecovery()(FakeEs(_restore_handler(other)), {**RESTORE_PARAMS, "timeout_secs": -1})


@pytest.mark.asyncio
async def test_restore_recovery_status_api_failed():
    _set_recovery_point()

    def other(method, path, params, body):
        return {"status": "failed"}

    with pytest.raises(RuntimeError, match="failed"):
        await track.RestoreRecovery()(FakeEs(_restore_handler(other)), {**RESTORE_PARAMS, "use_status_api": True})


@pytest.mark.asyncio
async def test_restore_recovery_status_api_completed_then_waits_for_docs():
    _set_recovery_point()
    statuses = ["running", "running", "completed"]

    def other(method, path, params, body):
        if path.startswith("/_data_recovery/recoveries/"):
            return {"status": statuses.pop(0)}
        return {"count": 3} if path.endswith("/_count") else [{"prirep": "p"}]

    es = FakeEs(_restore_handler(other))
    result = await track.RestoreRecovery()(es, {**RESTORE_PARAMS, "use_status_api": True})
    assert statuses == []
    assert result["restored_docs"] == 3
    assert ("GET", "/_data_recovery/recoveries/id1", None, None) in es.calls


@pytest.mark.asyncio
async def test_restore_recovery_status_api_times_out_while_still_running():
    _set_recovery_point()

    def other(method, path, params, body):
        return {"status": "running"}

    with pytest.raises(RuntimeError, match=r"still \[running\]"):
        await track.RestoreRecovery()(FakeEs(_restore_handler(other)), {**RESTORE_PARAMS, "use_status_api": True, "timeout_secs": -1})


@pytest.mark.asyncio
async def test_cleanup_deletes_both_indices_ignores_missing_and_never_deletes_snapshots():
    _set_indexed()
    track.STATE.destination = "restored-src"

    def handler(method, path, params, body):
        if path == "/restored-src":
            raise FakeApiError(404)
        return {}

    es = FakeEs(handler)
    result = await track.CleanupRecovery()(es, {})
    assert result["cleaned_up"] is True
    assert ("DELETE", "/restored-src", None, None) in es.calls
    assert ("DELETE", "/src", None, None) in es.calls
    assert not any("_snapshot" in call[1] for call in es.calls)


UNSAFE_NAMES = ["*", "recovery-*", "a,b", "_all", ".kibana", "-x", "+x", "", "has space", "a/b", "a?b"]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", UNSAFE_NAMES)
async def test_index_source_docs_refuses_unsafe_index_names_without_calling_es(name):
    es = FakeEs(_index_handler())
    with pytest.raises(ValueError, match="not a plain index name"):
        await track.IndexSourceDocs()(es, {**INDEX_PARAMS, "index": name})
    assert es.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["*", "a,b", "_all", ".kibana"])
async def test_cleanup_refuses_unsafe_names_without_deleting(name):
    track.STATE.destination = "restored-src"
    track.STATE.source_index = name
    es = FakeEs(lambda *a: {})
    with pytest.raises(ValueError, match="not a plain index name"):
        await track.CleanupRecovery()(es, {})
    assert es.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("override", [{"doc_count": 0}, {"doc_count": -1}, {"bulk_size": 0}, {"bulk_size": -5}])
async def test_index_source_docs_rejects_non_positive_counts_without_calling_es(override):
    es = FakeEs(_index_handler())
    with pytest.raises(ValueError, match="must be at least 1"):
        await track.IndexSourceDocs()(es, {**INDEX_PARAMS, **override})
    assert es.calls == []


def test_plain_index_names_are_accepted():
    for name in ["recovery-source", "recovery-source-rally", "my_index.v2", "rally-restore-abc123-src"]:
        assert track._require_plain_index_name(name) == name


def _index_handler(count=5, checksum=10.0):
    def handler(method, path, params, body):
        if method == "DELETE":
            raise FakeApiError(404)
        if path.endswith("/_bulk"):
            return {"errors": False}
        if path.endswith("/_count"):
            return {"count": count}
        if path.endswith("/_search"):
            return {"aggregations": {"total": {"value": checksum}}}
        return {}

    return handler


@pytest.mark.asyncio
async def test_index_source_docs_bulk_indexes_and_records_state():
    es = FakeEs(_index_handler(count=5, checksum=10.0))
    result = await track.IndexSourceDocs()(es, {**INDEX_PARAMS, "bulk_size": 2})
    bulks = [c for c in es.calls if c[1] == "/src/_bulk"]
    assert len(bulks) == 3
    assert result["weight"] == 5
    assert track.STATE.expected_docs == 5
    assert track.STATE.expected_checksum == 10.0
    assert track.STATE.indexed_at is not None


@pytest.mark.asyncio
async def test_index_source_docs_fails_when_source_count_differs():
    es = FakeEs(_index_handler(count=4))
    with pytest.raises(RuntimeError, match="holds 4 docs after indexing, expected 5"):
        await track.IndexSourceDocs()(es, INDEX_PARAMS)


@pytest.mark.asyncio
async def test_index_source_docs_skips_checksum_when_validation_disabled():
    es = FakeEs(_index_handler(count=5))
    await track.IndexSourceDocs()(es, {**INDEX_PARAMS, "validate_restore": False})
    assert track.STATE.expected_checksum is None
    assert not any(c[1].endswith("/_search") for c in es.calls)


def _validation_handler(count, checksum):
    def handler(method, path, params, body):
        if path.endswith("/_count"):
            return {"count": count}
        return {"aggregations": {"total": {"value": checksum}}}

    return handler


def _set_validation_state():
    _set_indexed(docs=3)
    track.STATE.expected_checksum = 3.0
    track.STATE.destination = "restored-src"


@pytest.mark.asyncio
async def test_validate_restore_passes_when_count_and_checksum_match():
    _set_validation_state()
    result = await track.ValidateRestore()(FakeEs(_validation_handler(3, 3.0)), {})
    assert result["validated"] is True
    assert result["restored_docs"] == 3


@pytest.mark.asyncio
async def test_validate_restore_fails_on_count_mismatch():
    _set_validation_state()
    with pytest.raises(RuntimeError, match="has 2 docs"):
        await track.ValidateRestore()(FakeEs(_validation_handler(2, 3.0)), {})


@pytest.mark.asyncio
async def test_validate_restore_fails_on_checksum_mismatch():
    _set_validation_state()
    with pytest.raises(RuntimeError, match="checksum"):
        await track.ValidateRestore()(FakeEs(_validation_handler(3, 99.0)), {})


@pytest.mark.asyncio
async def test_restore_recovery_reports_not_queryable_distinctly():
    _set_recovery_point()

    def other(method, path, params, body):
        raise FakeApiError(503)

    with pytest.raises(RuntimeError, match="not queryable"):
        await track.RestoreRecovery()(FakeEs(_restore_handler(other)), {**RESTORE_PARAMS, "timeout_secs": -1})


def test_register_registers_all_runners():
    class Registry:
        def __init__(self):
            self.runners = {}

        def register_runner(self, name, runner, async_runner=False):
            self.runners[name] = runner

    registry = Registry()
    track.register(registry)
    assert set(registry.runners) == {
        "data-recovery-preflight",
        "index-source-docs",
        "wait-for-recovery-point",
        "restore-recovery",
        "validate-restore",
        "cleanup-recovery",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "runner, base_params",
    [
        (track.IndexSourceDocs, INDEX_PARAMS),
        (track.WaitForRecoveryPoint, WAIT_PARAMS),
        (track.RestoreRecovery, RESTORE_PARAMS),
    ],
)
async def test_runners_fail_clearly_when_a_required_param_is_missing(runner, base_params):
    es = FakeEs(lambda *a: {})
    for key in runner.required_params:
        params = {k: v for k, v in base_params.items() if k != key}
        with pytest.raises(KeyError, match="missing required param"):
            await runner()(es, params)
    assert es.calls == []


def test_required_params_cover_every_param_the_runners_read():
    assert set(INDEX_PARAMS) == set(track.IndexSourceDocs.required_params)
    assert set(WAIT_PARAMS) == set(track.WaitForRecoveryPoint.required_params)
    assert set(RESTORE_PARAMS) == set(track.RestoreRecovery.required_params)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "runner, params",
    [
        (track.WaitForRecoveryPoint, WAIT_PARAMS),
        (track.RestoreRecovery, RESTORE_PARAMS),
        (track.ValidateRestore, {}),
    ],
)
async def test_runners_fail_clearly_when_an_earlier_task_has_not_set_the_state(runner, params):
    # STATE is empty (reset by the fixture), as when tasks run in different processes or out of order.
    es = FakeEs(lambda *a: {})
    with pytest.raises(RuntimeError, match="from an earlier task"):
        await runner()(es, params)
    assert es.calls == []
