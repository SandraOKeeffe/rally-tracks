# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib.util
import json
import pathlib
import re

import jinja2
import pytest
from esrally.track import loader

TRACK_DIR = pathlib.Path(__file__).parents[1]
TRACK_JSON = TRACK_DIR / "track.json"
README = TRACK_DIR / "README.md"
CORPORA = sorted(path.stem for path in (TRACK_DIR / "corpora").glob("*.json"))

# Import the track module under a unique name to avoid colliding with other
# tracks' track.py modules when the whole repo's tests are collected together.
_spec = importlib.util.spec_from_file_location("data_recovery_track_for_schedule", TRACK_DIR / "track.py")
track = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(track)


def _render(build_flavor="serverless", **track_params):
    """Render track.json the way Rally does: its own Jinja environment, helpers and includes."""
    return json.loads(loader.render_template_from_file(str(TRACK_JSON), track_params, build_flavor=build_flavor))


def _tasks(**track_params):
    return {task["name"]: task for task in _render(**track_params)["challenges"][0]["schedule"]}


BASE = [
    "preflight",
    "delete-source",
    "create-source",
    "index-source",
    "refresh-source",
    "record-source",
    "wait-for-recovery-point",
    "restore-recovery",
]


@pytest.mark.parametrize(
    "params, expected",
    [
        ({}, BASE + ["validate-restore", "cleanup-recovery"]),
        ({"cleanup": False}, BASE + ["validate-restore"]),
        ({"validate_restore": False}, BASE + ["cleanup-recovery"]),
        ({"cleanup": False, "validate_restore": False}, BASE),
    ],
)
def test_optional_tasks_are_left_out_when_switched_off(params, expected):
    # Rally fails a task whose runner makes no Elasticsearch request, so a switched-off
    # validate-restore / cleanup-recovery must not be scheduled at all.
    assert list(_tasks(**params)) == expected


def test_challenge_provides_every_param_its_runners_require():
    operations = {task["operation"]["operation-type"]: task["operation"] for task in _tasks().values()}
    for runner in (track.DataRecoveryPreflight, track.RecordSource, track.WaitForRecoveryPoint, track.RestoreRecovery):
        missing = set(runner.required_params) - set(operations[runner.name])
        assert not missing, f"{runner.name} requires {sorted(missing)} but the challenge does not set them"


def test_record_source_skips_the_checksum_when_validation_is_off():
    assert _tasks()["record-source"]["operation"]["checksum_field"] == "population"
    assert _tasks(validate_restore=False)["record-source"]["operation"]["checksum_field"] is None


def test_preflight_and_restore_recovery_validate_the_same_restore_prefix():
    for params in ({}, {"restore_prefix": "custom-"}):
        tasks = _tasks(**params)
        assert tasks["preflight"]["operation"]["restore_prefix"] == tasks["restore-recovery"]["operation"]["restore_prefix"]


def test_every_corpus_in_corpus_config_has_its_files():
    configured = set(re.findall(r'^\s*"(\w+)": \{"target"', TRACK_JSON.read_text(), re.MULTILINE))
    assert configured == set(CORPORA), "corpus_config in track.json and corpora/*.json must list the same corpora"
    for name in configured:
        assert (TRACK_DIR / "mappings" / f"{name}.json").is_file(), f"mappings/{name}.json is missing"


@pytest.mark.parametrize("build_flavor", ["serverless", "default"])
@pytest.mark.parametrize("corpus", CORPORA)
def test_each_corpus_renders_a_consistent_track(corpus, build_flavor):
    rendered = _render(build_flavor=build_flavor, corpus=corpus)
    (index,) = rendered["indices"]
    (corpus_entry,) = rendered["corpora"]
    tasks = {task["name"]: task for task in rendered["challenges"][0]["schedule"]}
    target = index["name"]

    assert corpus_entry["name"] == corpus
    assert {doc["target-index"] for doc in corpus_entry["documents"]} == {target}
    assert track._require_plain_index_name(target) == target
    assert tasks["index-source"]["operation"]["corpora"] == corpus
    for name in ("preflight", "refresh-source", "record-source"):
        key = "source_index" if name == "preflight" else "index"
        assert tasks[name]["operation"][key] == target, f"{name} does not target [{target}]"

    # The checksum is a sum compared exactly, so it needs an integer field (float sums depend on order).
    mapping = json.loads(jinja2.Template((TRACK_DIR / index["body"]).read_text()).render(build_flavor=build_flavor))
    field = tasks["record-source"]["operation"]["checksum_field"]
    if field is not None:
        field_type = mapping["mappings"]["properties"][field]["type"]
        assert field_type in ("integer", "long", "short", "byte"), f"checksum field [{field}] is a {field_type}"


def test_default_corpus_is_geonames():
    assert _render()["corpora"][0]["name"] == "geonames"


def test_unknown_corpus_fails_to_render():
    with pytest.raises(jinja2.exceptions.UndefinedError, match="bogus"):
        _render(corpus="bogus")


# README param name -> (task name, path into that task). ``cleanup`` and ``validate_restore`` are the presence of a task.
README_PARAMS = {
    "corpus": ("index-source", ("operation", "corpora")),
    "ingest_percentage": ("index-source", ("operation", "ingest-percentage")),
    "bulk_size": ("index-source", ("operation", "bulk-size")),
    "bulk_indexing_clients": ("index-source", ("clients",)),
    "recovery_point_mode": ("wait-for-recovery-point", ("operation", "recovery_point_mode")),
    "recovery_repo": ("wait-for-recovery-point", ("operation", "recovery_repo")),
    "recovery_point_timeout_secs": ("wait-for-recovery-point", ("operation", "timeout_secs")),
    "poll_interval_secs": ("wait-for-recovery-point", ("operation", "poll_interval_secs")),
    "restore_prefix": ("restore-recovery", ("operation", "restore_prefix")),
    "recovery_timeout_secs": ("restore-recovery", ("operation", "timeout_secs")),
    "restore_poll_interval_secs": ("restore-recovery", ("operation", "poll_interval_secs")),
    "use_status_api": ("restore-recovery", ("operation", "use_status_api")),
}


def _readme_defaults():
    """Rows of the README's track parameter table: ``| `name` | `default` | ... |``."""
    section = README.read_text().split("## Track parameters", 1)[1].split("\n## ", 1)[0]
    rows = (re.match(r"\|\s*`(\w+)`\s*\|\s*`([^`]*)`\s*\|", line) for line in section.splitlines())
    return {match.group(1): match.group(2) for match in rows if match}


def test_readme_parameter_table_matches_the_track_defaults():
    documented = _readme_defaults()
    assert set(documented) == set(README_PARAMS) | {"cleanup", "validate_restore"}
    tasks = _tasks()
    for name, (task_name, path) in README_PARAMS.items():
        actual = tasks[task_name]
        for key in path:
            actual = actual[key]
        actual = str(actual).lower()
        assert documented[name].lower() == actual, f"README default for {name} is {documented[name]}, the track uses {actual}"
    assert documented["cleanup"] == "true" and "cleanup-recovery" in tasks
    assert documented["validate_restore"] == "true" and "validate-restore" in tasks
