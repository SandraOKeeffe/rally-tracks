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
import json
import pathlib
import re

import jinja2
import pytest

TRACK_DIR = pathlib.Path(__file__).parents[1]
CHALLENGE = TRACK_DIR / "challenges" / "restore-baseline.json"
README = TRACK_DIR / "README.md"

# Import the track module under a unique name to avoid colliding with other
# tracks' track.py modules when the whole repo's tests are collected together.
_spec = importlib.util.spec_from_file_location("data_recovery_track_for_schedule", TRACK_DIR / "track.py")
track = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(track)


def _task_names(**track_params):
    """Render restore-baseline.json the way Rally does (Jinja with the track params) and list its tasks."""
    rendered = jinja2.Template(CHALLENGE.read_text()).render(**track_params)
    return [task["name"] for task in json.loads(rendered)["schedule"]]


BASE = ["preflight", "index-source-docs", "wait-for-recovery-point", "restore-recovery"]


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
    assert _task_names(**params) == expected


def _operations():
    rendered = jinja2.Template(CHALLENGE.read_text()).render()
    return {task["operation"]["operation-type"]: task["operation"] for task in json.loads(rendered)["schedule"]}


def test_challenge_provides_every_param_its_runners_require():
    operations = _operations()
    for runner in (track.IndexSourceDocs, track.WaitForRecoveryPoint, track.RestoreRecovery):
        missing = set(runner.required_params) - set(operations[runner.name])
        assert not missing, f"{runner.name} requires {sorted(missing)} but restore-baseline.json does not set them"


# README param name -> (operation type, key in that operation). ``cleanup`` is the presence of a task, not a value.
README_PARAMS = {
    "source_index": ("index-source-docs", "index"),
    "doc_count": ("index-source-docs", "doc_count"),
    "bulk_size": ("index-source-docs", "bulk_size"),
    "validate_restore": ("index-source-docs", "validate_restore"),
    "recovery_point_mode": ("wait-for-recovery-point", "recovery_point_mode"),
    "recovery_repo": ("wait-for-recovery-point", "recovery_repo"),
    "recovery_point_timeout_secs": ("wait-for-recovery-point", "timeout_secs"),
    "poll_interval_secs": ("wait-for-recovery-point", "poll_interval_secs"),
    "restore_prefix": ("restore-recovery", "restore_prefix"),
    "recovery_timeout_secs": ("restore-recovery", "timeout_secs"),
    "restore_poll_interval_secs": ("restore-recovery", "poll_interval_secs"),
    "use_status_api": ("restore-recovery", "use_status_api"),
}


def _readme_defaults():
    """Rows of the README's track parameter table: ``| `name` | `default` | ... |``."""
    rows = (re.match(r"\|\s*`(\w+)`\s*\|\s*`([^`]*)`\s*\|", line) for line in README.read_text().splitlines())
    return {match.group(1): match.group(2) for match in rows if match}


def test_readme_parameter_table_matches_the_challenge_defaults():
    documented = _readme_defaults()
    assert set(documented) == set(README_PARAMS) | {"cleanup"}
    operations = _operations()
    for name, (operation_type, key) in README_PARAMS.items():
        actual = str(operations[operation_type][key]).lower()
        assert documented[name].lower() == actual, f"README default for {name} is {documented[name]}, challenge uses {actual}"
    assert documented["cleanup"] == "true"


def test_index_source_docs_still_gets_validate_restore_param():
    rendered = jinja2.Template(CHALLENGE.read_text()).render(validate_restore=False)
    index_task = next(t for t in json.loads(rendered)["schedule"] if t["name"] == "index-source-docs")
    assert index_task["operation"]["validate_restore"] is False
