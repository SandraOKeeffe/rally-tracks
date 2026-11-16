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

import json
import pathlib

import jinja2
import pytest

CHALLENGE = pathlib.Path(__file__).parents[1] / "challenges" / "restore-baseline.json"


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


def test_index_source_docs_still_gets_validate_restore_param():
    rendered = jinja2.Template(CHALLENGE.read_text()).render(validate_restore=False)
    index_task = next(t for t in json.loads(rendered)["schedule"] if t["name"] == "index-source-docs")
    assert index_task["operation"]["validate_restore"] is False
