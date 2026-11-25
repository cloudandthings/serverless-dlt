"""db_bronze: the example Step Function (examples/db_snapshot.asl.json) drives the real handler end to end.

A small interpreter runs the ASL against the handler with a SQLite source:
only the subset the example uses (Task on `lambda:invoke`, Map, Pass, Succeed,
JSONPath `Parameters`/`ItemSelector`/`ResultSelector`/`ResultPath`, and
`States.Array`). Retry and Catch are not simulated. Because every Lambda
payload goes through the handler, the ASL can only use event shapes the
handler accepts.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_db_incremental import SourceDB, files, ids

from db_bronze.handler import handler

ASL = Path(__file__).resolve().parents[1] / "examples" / "db_snapshot.asl.json"


def resolve(path: str, data, context: dict):
    """A JSONPath subset: `$`, `$$`, `.key` steps and `[*]` (which maps, then flattens one level)."""
    root, rest = (context, path[2:]) if path.startswith("$$") else (data, path[1:])
    values, many = [root], False
    for step in re.findall(r"\.[^.\[]+|\[\*\]", rest):
        if step == "[*]":
            values, many = [item for v in values for item in v], True
        else:
            values = [v[step[1:]] for v in values]
    return values if many else values[0]


def evaluate(template, data, context: dict):
    """Apply a Parameters-style template: keys ending in `.$` take a path or `States.Array(...)` of paths."""
    if not isinstance(template, dict):
        return template
    out = {}
    for key, value in template.items():
        if key.endswith(".$"):
            array = re.fullmatch(r"States\.Array\((.*)\)", value)
            if array:
                out[key[:-2]] = [resolve(p.strip(), data, context) for p in array.group(1).split(",")]
            else:
                out[key[:-2]] = resolve(value, data, context)
        else:
            out[key] = evaluate(value, data, context)
    return out


def with_result(data, result, result_path: str | None):
    if result_path is None:
        return result
    assert re.fullmatch(r"\$\.[A-Za-z_]+", result_path), result_path
    return {**data, result_path[2:]: result}


def run_states(machine: dict, data, invoke, context: dict | None = None):
    context = context or {}
    name = machine["StartAt"]
    while True:
        state = machine["States"][name]
        kind = state["Type"]
        if kind == "Task":
            assert state["Resource"] == "arn:aws:states:::lambda:invoke"
            params = evaluate(state["Parameters"], data, context)
            result = {"Payload": invoke(params["Payload"]), "StatusCode": 200}
            if "ResultSelector" in state:
                result = evaluate(state["ResultSelector"], result, context)
            data = with_result(data, result, state.get("ResultPath"))
        elif kind == "Map":
            assert isinstance(state["MaxConcurrency"], int) and state["MaxConcurrency"] > 0
            items = resolve(state["ItemsPath"], data, context)
            results = []
            for index, item in enumerate(items):
                item_context = {"Map": {"Item": {"Index": index, "Value": item}}}
                item_input = evaluate(state["ItemSelector"], data, item_context) if "ItemSelector" in state else item
                results.append(run_states(state["ItemProcessor"], item_input, invoke, item_context))
            data = with_result(data, results, state.get("ResultPath"))
        elif kind == "Pass":
            data = evaluate(state["Parameters"], data, context) if "Parameters" in state else data
        elif kind == "Succeed":
            return data
        else:
            raise AssertionError(f"state {name}: type {kind} is not simulated")
        if state.get("End"):
            return data
        name = state["Next"]


@pytest.fixture
def lambda_invoke(tmp_path: Path, bucket: Path, pipelines_dir: str, monkeypatch):
    """The Lambda function the ASL calls: the handler, with config "core_banking_snapshot" from CONFIG_URI."""
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    db = SourceDB(tmp_path / "source.sqlite")
    db.insert(*[(i, f"2026-11-20 08:00:{i:02d}", "JHB", i) for i in range(1, 8)])
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())
    (config_dir / "core_banking_snapshot.json").write_text(json.dumps({
        "pipeline_name": "core_banking_snapshot",  # never the incremental's pipeline name
        "dataset_name": "core_banking",
        "bucket_url": bucket.as_uri(),
        "connection": {"dialect": "sqlite", "path": str(db.path)},
        "tables": [{"name": "transactions", "table": "Transactions", "primary_key": ["TransactionId"]}],
    }))
    events = []

    def invoke(event: dict) -> dict:
        events.append(event)
        return json.loads(json.dumps(handler(event, None)))  # what Lambda would hand back: JSON

    invoke.events = events
    return invoke


def test_example_asl_is_valid_json_with_plan_map_finalize():
    machine = json.loads(ASL.read_text())
    states = machine["States"]
    assert [states[n]["Type"] for n in ("Plan", "Pages", "Finalize")] == ["Task", "Map", "Task"]
    assert states["Pages"]["MaxConcurrency"] > 0


def test_example_asl_snapshots_a_table_through_the_handler(bucket, lambda_invoke):
    machine = json.loads(ASL.read_text())
    execution_input = {"source_id": "core_banking_snapshot", "table": "transactions", "page_size": 3}
    output = run_states(machine, execution_input, lambda_invoke)

    modes = [e["mode"] for e in lambda_invoke.events]
    assert modes == ["plan", "full", "full", "full", "finalize"]
    landed = [r for f in files(bucket) for r in pq.read_table(f).to_pylist()]
    assert ids(landed) == [1, 2, 3, 4, 5, 6, 7]
    [manifest_file] = (bucket / "core_banking" / "_manifests" / "transactions").glob("*.json")
    manifest = json.loads(manifest_file.read_text())
    assert sorted(manifest["load_ids"]) == sorted({f.name.rsplit(".", 2)[0] for f in files(bucket)})
    assert output["manifest"].endswith(f"/_manifests/transactions/{manifest_file.name}")
