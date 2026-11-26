"""db_bronze: failure types, the backend-change guard, run logs and EMF metrics (docs/db/errors.md, D6)."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from db_bronze import extract
from db_bronze.handler import handler


class PanicException(BaseException):  # stands in for pyo3_runtime.PanicException (a BaseException)
    pass


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.sqlite"
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE "Transactions" ("TransactionId" INTEGER PRIMARY KEY, "UpdatedAt" DATETIME)')
        con.execute("INSERT INTO \"Transactions\" VALUES (1, '2026-11-20 08:00:00')")
    return path


@pytest.fixture
def invoke(source: Path, bucket: Path, tmp_path: Path, pipelines_dir: str, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    configs = tmp_path / "configs"
    configs.mkdir()
    monkeypatch.setenv("CONFIG_URI", configs.as_uri())

    def run(**table) -> dict:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(source)},
            "tables": [
                {
                    "name": "transactions",
                    "table": "Transactions",
                    "primary_key": ["TransactionId"],
                    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00"},
                    "backend": "connectorx",
                }
                | table
            ],
        }
        (configs / "core.json").write_text(json.dumps(cfg))
        return handler({"source_id": "core"}, None)

    return run


NULLABLE_CURSOR = {
    "column": "UpdatedAt",
    "type": "timestamp",
    "initial_value": "2026-01-01",
    "include_null_cursor": True,
}


def add_row(source: Path, i: int, ts: str) -> None:
    with sqlite3.connect(source) as con:
        con.execute('INSERT INTO "Transactions" VALUES (?, ?)', (i, ts))


def landed_ids(bucket: Path) -> list[int]:
    root = bucket / "core_banking" / "transactions"
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    return sorted(r["TransactionId"] for f in files for r in pq.read_table(f).to_pylist())


def emf(out: str) -> list[dict]:
    return [d for line in out.splitlines() if line.startswith("{") and "_aws" in (d := json.loads(line))]


def metric(records: list[dict], name: str) -> dict | None:
    return next((r for r in records if name in r), None)


def break_connectorx(monkeypatch, exc: BaseException, *, during_iteration: bool) -> None:
    def read_sql(*args, **kwargs):
        if not during_iteration:
            raise exc

        def stream():
            raise exc
            yield  # pragma: no cover

        return stream()

    monkeypatch.setattr(extract.cx, "read_sql", read_sql)


@pytest.mark.parametrize("during_iteration", [False, True], ids=["on-call", "on-iteration"])
@pytest.mark.parametrize(
    "exc", [PanicException("thread panicked"), RuntimeError("bad column")], ids=["panic", "runtime-error"]
)
def test_connectorx_errors_become_connectorx_failed_and_the_cursor_holds(
    invoke, source, bucket, monkeypatch, capsys, exc, during_iteration
):
    invoke()
    add_row(source, 2, "2026-11-21 08:00:00")
    capsys.readouterr()
    with monkeypatch.context() as m:
        break_connectorx(m, exc, during_iteration=during_iteration)
        with pytest.raises(BaseException) as failure:
            invoke()
    assert type(failure.value).__name__ == "ConnectorXFailed"
    message = str(failure.value)
    assert "transactions" in message
    assert str(exc) in message
    assert '"backend": "pyarrow"' in message
    assert "bronze" in message and "types" in message
    assert landed_ids(bucket) == [1]  # nothing landed
    assert metric(emf(capsys.readouterr().out), "ConnectorXFailed")["table"] == "transactions"
    invoke()  # the cursor held: the row the failed run missed lands now
    assert landed_ids(bucket) == [1, 2]


def test_an_unrelated_error_is_not_rewrapped_as_connectorx_failed(invoke, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("not from connectorx")

    monkeypatch.setattr("db_bronze.source.with_extracted_at", boom)
    with pytest.raises(Exception) as failure:
        invoke()
    assert type(failure.value).__name__ != "ConnectorXFailed"
    assert "not from connectorx" in str(failure.value.__cause__ or failure.value) or "not from connectorx" in str(
        failure.value
    )


def test_an_unrelated_error_inside_the_connectorx_call_is_not_swallowed(invoke, monkeypatch):
    break_connectorx(monkeypatch, ValueError("from elsewhere"), during_iteration=False)
    with pytest.raises(Exception) as failure:
        invoke()
    assert type(failure.value).__name__ != "ConnectorXFailed"


def test_a_backend_change_fails_before_extraction_and_a_new_name_passes(invoke, source, bucket, capsys):
    invoke(backend="pyarrow")
    add_row(source, 2, "2026-11-21 08:00:00")
    capsys.readouterr()
    with pytest.raises(Exception) as failure:
        invoke(backend="connectorx")
    assert type(failure.value).__name__ == "BackendChanged"
    message = str(failure.value)
    assert "transactions" in message and "pyarrow" in message and "connectorx" in message
    assert "types" in message and "name" in message
    assert landed_ids(bucket) == [1]
    assert metric(emf(capsys.readouterr().out), "BackendChanged")["table"] == "transactions"

    invoke(backend="pyarrow")  # going back is not a change
    assert landed_ids(bucket) == [1, 2]

    result = invoke(backend="connectorx", name="transactions_v2")  # a new name has no previous backend
    assert result["rows"] == {"transactions_v2": 2}


def test_the_run_logs_are_structured_json_with_backend_and_row_counts(invoke, caplog):
    caplog.set_level(logging.INFO, logger="db_bronze")
    invoke()
    records = [json.loads(r.getMessage()) for r in caplog.records if r.getMessage().startswith("{")]
    starts = [r for r in records if r.get("event") == "run start"]
    dones = [r for r in records if r.get("event") == "run done"]
    assert len(starts) == 1 and len(dones) == 1
    assert starts[0]["pipeline"] == "core_banking"
    assert dones[0]["backends"] == {"transactions": "connectorx"}
    assert dones[0]["rows"] == {"transactions": 1}


def test_emf_metrics_for_a_successful_run(invoke, source, capsys):
    invoke(cursor=NULLABLE_CURSOR)
    capsys.readouterr()
    with sqlite3.connect(source) as con:
        con.execute('INSERT INTO "Transactions" VALUES (2, NULL), (3, NULL), (4, \'2026-11-22 08:00:00\')')
    capsys.readouterr()
    invoke(cursor=NULLABLE_CURSOR)
    records = emf(capsys.readouterr().out)
    rows = metric(records, "RowsExtracted")
    assert rows["RowsExtracted"] == 3
    assert (rows["pipeline"], rows["table"]) == ("core_banking", "transactions")
    cw = rows["_aws"]["CloudWatchMetrics"][0]
    assert cw["Dimensions"] == [["pipeline", "table"]]
    assert {"Name": "RowsExtracted", "Unit": "Count"} in cw["Metrics"]
    assert metric(records, "NullCursorRows")["NullCursorRows"] == 2


def test_null_cursor_rows_the_run_never_reads_are_counted_in_the_metric(invoke, source, capsys):
    """include_null_cursor false (the default): the NULL-cursor rows are the gap, so the metric shows them."""
    with sqlite3.connect(source) as con:
        con.execute('INSERT INTO "Transactions" VALUES (2, NULL), (3, NULL)')
    capsys.readouterr()
    result = invoke()
    assert result["rows"] == {"transactions": 1}
    assert result["null_cursor_rows"] == {"transactions": 2}
    assert metric(emf(capsys.readouterr().out), "NullCursorRows")["NullCursorRows"] == 2
