"""db_bronze: preflight and mode=validate through the handler, on SQLite."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

import pytest

from db_bronze.errors import DbBronzeError
from db_bronze.handler import handler


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.sqlite"
    with sqlite3.connect(path) as con:
        con.execute(
            'CREATE TABLE "Transactions" ("TransactionId" INTEGER PRIMARY KEY, "UpdatedAt" DATETIME, '
            '"Amount" DECIMAL(38,12), "Fee" DECIMAL(40,2))'
        )
        con.execute("INSERT INTO \"Transactions\" VALUES (1, '2026-11-20 08:00:00', 1.5, 2)")
        con.execute("INSERT INTO \"Transactions\" VALUES (2, NULL, 1.5, 2)")
        con.execute("INSERT INTO \"Transactions\" VALUES (3, NULL, 1.5, 2)")
    return path


@pytest.fixture
def invoke(source: Path, bucket: Path, tmp_path: Path, pipelines_dir: str, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    configs = tmp_path / "configs"
    configs.mkdir()
    monkeypatch.setenv("CONFIG_URI", configs.as_uri())

    def run(event: dict | None = None, **table) -> dict:
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
        return handler({"source_id": "core"} | (event or {}), None)

    return run


def nothing_landed(bucket: Path) -> bool:
    return not any(p.is_file() for p in bucket.rglob("*"))


def test_a_wide_decimal_on_connectorx_fails_preflight_listing_every_column(invoke, bucket):
    with pytest.raises(Exception) as failure:
        invoke()
    assert type(failure.value).__name__ == "PreflightError"
    message = str(failure.value)
    assert "Amount" in message and "Fee" in message  # every offender, not just the first
    assert '"backend": "pyarrow"' in message
    assert nothing_landed(bucket)


def emf(out: str) -> list[dict]:
    return [d for line in out.splitlines() if line.startswith("{") and "_aws" in (d := json.loads(line))]


def test_a_preflight_failure_emits_the_preflight_failed_metric_and_logs_the_error(invoke, capsys, caplog):
    capsys.readouterr()
    with caplog.at_level(logging.ERROR, logger="db_bronze"), pytest.raises(DbBronzeError) as failure:
        invoke()
    assert type(failure.value).__name__ == "PreflightError"
    metrics = [r for r in emf(capsys.readouterr().out) if "PreflightFailed" in r]
    assert [(m["pipeline"], m["PreflightFailed"]) for m in metrics] == [("core_banking", 1)]
    assert any('"error": "PreflightError"' in r.getMessage() for r in caplog.records)


def test_the_same_table_on_pyarrow_passes(invoke, bucket):
    assert invoke(backend="pyarrow")["rows"] == {"transactions": 1}


def test_excluding_the_columns_passes(invoke):
    assert invoke(columns=["TransactionId", "UpdatedAt"])["rows"] == {"transactions": 1}


def test_validate_reports_per_table_and_writes_nothing(invoke, bucket):
    report = invoke({"mode": "validate"})
    (table,) = report["tables"]
    assert table["name"] == "transactions"
    assert table["backend"] == "connectorx"
    assert len(table["findings"]) == 2
    assert table["null_cursor_count"] == 2
    assert report["ok"] is False
    assert nothing_landed(bucket)


def test_validate_on_a_clean_table_is_ok(invoke, bucket):
    report = invoke({"mode": "validate"}, columns=["TransactionId", "UpdatedAt"])
    assert report["ok"] is True
    assert report["tables"][0]["findings"] == []
    assert nothing_landed(bucket)


def test_validate_needs_no_window_and_does_not_need_a_cursor(invoke):
    report = invoke({"mode": "validate"}, cursor=None, columns=["TransactionId"])
    assert report["tables"][0]["null_cursor_count"] is None


def test_the_null_cursor_count_is_logged_on_a_run(invoke, caplog):
    with caplog.at_level(logging.INFO):
        invoke(backend="pyarrow")
    assert any('"null_cursor_count": 2' in r.getMessage() for r in caplog.records)


def test_no_null_cursor_count_when_nulls_are_included(invoke):
    cursor = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01", "include_null_cursor": True}
    report = invoke({"mode": "validate"}, cursor=cursor, backend="pyarrow")
    assert report["tables"][0]["null_cursor_count"] is None

