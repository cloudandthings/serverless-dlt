"""db_bronze: `full` and `backfill` modes and table subsets, handler-level against SQLite (DESIGN-db.md 7.5)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_db_incremental import SourceDB, files, ids, landed

from db_bronze.handler import handler

TRANSACTIONS = {
    "name": "transactions",
    "table": "Transactions",
    "primary_key": ["TransactionId"],
    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
}


@pytest.fixture
def db(tmp_path: Path) -> SourceDB:
    return SourceDB(tmp_path / "source.sqlite")


@pytest.fixture(params=["pyarrow", "connectorx"])
def backend(request) -> str:
    """Every handler-level scenario runs on every implemented backend."""
    return request.param


@pytest.fixture
def invoke(db: SourceDB, bucket: Path, tmp_path: Path, pipelines_dir: str, backend: str, monkeypatch):
    """Return a function that writes the config for source_id "core" and invokes the handler with `event`."""
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())

    def run(tables: list[dict] | None = None, **event) -> dict:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(db.path)},
            "defaults": {"backend": backend},
            "tables": tables or [TRANSACTIONS],
        }
        (config_dir / "core.json").write_text(json.dumps(cfg))
        return handler({"source_id": "core", **event}, None)

    return run


def partitions(bucket: Path, table: str = "transactions") -> set[str]:
    return {f.parent.relative_to(bucket / "core_banking" / table).as_posix() for f in files(bucket, table)}


def today_partition(mode: str) -> str:
    today = datetime.now(UTC)
    return f"mode={mode}/year={today:%Y}/month={today:%m}/day={today:%d}"


def test_full_lands_from_initial_value_to_run_start_under_mode_full(db, bucket, invoke):
    db.insert(
        (1, "2025-12-31 23:59:59", "JHB", 100),  # before initial_value
        (2, "2026-01-01 00:00:01", "CPT", 200),
        (3, "2026-11-20 10:00:00", "DBN", 300),
        (4, "2999-01-01 00:00:00", "PTA", 400),  # after run start
    )
    result = invoke(mode="full")
    assert result["mode"] == "full"
    assert result["rows"] == {"transactions": 2}
    assert ids(landed(bucket)) == [2, 3]
    assert partitions(bucket) == {today_partition("full")}
