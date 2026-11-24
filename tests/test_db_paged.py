"""db_bronze: paged snapshots and planned backfills (`plan`, pages, `finalize`), handler-level against SQLite.

DESIGN-db.md D8 and section 7.5. Paged and parallel runs use their own pipeline
name, never the incremental's, so these configs use `core_banking_snapshot`.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_db_incremental import SourceDB, ids

from db_bronze.handler import handler

TRANSACTIONS = {
    "name": "transactions",
    "table": "Transactions",
    "primary_key": ["TransactionId"],
    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
}
BRANCHES = {"name": "branches", "table": "Branches", "primary_key": ["BranchCode"]}  # a snapshot table
PIPELINE = "core_banking_snapshot"


@pytest.fixture
def db(tmp_path: Path) -> SourceDB:
    db = SourceDB(tmp_path / "source.sqlite")
    db.execute('CREATE TABLE "Branches" ("BranchCode" TEXT PRIMARY KEY, "OpenedAt" DATETIME)')
    return db


@pytest.fixture(params=["pyarrow", "connectorx"])
def backend(request) -> str:
    """Every handler-level scenario runs on every implemented backend."""
    return request.param


@pytest.fixture
def config(db: SourceDB, bucket: Path, backend: str) -> dict:
    return {
        "pipeline_name": PIPELINE,
        "dataset_name": "core_banking",
        "bucket_url": bucket.as_uri(),
        "connection": {"dialect": "sqlite", "path": str(db.path)},
        "defaults": {"backend": backend},
        "tables": [TRANSACTIONS, BRANCHES],
    }


@pytest.fixture
def invoke(config: dict, tmp_path: Path, pipelines_dir: str, monkeypatch):
    """Write `config` as source_id "core" and return a function that invokes the handler with an event."""
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())

    def run(**event) -> dict:
        (config_dir / "core.json").write_text(json.dumps(config))
        return handler({"source_id": "core", **event}, None)

    return run


def insert_transactions(db: SourceDB, n: int) -> None:
    db.insert(*[(i, f"2026-11-20 08:00:{i:02d}", "JHB", i * 100) for i in range(1, n + 1)])


def test_plan_snapshot_pages_every_table_with_an_open_ended_last_page(db, bucket, invoke):
    insert_transactions(db, 10)
    db.execute("INSERT INTO \"Branches\" VALUES ('JHB', '1990-01-01 00:00:00')")
    plan = invoke(mode="plan", plan="snapshot", page_size=4)
    assert plan["pages"] == [
        {"table": "transactions", "offset": 0, "limit": 4},
        {"table": "transactions", "offset": 4, "limit": 4},
        {"table": "transactions", "offset": 8, "limit": None},
        {"table": "branches", "offset": 0, "limit": None},
    ]
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", plan["snapshot_id"])
    assert plan["partition_date"] == f"{datetime.now(UTC):%Y-%m-%d}"
    assert not any(bucket.iterdir())  # a plan extracts nothing and writes nothing
