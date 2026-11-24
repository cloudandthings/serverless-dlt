"""db_bronze: `full` and `backfill` modes and table subsets, handler-level against SQLite (DESIGN-db.md 7.5)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_db_incremental import SourceDB, files, ids, landed

from db_bronze import ConfigError
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

    def run(config_tables: list[dict] | None = None, **event) -> dict:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(db.path)},
            "defaults": {"backend": backend},
            "tables": config_tables or [TRANSACTIONS],
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


def landed_ids(bucket: Path, mode: str) -> list[int]:
    return ids([r for f in files(bucket) if f"/mode={mode}/" in f.as_posix() for r in pq.read_table(f).to_pylist()])


def test_incremental_after_full_continues_from_the_previous_incremental_cursor(db, bucket, invoke):
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200))
    invoke()
    db.insert((3, "2026-11-20 10:00:00", "DBN", 300))
    invoke(mode="full")
    db.insert((4, "2026-11-20 11:00:00", "PTA", 400))
    result = invoke()
    assert result["rows"] == {"transactions": 2}
    assert landed_ids(bucket, "incremental") == [1, 2, 3, 4]
    assert landed_ids(bucket, "full") == [1, 2, 3]


BRANCHES = {"name": "branches", "table": "Branches", "primary_key": ["BranchCode"]}  # no cursor: a snapshot table


@pytest.fixture
def branches(db: SourceDB) -> SourceDB:
    db.execute('CREATE TABLE "Branches" ("BranchCode" TEXT PRIMARY KEY, "OpenedAt" DATETIME)')
    db.execute("INSERT INTO \"Branches\" VALUES ('JHB', '1990-01-01 00:00:00'), ('CPT', '2999-01-01 00:00:00')")
    return db


def test_full_lands_everything_for_a_snapshot_table(branches, bucket, invoke):
    result = invoke(config_tables=[TRANSACTIONS, BRANCHES], mode="full")
    assert result["rows"]["branches"] == 2
    assert sorted(r["BranchCode"] for r in landed(bucket, "branches")) == ["CPT", "JHB"]
    assert partitions(bucket, "branches") == {today_partition("full")}


def test_snapshot_table_in_incremental_fails_with_config_error_and_nothing_lands(branches, bucket, invoke):
    with pytest.raises(ConfigError, match=r"mode=incremental.*\['branches'\].*mode=full"):
        invoke(config_tables=[TRANSACTIONS, BRANCHES])
    assert files(bucket) == [] and files(bucket, "branches") == []


def test_tables_subset_runs_only_the_named_tables(branches, bucket, invoke):
    db = branches
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    result = invoke(config_tables=[TRANSACTIONS, BRANCHES], tables=["transactions"])
    assert set(result["rows"]) == {"transactions"}
    assert ids(landed(bucket)) == [1] and files(bucket, "branches") == []
    invoke(config_tables=[TRANSACTIONS, BRANCHES], mode="full", tables=["branches"])
    assert partitions(bucket) == {today_partition("incremental")}
    assert partitions(bucket, "branches") == {today_partition("full")}


def test_unknown_table_in_subset_fails_with_config_error(branches, bucket, invoke):
    with pytest.raises(ConfigError, match=r"unknown tables \['transaction'\]"):
        invoke(config_tables=[TRANSACTIONS, BRANCHES], mode="full", tables=["transaction"])
    assert files(bucket) == [] and files(bucket, "branches") == []
