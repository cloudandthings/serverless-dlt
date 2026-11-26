"""db_bronze: `full` and `backfill` modes and table subsets, handler-level against SQLite (docs/db/modes.md)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_db_incremental import DDL, SourceDB, files, ids, landed

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

    def run(config_tables: list[dict] | None = None, connection: dict | None = None, **event) -> dict:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(db.path)} | (connection or {}),
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
    with pytest.raises(ConfigError, match=r"table branches has no cursor.*'incremental'"):
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


def test_backfill_timestamp_window_lands_the_window_dated_by_window_start(db, bucket, invoke):
    db.insert(
        (1, "2026-02-28 23:59:59", "JHB", 100),
        (2, "2026-03-01 00:00:01", "CPT", 200),
        (3, "2026-03-31 23:59:59", "DBN", 300),
        (4, "2026-04-01 00:00:01", "PTA", 400),
    )
    result = invoke(mode="backfill", window_start="2026-03-01T00:00:00Z", window_end="2026-04-01T00:00:00Z")
    assert result["mode"] == "backfill"
    assert ids(landed(bucket)) == [2, 3]
    assert partitions(bucket) == {"mode=backfill/year=2026/month=03/day=01"}


INTEGER_CURSOR = TRANSACTIONS | {"cursor": {"column": "TransactionId", "type": "integer", "initial_value": 0}}


def test_backfill_integer_window_lands_the_window(db, bucket, invoke):
    db.insert(*[(i, "2026-11-20 08:00:00", "JHB", i) for i in range(1, 7)])
    result = invoke(config_tables=[INTEGER_CURSOR], mode="backfill", window_start=2, window_end=5)
    assert result["rows"] == {"transactions": 3}
    assert ids(landed(bucket)) == [2, 3, 4]
    assert partitions(bucket) == {today_partition("backfill")}  # no date in an integer window: the load date


def test_incremental_after_backfill_continues_from_the_previous_incremental_cursor(db, bucket, invoke):
    db.insert((1, "2026-03-10 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200))
    invoke()
    db.insert((3, "2026-11-20 10:00:00", "DBN", 300))
    invoke(mode="backfill", window_start="2026-03-01T00:00:00Z", window_end="2026-04-01T00:00:00Z")
    db.insert((4, "2026-11-20 11:00:00", "PTA", 400))
    result = invoke()
    assert result["rows"] == {"transactions": 2}
    assert landed_ids(bucket, "incremental") == [1, 2, 3, 4]
    assert landed_ids(bucket, "backfill") == [1]


@pytest.mark.parametrize(
    ("window", "message"),
    [
        ({}, "needs window_start and window_end"),
        ({"window_start": 1, "window_end": "2026-01-01T00:00:00Z"}, "both be integers or both ISO-8601"),
        ({"window_start": "2026-02-01T00:00:00Z", "window_end": "2026-01-01T00:00:00Z"}, "must be after"),
        ({"window_start": 2, "window_end": 5}, "must be an ISO-8601 timestamp for the timestamp cursor"),
    ],
)
def test_bad_backfill_window_fails_with_config_error(db, bucket, invoke, window, message):
    with pytest.raises(ConfigError, match=message):
        invoke(mode="backfill", **window)
    assert files(bucket) == []


def test_snapshot_table_in_backfill_fails_with_config_error(branches, bucket, invoke):
    window = {"window_start": "2026-03-01T00:00:00Z", "window_end": "2026-04-01T00:00:00Z"}
    with pytest.raises(ConfigError, match=r"table branches has no cursor.*'backfill'"):
        invoke(config_tables=[TRANSACTIONS, BRANCHES], mode="backfill", **window)
    assert files(bucket) == [] and files(bucket, "branches") == []


def test_full_on_an_integer_cursor_lands_from_initial_value_and_leaves_the_cursor(db, bucket, invoke):
    cursor_table = INTEGER_CURSOR | {"cursor": INTEGER_CURSOR["cursor"] | {"initial_value": 2}}
    db.insert(*[(i, "2026-11-20 08:00:00", "JHB", i) for i in range(1, 4)])
    invoke(config_tables=[cursor_table])
    db.insert((4, "2026-11-20 08:00:00", "JHB", 4))
    invoke(config_tables=[cursor_table], mode="full")
    assert landed_ids(bucket, "full") == [2, 3, 4]
    db.insert((5, "2026-11-20 08:00:00", "JHB", 5))
    invoke(config_tables=[cursor_table])
    assert landed_ids(bucket, "incremental") == [2, 3, 4, 5]


def manifests(bucket: Path) -> dict[str, dict]:
    root = bucket / "core_banking" / "_manifests"
    return {f.relative_to(root).as_posix(): json.loads(f.read_text()) for f in sorted(root.rglob("*.json"))}


def file_load_ids(bucket: Path, table: str, mode: str) -> set[str]:
    """Load ids from data file names (`<load_id>.<file_id>.parquet`)."""
    return {f.name.rsplit(".", 2)[0] for f in files(bucket, table) if f"/mode={mode}/" in f.as_posix()}


def test_full_run_writes_one_manifest_per_table_listing_exactly_its_load_ids(branches, bucket, invoke):
    db = branches
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    invoke(config_tables=[TRANSACTIONS, BRANCHES], tables=["transactions"])  # incremental: no manifest
    assert manifests(bucket) == {}
    first = invoke(config_tables=[TRANSACTIONS, BRANCHES], mode="full")
    second = invoke(config_tables=[TRANSACTIONS, BRANCHES], mode="full")
    assert first["snapshot_id"] != second["snapshot_id"]
    found = manifests(bucket)
    assert len(found) == 4
    for result in (first, second):
        for table in ("transactions", "branches"):
            manifest = found[f"{table}/{result['snapshot_id']}.json"]
            assert manifest["table"] == table and manifest["snapshot_id"] == result["snapshot_id"]
            assert manifest["load_ids"] == result["load_ids"]
    landed_full = file_load_ids(bucket, "transactions", "full")
    assert landed_full == set(first["load_ids"]) | set(second["load_ids"])
    assert set(first["load_ids"]).isdisjoint(second["load_ids"])


def test_failed_full_run_writes_no_manifest(db, bucket, invoke):
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    invoke()
    db.execute('ALTER TABLE "Transactions" RENAME TO "Old"')  # a type change fails the run (schema freeze)
    db.execute(DDL.replace('"amountCents" INTEGER', '"amountCents" TEXT'))
    db.execute('INSERT INTO "Transactions" VALUES (2, \'2026-11-21 08:00:00\', \'CPT\', \'two hundred\')')
    with pytest.raises(Exception, match="amountCents"):
        invoke(mode="full")
    assert manifests(bucket) == {}
    assert file_load_ids(bucket, "transactions", "full") == set()


def test_backfill_window_bounds_are_converted_through_naive_timezone(db, bucket, invoke):
    db.insert(
        (1, "2026-02-28 23:30:00", "JHB", 100),  # inside the window if the bounds were read as UTC
        (2, "2026-03-01 00:30:00", "CPT", 200),
        (3, "2026-03-02 00:30:00", "DBN", 300),
    )
    invoke(
        connection={"naive_timezone": "Africa/Johannesburg"},
        mode="backfill",
        window_start="2026-02-28T22:00:00Z",  # 2026-03-01 00:00 in Johannesburg
        window_end="2026-03-02T00:00:00+02:00",
    )
    assert ids(landed(bucket)) == [2]


def test_snapshot_table_honours_columns_and_where(branches, bucket, invoke):
    snapshot = BRANCHES | {"columns": ["BranchCode"], "where": "\"OpenedAt\" < '2000-01-01'"}
    invoke(config_tables=[snapshot], mode="full")
    assert [{k: v for k, v in r.items() if k != "_extracted_at"} for r in landed(bucket, "branches")] == [
        {"BranchCode": "JHB"}
    ]


def test_snapshot_query_table(branches, bucket, invoke):
    snapshot = {"name": "branches", "query": 'SELECT "BranchCode" AS code FROM "Branches"'}
    invoke(config_tables=[snapshot], mode="full")
    assert sorted(r["code"] for r in landed(bucket, "branches")) == ["CPT", "JHB"]
