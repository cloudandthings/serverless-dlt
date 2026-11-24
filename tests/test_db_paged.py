"""db_bronze: paged snapshots and planned backfills (`plan`, pages, `finalize`), handler-level against SQLite.

DESIGN-db.md D8 and section 7.5. Paged and parallel runs use their own pipeline
name, never the incremental's, so these configs use `core_banking_snapshot`.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_db_incremental import SourceDB, files, ids

from db_bronze import ConfigError
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


def page_event(page: dict, snapshot_id: str = "20261124T093000Z-0123abcd", partition_date: str = "2026-11-24") -> dict:
    return {
        "mode": "full",
        "tables": [page["table"]],
        "page": {"offset": page["offset"], "limit": page["limit"]},
        "snapshot_id": snapshot_id,
        "partition_date": partition_date,
    }


def load_rows(bucket: Path, load_ids: list[str], table: str = "transactions") -> list[dict]:
    """The rows the given loads landed."""
    landed_by = [f for f in files(bucket, table) if f.name.rsplit(".", 2)[0] in load_ids]
    return [r for f in landed_by for r in pq.read_table(f).to_pylist()]


def test_page_lands_its_slice_in_key_order_under_the_partition_date(db, bucket, invoke):
    db.insert(*[(i, f"2026-11-20 08:00:{60 - i:02d}", "JHB", i) for i in range(1, 11)])  # cursor order != key order
    result = invoke(**page_event({"table": "transactions", "offset": 3, "limit": 4}))
    assert result["mode"] == "full" and result["snapshot_id"] == "20261124T093000Z-0123abcd"
    assert ids(load_rows(bucket, result["load_ids"])) == [4, 5, 6, 7]
    assert {f.parent.relative_to(bucket / "core_banking" / "transactions").as_posix() for f in files(bucket)} == {
        "mode=full/year=2026/month=11/day=24"
    }
    assert not (bucket / "core_banking" / "_manifests").exists()  # finalize writes the manifest, not a page


def manifests(bucket: Path) -> dict[str, dict]:
    root = bucket / "core_banking" / "_manifests"
    return {f.relative_to(root).as_posix(): json.loads(f.read_text()) for f in sorted(root.rglob("*.json"))}


def test_plan_every_page_then_finalize_lands_every_row_exactly_once(db, bucket, invoke):
    insert_transactions(db, 10)
    db.insert((11, "2025-06-01 00:00:00", "JHB", 1))  # before initial_value: not part of the snapshot
    plan = invoke(mode="plan", plan="snapshot", tables=["transactions"], page_size=3)
    assert len(plan["pages"]) == 4
    db.insert((12, "2026-11-20 09:00:00", "CPT", 2))  # arrives after the plan: the open-ended last page takes it
    page_results = [invoke(**page_event(p, plan["snapshot_id"], plan["partition_date"])) for p in plan["pages"]]
    load_ids = [load_id for r in page_results for load_id in r["load_ids"]]
    result = invoke(mode="finalize", table="transactions", snapshot_id=plan["snapshot_id"], load_ids=load_ids)

    assert ids(load_rows(bucket, load_ids)) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]
    manifest = manifests(bucket)[f"transactions/{plan['snapshot_id']}.json"]
    assert manifest["table"] == "transactions" and manifest["snapshot_id"] == plan["snapshot_id"]
    assert manifest["load_ids"] == load_ids and len(set(load_ids)) == 4
    assert set(manifests(bucket)) == {f"transactions/{plan['snapshot_id']}.json"}
    assert result["manifest"].endswith(f"_manifests/transactions/{plan['snapshot_id']}.json")
    today = datetime.now(UTC)
    assert {f.parent.relative_to(bucket / "core_banking" / "transactions").as_posix() for f in files(bucket)} == {
        f"mode=full/year={today:%Y}/month={today:%m}/day={today:%d}"
    }


def test_paged_snapshot_table_lands_every_row_exactly_once(db, bucket, invoke):
    db.execute("INSERT INTO \"Branches\" VALUES ('JHB', NULL), ('CPT', NULL), ('DBN', NULL), ('PTA', NULL)")
    plan = invoke(mode="plan", plan="snapshot", tables=["branches"], page_size=2)
    load_ids = [i for p in plan["pages"] for i in invoke(**page_event(p, plan["snapshot_id"]))["load_ids"]]
    invoke(mode="finalize", table="branches", snapshot_id=plan["snapshot_id"], load_ids=load_ids)
    assert sorted(r["BranchCode"] for r in load_rows(bucket, load_ids, "branches")) == ["CPT", "DBN", "JHB", "PTA"]
    assert manifests(bucket)[f"branches/{plan['snapshot_id']}.json"]["load_ids"] == load_ids


@pytest.mark.parametrize(
    "event",
    [
        page_event({"table": "branches", "offset": 0, "limit": 2}),
        {"mode": "plan", "plan": "snapshot", "tables": ["branches"], "page_size": 2},
    ],
    ids=["page", "plan"],
)
def test_paging_a_table_without_primary_key_or_order_by_is_rejected(db, bucket, config, invoke, event):
    config["tables"] = [TRANSACTIONS, {k: v for k, v in BRANCHES.items() if k != "primary_key"}]
    db.execute("INSERT INTO \"Branches\" VALUES ('JHB', NULL)")
    with pytest.raises(ConfigError, match=r"table branches: .*no stable order.*\"primary_key\".*\"order_by\""):
        invoke(**event)
    assert not any(bucket.iterdir())


def test_page_orders_by_keyset_order_by_when_set(db, bucket, config, invoke):
    keyset = {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 100}
    config["tables"] = [TRANSACTIONS | {"chunking": keyset}]
    db.insert(*[(i, f"2026-11-20 08:00:{60 - i:02d}", "JHB", i) for i in range(1, 11)])  # cursor order != key order
    result = invoke(**page_event({"table": "transactions", "offset": 0, "limit": 3}))
    assert ids(load_rows(bucket, result["load_ids"])) == [8, 9, 10]
