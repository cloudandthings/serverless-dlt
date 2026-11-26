"""db_bronze integration: paged snapshots and planned backfills (`plan`, pages, `finalize`) on real engines.

docs/db/design.md D8 and docs/db/modes.md. Paged and parallel runs use a pipeline name of
their own, as in the SQLite suite.
"""

from __future__ import annotations

from itertools import pairwise

import pytest
import sqlalchemy as sa

from .conftest import today_partition

pytestmark = pytest.mark.integration

CURSOR = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}
SNAPSHOT_PIPELINE = {"pipeline_name": "it_snapshot"}


@pytest.fixture
def tx(backend: str) -> dict:
    return {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": CURSOR,
        "backend": backend,
    }


def page_event(page: dict, snapshot_id: str, partition_date: str) -> dict:
    return {
        "source_id": "core",
        "mode": "full",
        "tables": [page["table"]],
        "page": {"offset": page["offset"], "limit": page["limit"]},
        "snapshot_id": snapshot_id,
        "partition_date": partition_date,
    }


def run_snapshot(run, tables: list[dict], only: str, page_size: int, after_plan=None) -> tuple[dict, list[str]]:
    """plan → every page → finalize for table `only`; returns (the plan, the load ids)."""
    plan = run({"source_id": "core", "mode": "plan", "plan": "snapshot", "tables": [only], "page_size": page_size},
               *tables, **SNAPSHOT_PIPELINE)
    if after_plan:
        after_plan()
    load_ids = [
        load_id
        for page in plan["pages"]
        for load_id in run(page_event(page, plan["snapshot_id"], plan["partition_date"]), *tables,
                           **SNAPSHOT_PIPELINE)["load_ids"]
    ]
    run({"source_id": "core", "mode": "finalize", "table": only, "snapshot_id": plan["snapshot_id"],
         "load_ids": load_ids}, *tables, **SNAPSHOT_PIPELINE)
    return plan, load_ids


def test_plan_every_page_then_finalize_lands_every_row_exactly_once(source, run, tx):
    source.transactions(*[(i, f"2026-11-20 08:00:{i:02d}", "JHB", i * 100) for i in range(1, 11)])
    source.transactions((11, "2025-06-01 00:00:00", "JHB", 1))  # before initial_value: not part of the snapshot

    def arrives_after_the_plan():  # the open-ended last page takes it
        source.transactions((12, "2026-11-20 09:00:00", "CPT", 2))

    plan, load_ids = run_snapshot(run, [tx], "transactions", 3, arrives_after_the_plan)
    assert [p["limit"] for p in plan["pages"]] == [3, 3, 3, None]
    rows = source.landed("transactions", load_ids=load_ids)
    assert sorted(r["TransactionId"] for r in rows) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]  # none twice
    assert source.manifests()[f"transactions/{plan['snapshot_id']}.json"]["load_ids"] == load_ids
    assert source.partitions("transactions") == {today_partition("full")}


def test_paged_snapshot_table_without_a_cursor_lands_every_row_exactly_once(source, run, backend):
    table = source.create(
        lambda metadata: sa.Table("Branches", metadata, sa.Column("BranchCode", sa.String(10), primary_key=True))
    )
    source.insert(table, [{"BranchCode": c} for c in ("JHB", "CPT", "DBN", "PTA", "PLZ")])
    branches = {"name": "branches", "table": "Branches", "primary_key": ["BranchCode"], "backend": backend}
    plan, load_ids = run_snapshot(run, [branches], "branches", 2)
    assert len(plan["pages"]) == 3
    landed = sorted(r["BranchCode"] for r in source.landed("branches", load_ids=load_ids))
    assert landed == ["CPT", "DBN", "JHB", "PLZ", "PTA"]


def test_page_orders_by_keyset_order_by_when_set(source, run, tx):
    keyset = {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 100}
    # cursor order is the reverse of key order
    source.transactions(*[(i, f"2026-11-20 08:00:{60 - i:02d}", "JHB", i) for i in range(1, 11)])
    page = {"table": "transactions", "offset": 0, "limit": 3}
    result = run(page_event(page, "20261124T093000Z-0123abcd", "2026-11-24"), tx | {"chunking": keyset},
                 **SNAPSHOT_PIPELINE)
    assert source.landed_ids("transactions", "TransactionId", load_ids=result["load_ids"]) == [8, 9, 10]
    assert source.partitions("transactions") == {"mode=full/year=2026/month=11/day=24"}


def test_plan_backfill_splits_min_to_max_into_contiguous_utc_windows_that_land_every_row(source, run, tx):
    source.transactions(
        (1, "2026-03-01 02:00:00", "JHB", 1),  # MIN: 2026-03-01T00:00:00Z in Johannesburg
        (2, "2026-03-03 09:30:00", "CPT", 2),
        (3, "2026-03-05 05:00:00", "DBN", 3),
        (4, "2026-03-07 02:00:00", "PTA", 4),  # MAX: 2026-03-07T00:00:00Z
    )
    joburg = {"naive_timezone": "Africa/Johannesburg"}
    plan = run({"source_id": "core", "mode": "plan", "plan": "backfill", "windows": 3}, tx, connection=joburg,
               **SNAPSHOT_PIPELINE)
    windows = plan["windows"]
    assert [w["window_start"] for w in windows] == [
        "2026-03-01T00:00:00+00:00", "2026-03-03T00:00:00+00:00", "2026-03-05T00:00:00+00:00"
    ]
    assert windows[-1]["window_end"] == "2026-03-07T00:01:00+00:00"  # just past MAX
    for before, after in pairwise(windows):
        assert before["window_end"] == after["window_start"]
    for w in windows:
        run({"source_id": "core", "mode": "backfill", "window_start": w["window_start"],
             "window_end": w["window_end"]}, tx, connection=joburg, **SNAPSHOT_PIPELINE)
    assert source.landed_ids("transactions", "TransactionId", mode="backfill") == [1, 2, 3, 4]


def test_plan_backfill_on_an_integer_cursor_splits_by_window_size(source, run, tx):
    table = tx | {"cursor": {"column": "TransactionId", "type": "integer", "initial_value": 0}}
    source.transactions(*[(i, "2026-11-20 08:00:00", "JHB", i) for i in range(3, 11)])
    plan = run({"source_id": "core", "mode": "plan", "plan": "backfill", "window_size": 3}, table,
               **SNAPSHOT_PIPELINE)
    assert [(w["window_start"], w["window_end"]) for w in plan["windows"]] == [(3, 6), (6, 9), (9, 11)]
    for w in plan["windows"]:
        run({"source_id": "core", "mode": "backfill", "window_start": w["window_start"],
             "window_end": w["window_end"]}, table, **SNAPSHOT_PIPELINE)
    assert source.landed_ids("transactions", "TransactionId") == list(range(3, 11))


def test_plan_backfill_over_an_empty_table_has_no_windows(source, run, tx):
    source.transactions()
    plan = run({"source_id": "core", "mode": "plan", "plan": "backfill", "window_size": 3600}, tx,
               **SNAPSHOT_PIPELINE)
    assert plan["windows"] == []
    assert source.nothing_landed()
