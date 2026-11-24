"""db_bronze integration: the run modes, cursors and table options of the SQLite suite, on real engines.

Every test runs on each engine and on both backends (DESIGN-db.md 7.5, 7.3).
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

from .conftest import fails, today_partition

pytestmark = pytest.mark.integration

CURSOR = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}
INTEGER_CURSOR = {"column": "TransactionId", "type": "integer", "initial_value": 0}
WINDOW = {"window_start": "2026-03-01T00:00:00Z", "window_end": "2026-04-01T00:00:00Z"}


@pytest.fixture
def tx(backend: str) -> dict:
    return {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": CURSOR,
        "backend": backend,
    }


@pytest.fixture
def branches(source, backend: str) -> dict:
    """A snapshot table (no cursor) with two rows, and its config."""
    table = source.create(
        lambda metadata: sa.Table(
            "Branches",
            metadata,
            sa.Column("BranchCode", sa.String(10), primary_key=True),
            sa.Column("OpenedAt", sa.DateTime),
        )
    )
    source.insert(
        table,
        [
            {"BranchCode": "JHB", "OpenedAt": datetime(1990, 1, 1)},
            {"BranchCode": "CPT", "OpenedAt": datetime(2999, 1, 1)},
        ],
    )
    return {"name": "branches", "table": "Branches", "primary_key": ["BranchCode"], "backend": backend}


def ids(source, **only) -> list[int]:
    return source.landed_ids("transactions", "TransactionId", **only)


def test_full_lands_from_initial_value_to_run_start_under_mode_full_with_a_manifest(source, run, tx):
    source.transactions(
        (1, "2025-12-31 23:59:59", "JHB", 100),  # before initial_value
        (2, "2026-01-01 00:00:01", "CPT", 200),
        (3, "2026-11-20 10:00:00", "DBN", 300),
        (4, "2999-01-01 00:00:00", "PTA", 400),  # after run start
    )
    result = run({"source_id": "core", "mode": "full"}, tx)
    assert result["rows"] == {"transactions": 2}
    assert ids(source) == [2, 3]
    assert source.partitions("transactions") == {today_partition("full")}
    manifest = source.manifests()[f"transactions/{result['snapshot_id']}.json"]
    assert manifest["load_ids"] == result["load_ids"]


def test_incremental_after_full_and_backfill_continues_from_the_incremental_cursor(source, run, tx):
    source.transactions((1, "2026-03-10 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200))
    run({"source_id": "core"}, tx)
    source.transactions((3, "2026-11-20 10:00:00", "DBN", 300))
    run({"source_id": "core", "mode": "full"}, tx)
    run({"source_id": "core", "mode": "backfill", **WINDOW}, tx)
    source.transactions((4, "2026-11-20 11:00:00", "PTA", 400))
    assert run({"source_id": "core"}, tx)["rows"] == {"transactions": 2}
    assert ids(source, mode="incremental") == [1, 2, 3, 4]
    assert ids(source, mode="full") == [1, 2, 3]
    assert ids(source, mode="backfill") == [1]


def test_a_snapshot_table_lands_whole_in_full_and_fails_in_incremental(source, run, tx, branches):
    source.transactions()
    result = run({"source_id": "core", "mode": "full"}, tx, branches)
    assert result["rows"]["branches"] == 2
    assert sorted(r["BranchCode"] for r in source.landed("branches")) == ["CPT", "JHB"]
    with fails("ConfigError", r"table branches has no cursor.*'incremental'"):
        run({"source_id": "core"}, tx, branches)


def test_tables_subset_runs_only_the_named_tables(source, run, tx, branches):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100))
    assert set(run({"source_id": "core", "tables": ["transactions"]}, tx, branches)["rows"]) == {"transactions"}
    assert source.files("branches") == []


def test_backfill_timestamp_window_lands_the_window_dated_by_window_start(source, run, tx):
    source.transactions(
        (1, "2026-02-28 23:59:59", "JHB", 100),
        (2, "2026-03-01 00:00:00", "CPT", 200),  # the start is inclusive
        (3, "2026-03-31 23:59:59", "DBN", 300),
        (4, "2026-04-01 00:00:00", "PTA", 400),  # the end is exclusive
    )
    result = run({"source_id": "core", "mode": "backfill", **WINDOW}, tx)
    assert result["mode"] == "backfill"
    assert ids(source) == [2, 3]
    assert source.partitions("transactions") == {"mode=backfill/year=2026/month=03/day=01"}


def test_backfill_integer_window_lands_the_window(source, run, tx):
    source.transactions(*[(i, "2026-11-20 08:00:00", "JHB", i) for i in range(1, 7)])
    result = run({"source_id": "core", "mode": "backfill", "window_start": 2, "window_end": 5}, tx | {
        "cursor": INTEGER_CURSOR
    })
    assert result["rows"] == {"transactions": 3}
    assert ids(source) == [2, 3, 4]


def test_backfill_window_bounds_are_converted_through_naive_timezone(source, run, tx):
    source.transactions(
        (1, "2026-02-28 23:30:00", "JHB", 100),  # inside the window if the bounds were read as UTC
        (2, "2026-03-01 00:30:00", "CPT", 200),
        (3, "2026-03-02 00:30:00", "DBN", 300),
    )
    window = {"window_start": "2026-02-28T22:00:00Z", "window_end": "2026-03-02T00:00:00+02:00"}
    run(
        {"source_id": "core", "mode": "backfill", **window}, tx, connection={"naive_timezone": "Africa/Johannesburg"}
    )
    assert ids(source) == [2]


def test_integer_cursor_incremental(source, run, tx):
    table = tx | {"cursor": INTEGER_CURSOR | {"initial_value": 2}}
    source.transactions(*[(i, "2026-11-20 08:00:00", "JHB", i) for i in range(1, 4)])
    run({"source_id": "core"}, table)
    source.transactions((4, "2026-11-20 08:00:00", "JHB", 4))
    assert run({"source_id": "core"}, table)["rows"] == {"transactions": 1}
    assert ids(source) == [2, 3, 4]


def test_without_lag_a_new_row_tying_the_boundary_lands_and_a_landed_one_does_not(source, run, tx):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200))
    run({"source_id": "core"}, tx)
    source.transactions((3, "2026-11-20 09:00:00", "DBN", 300))  # ties the last cursor value
    assert run({"source_id": "core"}, tx)["rows"] == {"transactions": 1}
    assert ids(source) == [1, 2, 3]


def test_lag_lands_a_late_commit_and_re_lands_the_lag_window_on_every_run(source, run, tx):
    """Prototype S4: with `lag`, dlt deduplicates nothing at the boundary (DESIGN-db.md 7.3, 12)."""
    table = tx | {"cursor": CURSOR | {"lag": 10}}
    source.transactions(*[(i, f"2026-11-20 08:00:{i * 10:02}", "JHB", i) for i in range(1, 6)])  # :10 .. :50
    run({"source_id": "core"}, table)
    source.transactions(
        (6, "2026-11-20 08:00:50", "JHB", 6),  # ties the boundary
        (7, "2026-11-20 08:00:45", "JHB", 7),  # committed late, inside the lag window
        (8, "2026-11-20 08:01:00", "JHB", 8),
    )
    r2 = run({"source_id": "core"}, table)
    assert ids(source, load_ids=r2["load_ids"]) == [4, 5, 6, 7, 8]  # window [08:00:40, ...)
    r3 = run({"source_id": "core"}, table)  # no source changes
    assert ids(source, load_ids=r3["load_ids"]) == [5, 6, 8]  # window [08:00:50, ...) lands again


def test_include_null_cursor_lands_null_cursor_rows_on_every_run(source, run, tx):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100), (2, None, "CPT", 200), nullable_cursor=True)
    table = tx | {"cursor": CURSOR | {"include_null_cursor": True}}
    r1 = run({"source_id": "core"}, table)
    assert ids(source, load_ids=r1["load_ids"]) == [1, 2]
    source.transactions((3, "2026-11-21 08:00:00", "DBN", 300))
    r2 = run({"source_id": "core"}, table)
    assert ids(source, load_ids=r2["load_ids"]) == [2, 3]


def test_null_cursor_rows_are_excluded_by_default(source, run, tx):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100), (2, None, "CPT", 200), nullable_cursor=True)
    run({"source_id": "core"}, tx)
    assert ids(source) == [1]


def test_where_and_columns(source, run, tx):
    source.transactions(
        (1, "2025-12-31 23:00:00", "JHB", 100),  # before initial_value
        (2, "2026-11-20 08:00:00", "TEST", 200),
        (3, "2026-11-20 09:00:00", "CPT", 300),
    )
    table = tx | {"where": f"{source.quote('Branch Code')} <> 'TEST'", "columns": ["TransactionId", "UpdatedAt"]}
    run({"source_id": "core"}, table)
    assert ids(source) == [3]
    assert pq.read_schema(source.files("transactions")[0]).names == ["TransactionId", "UpdatedAt", "_extracted_at"]


def test_query_table_with_a_cursor(source, run, tx):
    q = source.quote
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200))
    source.create(
        lambda metadata: sa.Table(
            "Branches", metadata, sa.Column("Code", sa.String(10), primary_key=True), sa.Column("Region", sa.String(20))
        )
    )
    source.insert_into("Branches", [{"Code": "JHB", "Region": "Gauteng"}, {"Code": "CPT", "Region": "Western Cape"}])
    query = (
        f"SELECT t.{q('TransactionId')}, t.{q('UpdatedAt')}, b.{q('Region')} "
        f"FROM {q('Transactions')} t JOIN {q('Branches')} b ON b.{q('Code')} = t.{q('Branch Code')}"
    )
    table = {k: v for k, v in tx.items() if k != "table"} | {"query": query}
    run({"source_id": "core"}, table)
    source.transactions((3, "2026-11-20 09:00:00", "JHB", 300), (4, "2026-11-21 08:00:00", "CPT", 400))
    assert run({"source_id": "core"}, table)["rows"] == {"transactions": 2}
    rows = sorted(source.landed("transactions"), key=lambda r: r["TransactionId"])
    assert [(r["TransactionId"], r["Region"]) for r in rows] == [
        (1, "Gauteng"), (2, "Western Cape"), (3, "Gauteng"), (4, "Western Cape")
    ]
    schema = pq.read_schema(source.files("transactions")[0])
    assert schema.names == ["TransactionId", "UpdatedAt", "Region", "_extracted_at"]
    assert schema.field("UpdatedAt").type == pa.timestamp("us")


def test_a_new_source_column_lands_and_a_type_change_fails_the_run(source, run, tx):
    q = source.quote
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100))
    run({"source_id": "core"}, tx)
    source.execute(f"ALTER TABLE {q('Transactions')} ADD {q('Narrative')} varchar(20)")
    row = {"TransactionId": 2, "UpdatedAt": datetime(2026, 11, 21, 8), "Branch Code": "CPT", "Narrative": "salary"}
    source.insert_into("Transactions", [row | {"amountCents": 200}])
    run({"source_id": "core"}, tx)
    [r1, r2] = sorted(source.landed("transactions"), key=lambda r: r["TransactionId"])
    assert "Narrative" not in r1 and r2["Narrative"] == "salary"

    source.execute(f"DROP TABLE {q('Transactions')}")
    sa.Table(
        "Transactions",
        metadata := sa.MetaData(),
        sa.Column("TransactionId", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("UpdatedAt", sa.DateTime, nullable=False),
        sa.Column("Branch Code", sa.String(10)),
        sa.Column("amountCents", sa.String(20)),  # was BIGINT
        sa.Column("Narrative", sa.String(20)),
    )
    metadata.create_all(source.sa)
    source.insert_into("Transactions", [row | {"TransactionId": 3, "amountCents": "three hundred"}])
    with pytest.raises(BaseException, match="amountCents"):  # the schema contract freezes column types
        run({"source_id": "core"}, tx)
    assert ids(source) == [1, 2]
