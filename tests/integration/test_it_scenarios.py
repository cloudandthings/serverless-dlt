"""db_bronze integration: the handler-level scenarios of the SQLite suite, against real engines, on both backends.

Modes and manifests, lag, keyset paging and the time budget, preflight and
`validate`, error types, paged snapshots, planned backfills and partitioned
reads (DESIGN-db.md sections 6.2, 7.3-7.6). Timestamps are whole seconds:
Oracle maps SQLAlchemy's DateTime to DATE.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime
from itertools import pairwise

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration

TRANSACTIONS = {
    "name": "transactions",
    "table": "Transactions",
    "primary_key": ["TransactionId"],
    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
}
KEYSET = {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 7}


def define(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Transactions",
        metadata,
        sa.Column("TransactionId", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("UpdatedAt", sa.DateTime, nullable=False),
        sa.Column("Branch Code", sa.String(10)),
        sa.Column("amountCents", sa.BigInteger),
    )


@pytest.fixture
def transactions(source):
    """The Transactions table, empty; `add(*(id, "YYYY-MM-DD HH:MM:SS"))` inserts rows."""
    table = source.create(define)

    def add(*values: tuple[int, str]) -> None:
        source.insert(
            table,
            [
                {"TransactionId": i, "UpdatedAt": datetime.fromisoformat(ts), "Branch Code": "JHB", "amountCents": i}
                for i, ts in values
            ],
        )

    return add


def ids(rows: list[dict]) -> list[int]:
    return sorted(r["TransactionId"] for r in rows)


def landed_in(source, mode: str, table: str = "transactions") -> list[dict]:
    import pyarrow.parquet as pq

    return [r for f in source.files(table) if f"/mode={mode}/" in f.as_posix() for r in pq.read_table(f).to_pylist()]


def load_rows(source, load_ids: list[str], table: str = "transactions") -> list[dict]:
    import pyarrow.parquet as pq

    return [
        r for f in source.files(table) if f.name.rsplit(".", 2)[0] in load_ids for r in pq.read_table(f).to_pylist()
    ]


def manifests(source) -> dict[str, dict]:
    root = source.bucket / "it" / "_manifests"
    return {f.relative_to(root).as_posix(): json.loads(f.read_text()) for f in sorted(root.rglob("*.json"))}


# Modes ---------------------------------------------------------------------------------------------------------


def test_full_lands_initial_value_to_run_start_with_a_manifest_and_leaves_the_cursor(source, run, backend, transactions):
    transactions((1, "2025-12-31 23:59:59"), (2, "2026-01-01 00:00:01"), (3, "2026-11-20 10:00:00"))
    transactions((4, "2999-01-01 00:00:00"))  # after run start
    table = TRANSACTIONS | {"backend": backend}
    result = run({"source_id": "core", "mode": "full"}, table)
    assert result["rows"] == {"transactions": 2}
    assert ids(landed_in(source, "full")) == [2, 3]
    [manifest] = manifests(source).values()
    assert manifest["load_ids"] == result["load_ids"]

    assert run({"source_id": "core"}, table)["rows"] == {"transactions": 3}  # the full run left the cursor alone
    assert ids(landed_in(source, "incremental")) == [2, 3, 4]


def test_backfill_lands_its_half_open_window_dated_by_window_start(source, run, backend, transactions):
    transactions((1, "2026-11-20 07:59:59"), (2, "2026-11-20 08:00:00"), (3, "2026-11-20 09:59:59"))
    transactions((4, "2026-11-20 10:00:00"))
    event = {"source_id": "core", "mode": "backfill"}
    window = {"window_start": "2026-11-20T08:00:00Z", "window_end": "2026-11-20T10:00:00Z"}
    result = run(event | window, TRANSACTIONS | {"backend": backend})
    assert result["rows"] == {"transactions": 2}
    assert ids(landed_in(source, "backfill")) == [2, 3]
    assert {f.parent.name for f in source.files("transactions")} == {"day=20"}


def test_an_integer_cursor_lands_only_new_rows(source, run, backend, transactions):
    transactions((1, "2026-11-20 08:00:00"), (2, "2026-11-20 08:00:00"))
    table = TRANSACTIONS | {
        "backend": backend,
        "cursor": {"column": "TransactionId", "type": "integer", "initial_value": 0},
    }
    assert run({"source_id": "core"}, table)["rows"] == {"transactions": 2}
    transactions((3, "2026-11-19 08:00:00"))
    assert run({"source_id": "core"}, table)["rows"] == {"transactions": 1}
    assert ids(source.landed("transactions")) == [1, 2, 3]


def test_lag_lands_a_late_commit_and_re_lands_the_lag_window(source, run, backend, transactions):
    table = TRANSACTIONS | {"backend": backend, "cursor": TRANSACTIONS["cursor"] | {"lag": 10}}
    transactions(*[(i, f"2026-11-20 08:00:{i * 10:02}") for i in range(1, 6)])  # :10 .. :50
    run({"source_id": "core"}, table)
    transactions((6, "2026-11-20 08:00:50"), (7, "2026-11-20 08:00:45"), (8, "2026-11-20 08:01:00"))
    r2 = run({"source_id": "core"}, table)
    assert ids(load_rows(source, r2["load_ids"])) == [4, 5, 6, 7, 8]  # window [08:00:40, ...)
    r3 = run({"source_id": "core"}, table)
    assert ids(load_rows(source, r3["load_ids"])) == [5, 6, 8]  # [08:00:50, ...) lands again


# Keyset and the time budget -------------------------------------------------------------------------------------


def with_ties(transactions, first_id: int, count: int, day: int = 20) -> list[int]:
    """`count` rows in groups of 10 sharing one UpdatedAt, inserted in shuffled key order."""
    rows = [(i, f"2026-11-{day} {8 + (i - first_id) // 10:02d}:00:00") for i in range(first_id, first_id + count)]
    random.Random(first_id).shuffle(rows)
    transactions(*rows)
    return sorted(r[0] for r in rows)


def test_keyset_pages_through_ties_and_a_tiny_budget_loops_until_every_row_landed_once(
    source, run, backend, transactions
):
    table = TRANSACTIONS | {"backend": backend, "chunking": KEYSET}
    expected = with_ties(transactions, 1, 53)  # 8 pages of 7; most page boundaries fall inside a tie
    first = run({"source_id": "core"}, table, max_time_s=1e-6)
    assert first["time_limited"] and first["rows"] == {"transactions": 7}
    runs = [first]
    while not runs[-1]["is_empty"]:
        runs.append(run({"source_id": "core"}, table, max_time_s=1e-6))
        assert len(runs) < 30
    assert len(runs) > 5
    assert ids(source.landed("transactions")) == expected

    more = with_ties(transactions, 54, 20, day=21)
    assert run({"source_id": "core"}, table)["rows"] == {"transactions": 20}
    assert ids(source.landed("transactions")) == expected + more


# Preflight, validate and error types ----------------------------------------------------------------------------


def wide(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Wide",
        metadata,
        sa.Column("TransactionId", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("UpdatedAt", sa.DateTime, nullable=True),
        sa.Column("Amount", sa.Numeric(38, 12)),
    )


def test_a_wide_decimal_fails_preflight_on_connectorx_lands_on_pyarrow_and_validate_reports_it(source, run):
    table = source.create(wide)
    source.insert(
        table,
        [
            {"TransactionId": 1, "UpdatedAt": datetime(2026, 11, 20, 8), "Amount": "12345678901234567890.123456789012"},
            {"TransactionId": 2, "UpdatedAt": None, "Amount": None},
        ],
    )
    config = {"name": "wide", "table": "Wide", "primary_key": ["TransactionId"], "cursor": TRANSACTIONS["cursor"]}
    report = run({"source_id": "core", "mode": "validate"}, config | {"backend": "connectorx"})
    assert report["ok"] is False
    [found] = report["tables"]
    assert len(found["findings"]) == 1 and "Amount" in found["findings"][0]
    assert found["null_cursor_count"] == 1
    with pytest.raises(Exception) as failure:
        run({"source_id": "core"}, config | {"backend": "connectorx"})
    assert type(failure.value).__name__ == "PreflightError"
    assert source.files("wide") == []

    assert run({"source_id": "core"}, config | {"backend": "pyarrow", "name": "wide_pa"})["rows"] == {"wide_pa": 1}
    [row] = source.landed("wide_pa")
    assert str(row["Amount"]) == "12345678901234567890.123456789012"


# A query whose result has a type ConnectorX cannot read: preflight sees no types for a query's columns.
UNREADABLE = {
    "postgres": "SELECT 1 AS id, interval '1 day' AS v",
    "mysql": "SELECT 1 AS id, ST_GeomFromText('POINT(1 2)') AS v",
    "oracle": "SELECT 1 AS id, INTERVAL '1' DAY AS v FROM dual",
}


def test_a_connectorx_read_failure_is_connectorx_failed_and_nothing_lands(source, run):
    table = {"name": "odd", "query": UNREADABLE[source.dialect], "backend": "connectorx"}
    with pytest.raises(Exception) as failure:
        run({"source_id": "core", "mode": "full"}, table)
    assert type(failure.value).__name__ == "ConnectorXFailed"
    assert '"backend": "pyarrow"' in str(failure.value)
    assert source.files("odd") == []


def test_a_backend_change_fails_with_backend_changed_and_a_new_name_passes(source, run, transactions):
    transactions((1, "2026-11-20 08:00:00"))
    run({"source_id": "core"}, TRANSACTIONS | {"backend": "pyarrow"})
    transactions((2, "2026-11-21 08:00:00"))
    with pytest.raises(Exception) as failure:
        run({"source_id": "core"}, TRANSACTIONS | {"backend": "connectorx"})
    assert type(failure.value).__name__ == "BackendChanged"
    assert ids(source.landed("transactions")) == [1]
    renamed = TRANSACTIONS | {"backend": "connectorx", "name": "transactions_v2"}
    assert run({"source_id": "core"}, renamed)["rows"] == {"transactions_v2": 2}


# Paged snapshots and planned backfills -------------------------------------------------------------------------


def page_event(page: dict, plan: dict) -> dict:
    return {
        "source_id": "core",
        "mode": "full",
        "tables": [page["table"]],
        "page": {"offset": page["offset"], "limit": page["limit"]},
        "snapshot_id": plan["snapshot_id"],
        "partition_date": plan["partition_date"],
    }


def test_plan_pages_and_finalize_land_every_row_exactly_once(source, run, backend, transactions):
    transactions(*[(i, f"2026-11-20 08:00:{60 - i:02d}") for i in range(1, 11)])  # cursor order != key order
    table = TRANSACTIONS | {"backend": backend}
    plan = run({"source_id": "core", "mode": "plan", "plan": "snapshot", "page_size": 3}, table)
    assert [(p["offset"], p["limit"]) for p in plan["pages"]] == [(0, 3), (3, 3), (6, 3), (9, None)]
    transactions((11, "2026-11-20 09:00:00"))  # arrives after the plan: the open-ended last page takes it
    pages = [run(page_event(p, plan), table) for p in plan["pages"]]
    assert ids(load_rows(source, pages[1]["load_ids"])) == [4, 5, 6]
    load_ids = [i for r in pages for i in r["load_ids"]]
    event = {"source_id": "core", "mode": "finalize", "table": "transactions", "snapshot_id": plan["snapshot_id"]}
    run(event | {"load_ids": load_ids}, table)
    assert ids(load_rows(source, load_ids)) == list(range(1, 12))
    assert manifests(source)[f"transactions/{plan['snapshot_id']}.json"]["load_ids"] == load_ids


def test_plan_backfill_windows_cover_min_to_max_and_land_every_row_once(source, run, backend, transactions):
    transactions((1, "2026-03-01 00:00:00"), (2, "2026-03-03 09:30:00"), (3, "2026-03-05 05:00:00"))
    transactions((4, "2026-03-07 00:00:00"))
    table = TRANSACTIONS | {"backend": backend}
    windows = run({"source_id": "core", "mode": "plan", "plan": "backfill", "windows": 3}, table)["windows"]
    assert windows[0]["window_start"] == "2026-03-01T00:00:00+00:00"
    assert windows[-1]["window_end"] == "2026-03-07T00:01:00+00:00"
    assert all(a["window_end"] == b["window_start"] for a, b in pairwise(windows))
    for w in windows:
        bounds = {"window_start": w["window_start"], "window_end": w["window_end"]}
        run({"source_id": "core", "mode": "backfill"} | bounds, table)
    assert ids(landed_in(source, "backfill")) == [1, 2, 3, 4]


def test_a_partitioned_read_lands_the_same_rows_as_an_unpartitioned_one(source, run, transactions):
    transactions(*[(i, f"2026-03-{i:02d} 12:00:00") for i in range(1, 21)])
    window = {"source_id": "core", "mode": "backfill"}
    window |= {"window_start": "2026-03-05T00:00:00Z", "window_end": "2026-03-15T00:00:00Z"}
    plain = TRANSACTIONS | {"backend": "connectorx"}
    run(window, plain)
    partitioned = plain | {"name": "partitioned", "partition": {"column": "TransactionId", "num": 3}}
    assert run(window, partitioned)["rows"] == {"partitioned": 10}
    assert ids(source.landed("partitioned")) == ids(source.landed("transactions")) == list(range(5, 15))


def test_the_run_date_partitions_are_today(source, run, transactions):
    transactions((1, "2026-11-20 08:00:00"))
    run({"source_id": "core"}, TRANSACTIONS | {"backend": "pyarrow"})
    today = datetime.now(UTC)
    [f] = source.files("transactions")
    assert f"mode=incremental/year={today:%Y}/month={today:%m}/day={today:%d}" in f.as_posix()
