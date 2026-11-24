"""db_bronze integration: opt-in partitioned ConnectorX reads on real engines (DESIGN-db.md 3.1, 5.2, 6.1).

`partition_on` opens N connections to the server, which SQLite never showed.
"""

from __future__ import annotations

from datetime import datetime

import pytest
import sqlalchemy as sa

from .conftest import fails

pytestmark = pytest.mark.integration

TRANSACTIONS = {
    "name": "transactions",
    "table": "Transactions",
    "primary_key": ["TransactionId"],
    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
    "backend": "connectorx",
}
PARTITIONED = TRANSACTIONS | {"partition": {"column": "TransactionId", "num": 3}}
BACKFILL = {"mode": "backfill", "window_start": "2026-03-05T00:00:00Z", "window_end": "2026-03-15T00:00:00Z"}


@pytest.fixture
def source(source):
    source.transactions(*[(i, f"2026-03-{i:02d} 08:00:00", "JHB", i * 10) for i in range(1, 21)])
    return source


def landed_then_cleared(source) -> list[int]:
    ids = source.landed_ids("transactions", "TransactionId")
    for f in source.files("transactions"):
        f.unlink()
    return ids


@pytest.mark.parametrize(("event", "expected"), [({"mode": "full"}, range(1, 21)), (BACKFILL, range(5, 15))],
                         ids=["full", "backfill"])
def test_a_partitioned_read_lands_the_same_rows_as_an_unpartitioned_one(source, run, event, expected):
    if source.dialect == "oracle":  # ConnectorX upper-cases the column in its range query, quoted or not
        with fails("PreflightError", r"case-sensitive column 'TransactionId'.*case-insensitive name"):
            run({"source_id": "core", **event}, PARTITIONED)
        return
    run({"source_id": "core", **event}, TRANSACTIONS)
    assert landed_then_cleared(source) == list(expected)
    assert run({"source_id": "core", **event}, PARTITIONED)["rows"] == {"transactions": len(expected)}
    assert landed_then_cleared(source) == list(expected)


def test_partition_in_an_incremental_run_fails_before_extraction(source, run):
    with fails("ConfigError", r"table transactions: partition reads.*cursor order.*incremental"):
        run({"source_id": "core"}, PARTITIONED)
    assert source.nothing_landed()


def test_a_non_integer_partition_column_fails_the_preflight(source, run):
    with fails("PreflightError", r"partition column 'Branch Code' is not an integer"):
        run({"source_id": "core", "mode": "full"}, TRANSACTIONS | {"partition": {"column": "Branch Code", "num": 2}})
    assert source.nothing_landed()


def test_a_partitioned_read_by_a_case_insensitive_column(source, run):
    """Lower-case names, which Oracle stores upper case: ConnectorX partitions by them on every engine."""
    table = source.create(lambda metadata: sa.Table(
        "ledger", metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    ))
    source.insert(table, [{"id": i, "updated_at": datetime(2026, 3, i, 12)} for i in range(1, 21)])
    cursor = {"column": "updated_at", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}
    plain = {"name": "ledger", "table": "ledger", "primary_key": ["id"], "cursor": cursor, "backend": "connectorx"}
    run({"source_id": "core", **BACKFILL}, plain)
    partitioned = plain | {"name": "partitioned", "partition": {"column": "id", "num": 3}}
    assert run({"source_id": "core", **BACKFILL}, partitioned)["rows"] == {"partitioned": 10}
    assert source.landed_ids("partitioned", "id") == source.landed_ids("ledger", "id") == list(range(5, 15))
