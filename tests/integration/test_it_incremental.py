"""db_bronze integration: the incremental handler scenario against real engines, on both backends."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration


def transactions(metadata: sa.MetaData) -> sa.Table:
    """Mixed-case and spaced names, as in the SQLite suite: they must land exactly as in the source."""
    return sa.Table(
        "Transactions",
        metadata,
        sa.Column("TransactionId", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("UpdatedAt", sa.DateTime, nullable=False),
        sa.Column("Branch Code", sa.String(10)),
        sa.Column("amountCents", sa.BigInteger),
    )


def rows(*values: tuple) -> list[dict]:
    return [
        {"TransactionId": i, "UpdatedAt": datetime.fromisoformat(ts), "Branch Code": b, "amountCents": a}
        for i, ts, b, a in values
    ]


def test_incremental_runs_land_only_new_rows(source, run, backend):
    table = source.create(transactions)
    source.insert(
        table,
        rows(
            (1, "2026-11-20 08:00:00", "JHB", 100),
            (2, "2026-11-20 09:00:00", "CPT", 200),
            (3, "2026-11-20 10:00:00", "DBN", 300),
        ),
    )
    config = {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
        "backend": backend,
    }
    r1 = run({"source_id": "core"}, config)
    assert r1["rows"] == {"transactions": 3}
    assert source.landed_ids("transactions", "TransactionId") == [1, 2, 3]

    source.insert(table, rows((4, "2026-11-20 11:00:00", "JHB", 400), (5, "2026-11-21 08:00:00", "PTA", 500)))
    r2 = run({"source_id": "core"}, config)
    assert r2["rows"] == {"transactions": 2}
    assert source.landed_ids("transactions", "TransactionId") == [1, 2, 3, 4, 5]

    assert run({"source_id": "core"}, config)["is_empty"]
    [first, *_] = source.files("transactions")
    schema = pq.read_schema(first)
    assert schema.names == ["TransactionId", "UpdatedAt", "Branch Code", "amountCents", "_extracted_at"]
    assert schema.field("UpdatedAt").type == pa.timestamp("us")  # naive in the source, naive in bronze
