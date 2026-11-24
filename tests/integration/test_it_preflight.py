"""db_bronze integration: the preflight, `validate` and the error types on real engines (DESIGN-db.md 6.2, 7.6).

What Lambda reports is the `errorType` (the raised class name) and its message,
so these tests check exactly that.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
import sqlalchemy as sa

from .conftest import fails

pytestmark = pytest.mark.integration

CURSOR = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}

# A column type each engine has that ConnectorX cannot read (the type matrix), as (DDL, literal).
UNREADABLE = {
    "postgres": ("money", "1.50"),
    "mysql": ("geometry", "ST_GeomFromText('POINT(1 2)')"),
    "oracle": ("raw(16)", "hextoraw('0102')"),
}


@pytest.fixture
def wide(source) -> sa.Table:
    """`Transactions` with two decimal columns ConnectorX would truncate, and two rows with a NULL cursor."""
    table = source.create(
        lambda metadata: sa.Table(
            "Transactions",
            metadata,
            sa.Column("TransactionId", sa.Integer, primary_key=True, autoincrement=False),
            sa.Column("UpdatedAt", sa.DateTime),
            sa.Column("Amount", sa.Numeric(38, 12)),
            sa.Column("Fee", sa.Numeric(20, 11)),
        )
    )
    source.insert(
        table,
        [
            {"TransactionId": i, "UpdatedAt": ts, "Amount": Decimal("1.5"), "Fee": Decimal("2")}
            for i, ts in ((1, datetime(2026, 11, 20, 8)), (2, None), (3, None))
        ],
    )
    return table


def tx(backend: str, **overrides) -> dict:
    return {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": CURSOR,
        "backend": backend,
    } | overrides


def test_a_wide_decimal_on_connectorx_fails_the_preflight_and_lands_on_pyarrow(source, run, wide):
    with fails("PreflightError", r'(?s)column Amount: .*column Fee: .*"backend": "pyarrow"'):
        run({"source_id": "core"}, tx("connectorx"))
    assert source.nothing_landed()
    assert run({"source_id": "core"}, tx("connectorx", columns=["TransactionId", "UpdatedAt"]))["rows"] == {
        "transactions": 1
    }
    assert run({"source_id": "core"}, tx("pyarrow", name="transactions_pa"))["rows"] == {"transactions_pa": 1}


def test_validate_reports_per_table_and_writes_nothing(source, run, wide):
    report = run({"source_id": "core", "mode": "validate"}, tx("connectorx"))
    [table] = report["tables"]
    assert (table["name"], table["backend"], table["null_cursor_count"]) == ("transactions", "connectorx", 2)
    assert [f.split(":")[0] for f in table["findings"]] == ["column Amount", "column Fee"]
    assert report["ok"] is False
    clean = run({"source_id": "core", "mode": "validate"}, tx("pyarrow"))
    assert clean["ok"] is True and clean["tables"][0]["findings"] == []
    assert source.nothing_landed()


def test_an_invalid_config_fails_with_config_error_and_nothing_lands(source, run, backend):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100))
    with fails("ConfigError", "order_by"):
        run({"source_id": "core"}, tx(backend, chunking={"strategy": "keyset", "page_size": 10}))
    assert source.nothing_landed()


def test_kerberos_fails_at_run_start_with_not_implemented_and_nothing_lands(source, run):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100))
    kerberos = {"type": "kerberos", "principal": "svc_ingest@CORP.EXAMPLE", "keytab": {"secret_id": "ingest/keytab"}}
    with fails("NotImplementedError", "docs/KERBEROS.md"):
        run({"source_id": "core"}, tx("pyarrow"), connection={"auth": kerberos})
    assert source.nothing_landed()


def test_a_backend_change_fails_before_extraction_and_a_new_name_passes(source, run):
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100))
    run({"source_id": "core"}, tx("pyarrow"))
    source.transactions((2, "2026-11-21 08:00:00", "CPT", 200))
    with fails("BackendChanged", r'from "pyarrow" to "connectorx"'):
        run({"source_id": "core"}, tx("connectorx"))
    assert source.landed_ids("transactions", "TransactionId") == [1]
    assert run({"source_id": "core"}, tx("connectorx", name="transactions_v2"))["rows"] == {"transactions_v2": 2}


def test_a_column_connectorx_cannot_read_in_a_query_fails_with_connectorx_failed_and_the_cursor_holds(source, run):
    """A `query` table's column types are not reflected, so the preflight cannot see them: ConnectorX fails."""
    q = source.quote
    ddl, literal = UNREADABLE[source.dialect]
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100))
    source.execute(
        f"ALTER TABLE {q('Transactions')} ADD {q('Odd')} {ddl}",
        f"UPDATE {q('Transactions')} SET {q('Odd')} = {literal}",
    )
    query = f"SELECT {q('TransactionId')}, {q('UpdatedAt')}, {q('Odd')} FROM {q('Transactions')}"
    table = {k: v for k, v in tx("connectorx").items() if k != "table"} | {"query": query}
    with fails("ConnectorXFailed", r'(?s)table transactions: ConnectorX failed: .*"backend": "pyarrow"'):
        run({"source_id": "core"}, table)
    assert source.nothing_landed()
    query = f"SELECT {q('TransactionId')}, {q('UpdatedAt')} FROM {q('Transactions')}"
    assert run({"source_id": "core"}, table | {"query": query})["rows"] == {"transactions": 1}  # the cursor held
