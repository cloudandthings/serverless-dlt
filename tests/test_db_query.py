"""db_bronze cursor queries: compiled offline for every dialect, and windows executed on SQLite."""

from __future__ import annotations

from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, mysql, oracle, postgresql, sqlite

from db_bronze.config import CursorConfig, TableConfig, to_source_time
from db_bronze.query import cursor_select, query_source

DIALECTS = {
    "sqlite": sqlite.dialect(),
    "postgresql": postgresql.dialect(),
    "mysql": mysql.dialect(),
    "mssql": mssql.dialect(),
    "oracle": oracle.dialect(),
}


def db2_dialect():
    ibm_db_sa = pytest.importorskip("ibm_db_sa", reason="the Db2 SQLAlchemy dialect is not installed")
    return ibm_db_sa.dialect()


def transactions(metadata: sa.MetaData | None = None) -> sa.Table:
    return sa.Table(
        "Transactions",
        metadata or sa.MetaData(),
        sa.Column("TransactionId", sa.Integer, primary_key=True),
        sa.Column("UpdatedAt", sa.DateTime),
        sa.Column("Branch Code", sa.String(10)),
        schema="dbo",
    )


def table_config(**overrides) -> TableConfig:
    cursor = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2020-01-01T00:00:00"}
    cursor |= overrides.pop("cursor", {})
    base = {"name": "transactions", "table": "Transactions", "cursor": CursorConfig(**cursor)}
    return TableConfig(**base | overrides)


START, END = datetime(2026, 11, 20, 8, 0), datetime(2026, 11, 21, 0, 0)


@pytest.mark.parametrize("dialect", [*DIALECTS, "db2"])
def test_cursor_queries_compile_offline_for_every_dialect(dialect):
    d = db2_dialect() if dialect == "db2" else DIALECTS[dialect]
    full = table_config(columns=("TransactionId", "UpdatedAt"), where="1 = 1", cursor={"include_null_cursor": True})
    query = table_config(table=None, query='SELECT * FROM "Transactions"')
    statements = [
        cursor_select(transactions(), table_config(), START),
        cursor_select(transactions(), full, START, END),
        cursor_select(query_source(query, ["TransactionId", "UpdatedAt"]), query, START, END),
    ]
    for stmt in statements:
        sql = str(stmt.compile(dialect=d))
        assert "UpdatedAt" in sql and "ORDER BY" in sql


def test_sqlite_compiles_to_the_documented_where_clause():
    tc = table_config(columns=("TransactionId", "UpdatedAt"), where="\"Branch Code\" <> 'TEST'",
                      cursor={"include_null_cursor": True})
    sql = str(cursor_select(transactions(), tc, START, END).compile(dialect=DIALECTS["sqlite"]))
    assert " ".join(sql.split()) == (
        'SELECT dbo."Transactions"."TransactionId", dbo."Transactions"."UpdatedAt" FROM dbo."Transactions" '
        'WHERE (julianday(dbo."Transactions"."UpdatedAt") >= julianday(?) '
        'AND julianday(dbo."Transactions"."UpdatedAt") < julianday(?) OR dbo."Transactions"."UpdatedAt" IS NULL) '
        "AND \"Branch Code\" <> 'TEST' "
        'ORDER BY dbo."Transactions"."UpdatedAt"'
    )


def test_mssql_compares_the_cursor_column_directly_so_indexes_stay_usable():
    sql = str(cursor_select(transactions(), table_config(), START, END).compile(dialect=DIALECTS["mssql"]))
    assert " ".join(sql.split()) == (
        "SELECT dbo.[Transactions].[TransactionId], dbo.[Transactions].[UpdatedAt], dbo.[Transactions].[Branch Code] "
        "FROM dbo.[Transactions] "
        "WHERE dbo.[Transactions].[UpdatedAt] >= :param_1 AND dbo.[Transactions].[UpdatedAt] < :param_2 "
        "ORDER BY dbo.[Transactions].[UpdatedAt]"
    )


def test_oracle_query_subquery_has_no_as_keyword():
    tc = table_config(table=None, query='SELECT * FROM "Transactions"')
    sql = str(cursor_select(query_source(tc, ["UpdatedAt"]), tc, START).compile(dialect=DIALECTS["oracle"]))
    assert 'FROM (SELECT * FROM "Transactions") q' in sql


def test_window_is_half_open():
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData()
    table = sa.Table(
        "Transactions",
        metadata,
        sa.Column("TransactionId", sa.Integer, primary_key=True),
        sa.Column("UpdatedAt", sa.DateTime),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            'INSERT INTO "Transactions" VALUES (1, \'2026-11-20 07:59:59\'), (2, \'2026-11-20 08:00:00\'), '
            "(3, '2026-11-20 23:59:59'), (4, '2026-11-21 00:00:00')"
        )
        rows = conn.execute(cursor_select(table, table_config(), START, END)).all()
    assert [r.TransactionId for r in rows] == [2, 3]


@pytest.mark.parametrize(
    ("value", "naive_timezone", "expected"),
    [
        ("2020-01-01T00:00:00+02:00", "Africa/Johannesburg", datetime(2020, 1, 1, 0, 0)),
        ("2020-01-01T00:00:00Z", "Africa/Johannesburg", datetime(2020, 1, 1, 2, 0)),
        ("2020-01-01T00:00:00+02:00", "UTC", datetime(2019, 12, 31, 22, 0)),
        ("2020-01-01T00:00:00", "Africa/Johannesburg", datetime(2020, 1, 1, 0, 0)),  # naive: already source time
    ],
)
def test_bounds_with_an_offset_convert_to_naive_source_time(value, naive_timezone, expected):
    assert to_source_time(datetime.fromisoformat(value), naive_timezone) == expected
