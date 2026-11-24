"""db_bronze cursor queries: compiled offline for every dialect, and windows executed on SQLite."""

from __future__ import annotations

from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, mysql, oracle, postgresql, sqlite

from db_bronze.config import ChunkingConfig, CursorConfig, TableConfig, to_source_time
from db_bronze.query import cursor_select, keyset_select, literal_sql, page_select, paged_rows, query_source

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


def keyset_config(order_by=("UpdatedAt", "TransactionId"), page_size=1000, **overrides) -> TableConfig:
    chunking = ChunkingConfig(strategy="keyset", order_by=tuple(order_by), page_size=page_size)
    return table_config(primary_key=("TransactionId",), chunking=chunking, **overrides)


AFTER = (datetime(2026, 11, 20, 9, 30), 42)


def flat(stmt: sa.Select, dialect) -> str:
    return " ".join(str(stmt.compile(dialect=dialect)).split())


@pytest.mark.parametrize(
    ("dialect", "row_limit"),
    [
        ("sqlite", "LIMIT ? OFFSET ?"),
        ("postgresql", "LIMIT %(param_"),
        ("mysql", "LIMIT %s"),
        ("mssql", "SELECT TOP __[POSTCOMPILE_param_"),
        ("oracle", "FETCH FIRST __[POSTCOMPILE_param_"),
        ("db2", "FETCH FIRST"),
    ],
)
def test_keyset_page_compiles_offline_with_the_dialects_own_row_limit(dialect, row_limit):
    d = db2_dialect() if dialect == "db2" else DIALECTS[dialect]
    sql = flat(keyset_select(transactions(), keyset_config(), START, None, AFTER), d)
    assert row_limit in sql
    assert "ORDER BY" in sql and "OFFSET" not in sql.replace("LIMIT ? OFFSET ?", "")


@pytest.mark.parametrize("dialect", [*DIALECTS, "db2"])
def test_keyset_tie_break_is_the_expanded_form_not_a_row_value_comparison(dialect):
    d = db2_dialect() if dialect == "db2" else DIALECTS[dialect]
    tc = keyset_config(order_by=("UpdatedAt", "TransactionId", "Branch Code"))
    sql = flat(keyset_select(transactions(), tc, START, None, (*AFTER, "JHB")), d)
    assert sql.count("TransactionId") >= 3 and ") > (" not in sql  # no (a, b) > (x, y)
    assert sql.count(" OR ") == 2


def test_mssql_keyset_page_is_the_documented_statement():
    sql = flat(keyset_select(transactions(), keyset_config(), START, END, AFTER), DIALECTS["mssql"])
    t = "dbo.[Transactions]"
    assert sql == (
        f"SELECT TOP __[POSTCOMPILE_param_1] {t}.[TransactionId], {t}.[UpdatedAt], {t}.[Branch Code] FROM {t} "
        f"WHERE {t}.[UpdatedAt] >= :param_2 AND {t}.[UpdatedAt] < :param_3 "
        f"AND ({t}.[UpdatedAt] > :param_4 OR {t}.[UpdatedAt] = :param_5 AND {t}.[TransactionId] > :param_6) "
        f"ORDER BY {t}.[UpdatedAt], {t}.[TransactionId]"
    )


def test_the_first_keyset_page_has_no_seed():
    sql = flat(keyset_select(transactions(), keyset_config(), START, END, None), DIALECTS["oracle"])
    assert " OR " not in sql
    assert sql.endswith('ORDER BY dbo."Transactions"."UpdatedAt", dbo."Transactions"."TransactionId" '
                        "FETCH FIRST __[POSTCOMPILE_param_3] ROWS ONLY")


PAGE_SYNTAX = {  # each dialect's own row-limit syntax for OFFSET 400 [and 100 rows]
    "sqlite": ("LIMIT 100 OFFSET 400", "LIMIT -1 OFFSET 400"),
    "postgresql": ("LIMIT 100 OFFSET 400", "LIMIT ALL OFFSET 400"),
    "mysql": ("LIMIT 400, 100", "LIMIT 400, 18446744073709551615"),
    "mssql": ("mssql_rn > 400 AND mssql_rn <= 100 + 400", "mssql_rn > 400"),
    "oracle": ("OFFSET 400 ROWS FETCH FIRST 100 ROWS ONLY", "OFFSET 400 ROWS"),
}


@pytest.mark.parametrize("dialect", PAGE_SYNTAX)
def test_page_queries_compile_offline_with_the_dialects_row_limit_ordered_by_the_primary_key(dialect):
    d = DIALECTS[dialect]
    tc = table_config(primary_key=("TransactionId",))
    rows = paged_rows(transactions(), tc, START)
    for limit, syntax in zip((100, None), PAGE_SYNTAX[dialect], strict=True):
        sql = " ".join(literal_sql(page_select(transactions(), rows, tc, 400, limit), d).split())
        assert "ORDER BY" in sql and "TransactionId" in sql.split("ORDER BY")[1]
        assert syntax in sql


def test_db2_page_query_compiles_offline():
    tc = table_config(primary_key=("TransactionId",))
    stmt = page_select(transactions(), paged_rows(transactions(), tc, START), tc, 400, 100)
    assert "ORDER BY" in str(stmt.compile(dialect=db2_dialect()))
