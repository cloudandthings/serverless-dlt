"""db_bronze integration: the design unknowns S1 (server side), S2, S3 and S9, measured on real engines.

Each test pins the behaviour a design rule rests on (DESIGN-db.md section 11),
so a driver or ConnectorX upgrade that changes it fails here, not in production.
"""

from __future__ import annotations

import time
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import connectorx as cx
import pytest
import sqlalchemy as sa

from db_bronze.config import ChunkingConfig, CursorConfig, SourceConfig, TableConfig
from db_bronze.extract import ColumnSpec, read_pyarrow
from db_bronze.preflight import MIN_SERVER_VERSION
from db_bronze.query import cursor_select, keyset_select, literal_sql
from db_bronze.urls import connectorx_url

from .conftest import NO_CONNECTORX, connectorx_skip_reason, fails

pytestmark = pytest.mark.integration

ROWS = 2500  # at 2 ms a row, a slow query runs about 5 s
# ConnectorX hands over its first SQL Server batch only after seconds, so the query must run on well past that
ROWS_FOR = {"mssql": 10000, "oracle": 500, "db2": 2500}
SLOW = {
    "postgres": "SELECT g AS id, (pg_sleep(0.002) IS NULL)::int AS {marker} FROM generate_series(1, {rows}) g",
    "mysql": "SELECT /*+ SET_VAR(cte_max_recursion_depth=100000) */ seq AS id, SLEEP(0.002) AS {marker} "
    "FROM (WITH RECURSIVE r(seq) AS (SELECT 1 UNION ALL SELECT seq + 1 FROM r WHERE seq < {rows}) SELECT seq FROM r) x",
    # T-SQL has no per-row sleep (WAITFOR is not allowed in a function): burn about 2 ms of CPU per row instead
    "mssql": "SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id, dbo.burn(2500) AS {marker} "
    "FROM sys.all_objects a CROSS JOIN sys.all_objects b",
    # DBMS_SESSION.SLEEP barely sleeps below 10 ms, so Oracle sleeps 10 ms a row, over fewer rows (ROWS_FOR)
    "oracle": "SELECT level AS id, it_sleep(level) AS {marker} FROM dual CONNECT BY level <= {rows}",
    # Db2 allows WITH only at the top, and resolves an unqualified function by CURRENT PATH, not the schema
    "db2": "SELECT ROW_NUMBER() OVER () AS id, {schema}.it_burn(BURN) AS {marker} "
    "FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b FETCH FIRST {rows} ROWS ONLY",
}
SLOW_SETUP = {
    "mssql": "CREATE FUNCTION dbo.burn(@n int) RETURNS int AS BEGIN "
    "DECLARE @i int = 0; WHILE @i < @n SET @i += 1; RETURN @i END",
    # Oracle has no sleep usable in a query; a function of the test's own schema wraps one.
    "oracle": "CREATE OR REPLACE FUNCTION it_sleep(n NUMBER) RETURN NUMBER IS "
    "BEGIN DBMS_SESSION.SLEEP(0.01); RETURN 0; END;",
    "db2": "CREATE FUNCTION it_burn(n INTEGER) RETURNS INTEGER LANGUAGE SQL NOT DETERMINISTIC NO EXTERNAL ACTION "
    "BEGIN DECLARE i INTEGER DEFAULT 0; WHILE i < n DO SET i = i + 1; END WHILE; RETURN i; END",
}
RUNNING = {
    "postgres": "SELECT pid FROM pg_stat_activity WHERE state = 'active' AND query LIKE :q AND pid <> pg_backend_pid()",
    "mysql": "SELECT id FROM information_schema.processlist WHERE info LIKE :q AND id <> connection_id()",
    # the batch as sent (the request's own sql_handle points into dbo.burn while it runs)
    "mssql": "SELECT r.session_id FROM sys.dm_exec_requests r "
    "CROSS APPLY sys.dm_exec_input_buffer(r.session_id, NULL) b WHERE b.event_info LIKE :q AND r.session_id <> @@SPID",
    "oracle": "SELECT DISTINCT s.sid || ',' || s.serial# FROM v$session s JOIN v$sql q ON q.sql_id = s.sql_id "
    "WHERE s.status = 'ACTIVE' AND q.sql_text LIKE :q AND s.sid <> SYS_CONTEXT('USERENV', 'SID')",
    "db2": "SELECT VARCHAR(a.application_handle) || ',' || VARCHAR(a.uow_id) || ',' || VARCHAR(a.activity_id) "
    "FROM TABLE(MON_GET_ACTIVITY(NULL, -2)) a "
    "WHERE a.stmt_text LIKE :q AND a.application_handle <> MON_GET_APPLICATION_HANDLE()",
}
CANCEL = {
    "postgres": "SELECT pg_cancel_backend({id})",
    "mysql": "KILL QUERY {id}",
    "mssql": "KILL {id}",
    "oracle": "ALTER SYSTEM CANCEL SQL '{id}'",
    "db2": "CALL SYSPROC.WLM_CANCEL_ACTIVITY({id})",
}


class SlowQuery:
    """A query that runs for seconds, tagged with a unique column name so the server's activity view finds it."""

    def __init__(self, source) -> None:
        self.source = source
        self.marker = f"slow_{uuid.uuid4().hex[:10]}"
        rows = ROWS_FOR.get(source.dialect, ROWS)
        self.sql = SLOW[source.dialect].format(marker=self.marker, rows=rows, schema=source.database)
        if source.dialect in SLOW_SETUP:
            source.execute(SLOW_SETUP[source.dialect])

    def running(self) -> list:
        with self.source.sa.connect() as conn:
            return list(conn.execute(sa.text(RUNNING[self.source.dialect]), {"q": f"%{self.marker}%"}).scalars())

    def cancel(self) -> None:
        with self.source.sa.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:  # KILL: no transaction
            for session in self.running():
                conn.exec_driver_sql(CANCEL[self.source.dialect].format(id=session))


def connectorx_here(source) -> None:
    """Skip unless this process can call ConnectorX on the engine directly (Oracle: Instant Client, `--inside`)."""
    if reason := connectorx_skip_reason(source.dialect):
        pytest.skip(reason)


def cx_url(source, **connection) -> str:
    cfg = SourceConfig.from_dict(
        {
            "pipeline_name": "it",
            "dataset_name": "it",
            "bucket_url": "file:///unused",
            "connection": source.connection(**connection),
            "tables": [{"name": "t", "table": "t", "backend": "pyarrow"}],
        }
    )
    return connectorx_url(cfg.connection)


# S1 (server side) ----------------------------------------------------------------------------------------------


# What a ConnectorX stream does on each engine (S1, S7).
STREAMS = {
    "postgres": "streams",  # batches arrive while the query runs; a dropped stream's query runs on
    "mysql": "streams",
    "mssql": "streams",
    "oracle": "buffers",  # the first batch arrives only once the whole query has run (ConnectorX 0.4.6)
}


def test_a_dropped_connectorx_stream_leaves_its_query_running_on_the_server(source):
    connectorx_here(source)
    slow = SlowQuery(source)
    started = time.monotonic()
    reader = cx.read_sql(cx_url(source), slow.sql, return_type="arrow_stream", batch_size=50)
    try:
        assert reader.read_next_batch().num_rows > 0
        first_batch = time.monotonic() - started
        dropped = time.monotonic()
        reader.close()
        del reader
        assert time.monotonic() - dropped < 1  # the drop itself returns at once
        time.sleep(2)
        if STREAMS[source.dialect] == "streams":
            assert len(slow.running()) == 1  # but the server is still running the query
        else:
            assert first_batch > 4  # the whole query (about 5 s) ran first: there is nothing left to run
            assert slow.running() == []
    finally:
        slow.cancel()


# How closing the traditional backend's streaming cursor mid-result behaves, per engine.
CLOSE = {
    "postgres": "ends",  # psycopg closes the server-side cursor: the query stops at once
    "mysql": "drains",  # PyMySQL reads the rest of an unbuffered result: the close waits for the whole query
    "mssql": "ends",  # ODBC Driver 18 cancels the rest of the result: the query stops at once
    "oracle": "ends",  # oracledb closes the cursor: the query stops at once
}


def test_closing_a_traditional_stream_early(source):
    slow = SlowQuery(source)
    stmt = sa.select(sa.text("*")).select_from(sa.text(f"({slow.sql}) q"))  # Oracle takes no AS here
    specs = {"id": ColumnSpec(None, True), slow.marker: ColumnSpec(None, True)}
    batches = read_pyarrow(source.sa, stmt, specs, batch_size=50)
    try:
        assert next(batches).num_rows == 50
        started = time.monotonic()
        batches.close()  # what dlt's add_limit(max_time=...) does to the resource
        took = time.monotonic() - started
        assert slow.running() == []
        behaviour = "ends" if took < 1 else "drains"
        assert behaviour == CLOSE[source.dialect], f"close took {took:.1f} s"
    finally:
        slow.cancel()


# S2: literal_binds round trip ---------------------------------------------------------------------------------


def literals(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "literals",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column(
            "at",
            sa.DateTime()
            .with_variant(sa.dialects.mysql.DATETIME(fsp=6), "mysql")
            .with_variant(sa.dialects.mssql.DATETIME2(6), "mssql")
            .with_variant(sa.dialects.oracle.TIMESTAMP(), "oracle"),  # Oracle DATE has no fractional seconds
        ),
        sa.Column("amount", sa.Numeric(28, 10)),
        sa.Column("day", sa.Date),
    )


BOUNDARY = datetime(2026, 11, 20, 8, 0, 0, 123456)
AMOUNT = Decimal("123456789012345678.0123456789")


def test_datetime_and_decimal_literals_round_trip_through_connectorx(source):
    connectorx_here(source)
    table = source.create(literals)
    one_us = timedelta(microseconds=1)
    tiny = Decimal("0.0000000001")
    source.insert(
        table,
        [
            {"id": 1, "at": BOUNDARY - one_us, "amount": AMOUNT, "day": date(2026, 11, 19)},
            {"id": 2, "at": BOUNDARY, "amount": AMOUNT, "day": date(2026, 11, 19)},
            {"id": 3, "at": BOUNDARY + one_us, "amount": AMOUNT - tiny, "day": date(2026, 11, 19)},
            {"id": 4, "at": BOUNDARY + one_us, "amount": AMOUNT, "day": date(2026, 11, 20)},
            {"id": 5, "at": BOUNDARY + one_us, "amount": AMOUNT + tiny, "day": date(2026, 11, 19)},
        ],
    )
    stmt = (
        sa.select(table.c.id)
        .where(table.c.at >= BOUNDARY, table.c.amount >= AMOUNT, table.c.day < date(2026, 11, 20))
        .order_by(table.c.id)
    )
    with source.sa.connect() as conn:
        bound = list(conn.execute(stmt).scalars())
    sql = literal_sql(stmt, source.sa.dialect)
    inlined = cx.read_sql(cx_url(source), sql, return_type="arrow").column(0).to_pylist()  # Oracle says ID
    assert bound == [2, 5]
    assert inlined == bound


def test_inlined_cursor_and_keyset_literals_select_what_the_bound_queries_do_on_db2(source):
    """S2 on Db2. ConnectorX does not read Db2, so no Db2 query is inlined today; this pins that it could be.

    The cursor window and the keyset seed, inlined (microsecond timestamps, a
    28-digit decimal), run through ibm_db and select exactly what the bound
    statements do.
    """
    if source.dialect != "db2":
        pytest.skip("Db2 only: the other engines run S2 through ConnectorX")
    table = source.create(literals)
    one_us = timedelta(microseconds=1)
    tiny = Decimal("0.0000000001")
    source.insert(table, [
        {"id": 1, "at": BOUNDARY - one_us, "amount": AMOUNT, "day": date(2026, 11, 20)},
        {"id": 2, "at": BOUNDARY, "amount": AMOUNT - tiny, "day": date(2026, 11, 20)},
        {"id": 3, "at": BOUNDARY, "amount": AMOUNT, "day": date(2026, 11, 20)},
        {"id": 4, "at": BOUNDARY, "amount": AMOUNT + tiny, "day": date(2026, 11, 20)},
        {"id": 5, "at": BOUNDARY + one_us, "amount": AMOUNT, "day": date(2026, 11, 20)},
        {"id": 6, "at": BOUNDARY + 2 * one_us, "amount": AMOUNT, "day": date(2026, 11, 20)},
    ])
    cursor = CursorConfig(column="at", type="timestamp", initial_value="2026-01-01")
    keyset = ChunkingConfig(strategy="keyset", order_by=("at", "amount", "id"), page_size=10)
    tc = TableConfig(name="lit", table="literals", primary_key=("id",), cursor=cursor, chunking=keyset)
    end = BOUNDARY + 2 * one_us
    statements = {
        "cursor": cursor_select(table, tc, BOUNDARY, end),
        "keyset": keyset_select(table, tc, BOUNDARY, end, after=(BOUNDARY, AMOUNT - tiny, 2)),
    }
    dialect = source.sa.dialect
    with source.sa.connect() as conn:
        selected = {
            name: (list(conn.execute(stmt).scalars()), [r[0] for r in conn.exec_driver_sql(literal_sql(stmt, dialect))])
            for name, stmt in statements.items()
        }
    assert selected == {"cursor": ([2, 3, 4, 5], [2, 3, 4, 5]), "keyset": ([3, 4, 5], [3, 4, 5])}


def test_a_time_zone_aware_literal_round_trips_on_postgres(source):
    if source.dialect != "postgres":
        pytest.skip("only Postgres has a time-zone-aware timestamp the cursor can be")
    source.execute(
        "CREATE TABLE tz (id integer PRIMARY KEY, at timestamptz)",
        "INSERT INTO tz VALUES (1, '2026-11-20 06:00:00.123455+00'), (2, '2026-11-20 06:00:00.123456+00')",
    )
    table = sa.Table("tz", sa.MetaData(), autoload_with=source.sa)
    bound = datetime(2026, 11, 20, 8, 0, 0, 123456, tzinfo=ZoneInfo("Africa/Johannesburg"))  # 06:00 UTC
    sql = literal_sql(sa.select(table.c.id).where(table.c.at >= bound), source.sa.dialect)
    assert cx.read_sql(cx_url(source), sql, return_type="arrow").column("id").to_pylist() == [2]


# column type: (stored values, cursor window [start, end), the ids the bound query selects)
MSSQL_TIMESTAMPS = {
    # DATETIME counts in 1/300 s: .127 is .12666..., so it is inside a window ending at .126999
    "datetime": (["2026-11-20 07:59:59.997", "2026-11-20 08:00:00.123", "2026-11-20 08:00:00.127"],
                 (datetime(2026, 11, 20, 8, 0, 0, 123000), datetime(2026, 11, 20, 8, 0, 0, 126999)), [2, 3]),
    "smalldatetime": (["2026-11-20 07:59", "2026-11-20 08:00", "2026-11-20 08:01"],
                      (datetime(2026, 11, 20, 7, 59, 30, 1), datetime(2026, 11, 20, 8, 0, 30, 123456)), [2]),
    "datetime2(7)": (["2026-11-20 08:00:00.1234560", "2026-11-20 08:00:00.1234565", "2026-11-20 08:00:00.1234570"],
                     (datetime(2026, 11, 20, 8, 0, 0, 123456), datetime(2026, 11, 20, 8, 0, 0, 123457)), [1, 2]),
    "datetimeoffset": (["2026-11-20 08:00:00.123455+02:00", "2026-11-20 06:00:00.123456+00:00",
                        "2026-11-20 08:00:00.123457+02:00"],
                       (datetime(2026, 11, 20, 6, 0, 0, 123456, tzinfo=ZoneInfo("UTC")),
                        datetime(2026, 11, 20, 8, 0, 0, 123457, tzinfo=ZoneInfo("Africa/Johannesburg"))), [2]),
}


@pytest.mark.parametrize("column_type", MSSQL_TIMESTAMPS)
def test_a_cursor_window_inlined_for_connectorx_selects_what_the_bound_one_does_on_sql_server(source, column_type):
    """SQL Server converts a text literal to the column's type, and DATETIME takes 3 decimals at most (S2).

    The cursor query inlines its bounds as DATETIME2 (DATETIMEOFFSET with an
    offset), so a microsecond bound works against every timestamp type.
    """
    if source.dialect != "mssql":
        pytest.skip("SQL Server's timestamp types")
    values, (start, end), expected = MSSQL_TIMESTAMPS[column_type]
    source.execute(
        f"CREATE TABLE ts (id integer PRIMARY KEY, at {column_type})",
        *(f"INSERT INTO ts VALUES ({i}, '{v}')" for i, v in enumerate(values, 1)),
    )
    table = sa.Table("ts", sa.MetaData(), autoload_with=source.sa)
    cursor = CursorConfig(column="at", type="timestamp", initial_value="2026-01-01")
    tc = TableConfig(name="ts", table="ts", cursor=cursor)
    stmt = cursor_select(table, tc, start, end)
    with source.sa.connect() as conn:
        bound = list(conn.execute(stmt).scalars())
    inlined = cx.read_sql(cx_url(source), literal_sql(stmt, source.sa.dialect), return_type="arrow").column("id")
    assert bound == expected
    assert inlined.to_pylist() == bound
    if column_type == "datetime":  # why: the plain SQLAlchemy literal does not even run against DATETIME
        plain = sa.select(table.c.id).where(table.c.at >= start)
        with pytest.raises(RuntimeError, match="Conversion failed when converting date and/or time"):
            cx.read_sql(cx_url(source), literal_sql(plain, source.sa.dialect), return_type="arrow")


def test_connectorx_drops_digits_of_an_unconstrained_postgres_numeric(source):
    """Why preflight rejects NUMERIC without precision on ConnectorX for Postgres."""
    if source.dialect != "postgres":
        pytest.skip("MySQL DECIMAL always has a precision")
    source.execute("CREATE TABLE n (v numeric)", "INSERT INTO n VALUES (1.123456789012345678)")
    [value] = cx.read_sql(cx_url(source), "SELECT v FROM n", return_type="arrow").column("v").to_pylist()
    assert value == Decimal("1.1234567890")  # silently, without an error


def test_a_microsecond_backfill_window_selects_the_same_rows_on_both_backends(source, run):
    """S2 through the handler: the window bounds are inlined for ConnectorX and bound for pyarrow."""
    table = source.create(literals)
    one_us = timedelta(microseconds=1)
    source.insert(table, [
        {"id": i, "at": at, "amount": AMOUNT, "day": date(2026, 11, 20)}
        for i, at in enumerate([BOUNDARY - one_us, BOUNDARY, BOUNDARY + one_us, BOUNDARY + 2 * one_us], start=1)
    ])
    window = {"window_start": "2026-11-20T08:00:00.123456Z", "window_end": "2026-11-20T08:00:00.123458Z"}
    cursor = {"column": "at", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}
    for backend in ("pyarrow",) if source.dialect in NO_CONNECTORX else ("pyarrow", "connectorx"):
        config = {"name": f"lit_{backend}", "table": "literals", "cursor": cursor, "backend": backend}
        run({"source_id": "lit", "mode": "backfill"} | window, config)
        assert sorted(r["id"] for r in source.landed(f"lit_{backend}")) == [2, 3], backend


def test_a_time_zone_aware_cursor_on_oracle_selects_by_instant(source, run):
    """Oracle TIMESTAMP WITH TIME ZONE: ConnectorX cannot read it (preflight); pyarrow lands the UTC instant."""
    if source.dialect != "oracle":
        pytest.skip("Oracle's TIMESTAMP WITH TIME ZONE")
    source.execute(
        "CREATE TABLE tz (id integer PRIMARY KEY, at timestamp with time zone)",
        "INSERT INTO tz VALUES (1, timestamp '2026-11-20 08:00:00 +02:00')",  # 06:00 UTC
        "INSERT INTO tz VALUES (2, timestamp '2026-11-20 07:00:00 +00:00')",  # 07:00 UTC
    )
    cursor = {"column": "at", "type": "timestamp", "initial_value": "2026-11-20T06:30:00Z"}
    result = run({"source_id": "tz"}, {"name": "tz", "table": "tz", "cursor": cursor, "backend": "pyarrow"})
    assert result["rows"] == {"tz": 1}
    [row] = source.landed("tz")
    assert row["id"] == 2 and row["at"] == datetime(2026, 11, 20, 7, tzinfo=ZoneInfo("UTC"))
    report = run({"source_id": "tz", "mode": "validate"}, {"name": "tz_cx", "table": "tz", "backend": "connectorx"})
    [finding] = report["tables"][0]["findings"]
    assert "timestamp with time zone" in finding


# S6: thick-mode oracledb and ConnectorX in one process -------------------------------------------------------


@pytest.mark.connectorx
def test_both_backends_run_in_one_invocation(source, run):
    """S6 (Oracle): both load libclntsh in one process, one table each, in the same invocation."""
    source.transactions((1, "2026-11-20 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200))
    base = {"table": "Transactions", "primary_key": ["TransactionId"]}
    base["cursor"] = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}
    result = run({"source_id": "both"}, base | {"name": "tx_pa", "backend": "pyarrow"},
                 base | {"name": "tx_cx", "backend": "connectorx"})
    assert result["rows"] == {"tx_pa": 2, "tx_cx": 2}
    assert result["backends"] == {"tx_pa": "pyarrow", "tx_cx": "connectorx"}


# S3: session settings through ConnectorX ----------------------------------------------------------------------

ISOLATION_QUERY = {
    "postgres": "SELECT 1 AS id, current_setting('transaction_isolation') AS iso",
    "mysql": "SELECT 1 AS id, @@transaction_isolation AS iso",
    "mssql": "SELECT 1 AS id, CASE transaction_isolation_level WHEN 1 THEN 'READ UNCOMMITTED' "
    "WHEN 2 THEN 'READ COMMITTED' WHEN 3 THEN 'REPEATABLE READ' WHEN 4 THEN 'SERIALIZABLE' WHEN 5 THEN 'SNAPSHOT' "
    "END AS iso FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
    "oracle": "SELECT 1 AS id, it_isolation() AS iso FROM dual",
    # the level this very statement runs at (Db2 names: UR, CS, RS, RR)
    "db2": "SELECT 1 AS id, a.effective_isolation AS iso "
    "FROM TABLE(MON_GET_ACTIVITY(MON_GET_APPLICATION_HANDLE(), -1)) a FETCH FIRST 1 ROW ONLY",
}
# Oracle shows a session's isolation only on its open transaction (v$transaction), so a function of
# the test's schema opens one and reads it back, as SQLAlchemy's own get_isolation_level does.
ISOLATION_SETUP = {
    "oracle": "CREATE OR REPLACE FUNCTION it_isolation RETURN VARCHAR2 IS "
    "tx VARCHAR2(100) := DBMS_TRANSACTION.LOCAL_TRANSACTION_ID(TRUE); lvl VARCHAR2(20); "
    "BEGIN SELECT CASE BITAND(t.flag, POWER(2, 28)) WHEN 0 THEN 'READ COMMITTED' ELSE 'SERIALIZABLE' END "
    "INTO lvl FROM v$transaction t WHERE t.xidusn || '.' || t.xidslot || '.' || t.xidsqn = tx; "
    "RETURN lvl; END;",
}
APPLIED = {
    "postgres": "repeatable read", "mysql": "READ-COMMITTED", "mssql": "REPEATABLE READ", "oracle": "SERIALIZABLE",
    "db2": "RR",  # Db2's Repeatable Read is the standard's SERIALIZABLE
}
LEVEL = {  # never the engine's default
    "postgres": "REPEATABLE READ", "mysql": "READ COMMITTED", "mssql": "REPEATABLE READ", "oracle": "SERIALIZABLE",
    "db2": "SERIALIZABLE",
}


def isolation_run(source, run, backend):
    if setup := ISOLATION_SETUP.get(source.dialect):
        source.execute(setup)
    table = {"name": "iso", "query": ISOLATION_QUERY[source.dialect], "backend": backend}
    return run(
        {"source_id": "iso", "mode": "full"}, table, connection={"isolation_level": LEVEL[source.dialect]}
    )


def test_isolation_level_applies_on_the_traditional_backend(source, run):
    isolation_run(source, run, "pyarrow")
    assert [r["iso"] for r in source.landed("iso")] == [APPLIED[source.dialect]]


@pytest.mark.connectorx
def test_isolation_level_with_connectorx(source, run):
    """Postgres: ConnectorX honours libpq `options`. SQL Server: a SET statement ahead of the query, in the same batch.

    MySQL, Oracle: no way in, so rejected.
    """
    if source.dialect in ("mysql", "oracle"):
        with fails("ConfigError", "isolation_level"):  # by errorType: the same check in the image
            isolation_run(source, run, "connectorx")
        return
    isolation_run(source, run, "connectorx")
    assert [r["iso"] for r in source.landed("iso")] == [APPLIED[source.dialect]]


# S9: minimum server versions ----------------------------------------------------------------------------------


def test_the_server_meets_the_recorded_minimum(source, run):
    minimum = MIN_SERVER_VERSION[source.dialect]
    assert minimum is not None
    with source.sa.connect():
        assert source.sa.dialect.server_version_info >= minimum
    source.execute("CREATE TABLE v (id integer PRIMARY KEY)")
    report = run({"source_id": "v", "mode": "validate"}, {"name": "v", "table": "v"})
    assert report["ok"] and report["server_findings"] == []
