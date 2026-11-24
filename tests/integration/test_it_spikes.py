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

from db_bronze import ConfigError
from db_bronze.config import SourceConfig
from db_bronze.extract import ColumnSpec, read_pyarrow
from db_bronze.preflight import MIN_SERVER_VERSION
from db_bronze.query import literal_sql
from db_bronze.urls import connectorx_url

pytestmark = pytest.mark.integration

ROWS = 2500  # at 2 ms a row, a slow query runs about 5 s
SLOW = {
    "postgres": "SELECT g AS id, (pg_sleep(0.002) IS NULL)::int AS {marker} FROM generate_series(1, {rows}) g",
    "mysql": "SELECT /*+ SET_VAR(cte_max_recursion_depth=100000) */ seq AS id, SLEEP(0.002) AS {marker} "
    "FROM (WITH RECURSIVE r(seq) AS (SELECT 1 UNION ALL SELECT seq + 1 FROM r WHERE seq < {rows}) SELECT seq FROM r) x",
}
RUNNING = {
    "postgres": "SELECT pid FROM pg_stat_activity WHERE state = 'active' AND query LIKE :q AND pid <> pg_backend_pid()",
    "mysql": "SELECT id FROM information_schema.processlist WHERE info LIKE :q AND id <> connection_id()",
}
CANCEL = {"postgres": "SELECT pg_cancel_backend({id})", "mysql": "KILL QUERY {id}"}


class SlowQuery:
    """A query that runs for seconds, tagged with a unique column name so the server's activity view finds it."""

    def __init__(self, source) -> None:
        self.source = source
        self.marker = f"slow_{uuid.uuid4().hex[:10]}"
        self.sql = SLOW[source.dialect].format(marker=self.marker, rows=ROWS)

    def running(self) -> list:
        with self.source.sa.connect() as conn:
            return list(conn.execute(sa.text(RUNNING[self.source.dialect]), {"q": f"%{self.marker}%"}).scalars())

    def cancel(self) -> None:
        with self.source.sa.connect() as conn:
            for session in self.running():
                conn.exec_driver_sql(CANCEL[self.source.dialect].format(id=session))


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


def test_a_dropped_connectorx_stream_leaves_its_query_running_on_the_server(source):
    slow = SlowQuery(source)
    reader = cx.read_sql(cx_url(source), slow.sql, return_type="arrow_stream", batch_size=50)
    try:
        assert reader.read_next_batch().num_rows > 0
        started = time.monotonic()
        reader.close()
        del reader
        assert time.monotonic() - started < 1  # the drop itself returns at once
        time.sleep(2)
        assert len(slow.running()) == 1  # but the server is still running the query
    finally:
        slow.cancel()


# How closing the traditional backend's streaming cursor mid-result behaves, per engine.
CLOSE = {
    "postgres": "ends",  # psycopg closes the server-side cursor: the query stops at once
    "mysql": "drains",  # PyMySQL reads the rest of an unbuffered result: the close waits for the whole query
}


def test_closing_a_traditional_stream_early(source):
    slow = SlowQuery(source)
    stmt = sa.select(sa.text("*")).select_from(sa.text(f"({slow.sql}) AS q"))
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
        sa.Column("at", sa.DateTime().with_variant(sa.dialects.mysql.DATETIME(fsp=6), "mysql")),
        sa.Column("amount", sa.Numeric(28, 10)),
        sa.Column("day", sa.Date),
    )


BOUNDARY = datetime(2026, 11, 20, 8, 0, 0, 123456)
AMOUNT = Decimal("123456789012345678.0123456789")


def test_datetime_and_decimal_literals_round_trip_through_connectorx(source):
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
    inlined = cx.read_sql(cx_url(source), sql, return_type="arrow").column("id").to_pylist()
    assert bound == [2, 5]
    assert inlined == bound


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


def test_connectorx_drops_digits_of_an_unconstrained_postgres_numeric(source):
    """Why preflight rejects NUMERIC without precision on ConnectorX for Postgres."""
    if source.dialect != "postgres":
        pytest.skip("MySQL DECIMAL always has a precision")
    source.execute("CREATE TABLE n (v numeric)", "INSERT INTO n VALUES (1.123456789012345678)")
    [value] = cx.read_sql(cx_url(source), "SELECT v FROM n", return_type="arrow").column("v").to_pylist()
    assert value == Decimal("1.1234567890")  # silently, without an error


# S3: session settings through ConnectorX ----------------------------------------------------------------------

ISOLATION_QUERY = {
    "postgres": "SELECT 1 AS id, current_setting('transaction_isolation') AS iso",
    "mysql": "SELECT 1 AS id, @@transaction_isolation AS iso",
}
APPLIED = {"postgres": "repeatable read", "mysql": "READ-COMMITTED"}
LEVEL = {"postgres": "REPEATABLE READ", "mysql": "READ COMMITTED"}  # never the engine's default


def isolation_run(source, run, backend):
    table = {"name": "iso", "query": ISOLATION_QUERY[source.dialect], "backend": backend}
    return run(
        {"source_id": "iso", "mode": "full"}, table, connection={"isolation_level": LEVEL[source.dialect]}
    )


def test_isolation_level_applies_on_the_traditional_backend(source, run):
    isolation_run(source, run, "pyarrow")
    assert [r["iso"] for r in source.landed("iso")] == [APPLIED[source.dialect]]


def test_isolation_level_with_connectorx(source, run):
    """Postgres: ConnectorX honours libpq `options`, so the level is applied. MySQL: no way in, so rejected."""
    if source.dialect == "mysql":
        with pytest.raises(ConfigError, match="isolation_level"):
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
