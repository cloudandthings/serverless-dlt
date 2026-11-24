"""Build one dlt resource per configured table (DESIGN-db.md section 7.1)."""

from __future__ import annotations

import json
import logging
from datetime import datetime

import dlt
import sqlalchemy as sa

from .config import RunRequest, SourceConfig, TableConfig, to_source_time
from .extract import column_specs, naive_timestamp_columns, read_connectorx, read_pyarrow, with_extracted_at
from .query import cursor_select, page_select, paged_rows, probe_columns, query_source, snapshot_select
from .urls import connectorx_url, sqlalchemy_url

log = logging.getLogger(__name__)

# A new source column lands; a type change on an existing column fails the run (section 7.2).
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}


def make_engine(cfg: SourceConfig) -> sa.Engine:
    """The SQLAlchemy engine: reflection, and extraction on the traditional backends."""
    options = {"isolation_level": cfg.connection.isolation_level} if cfg.connection.isolation_level else {}
    return sa.create_engine(sqlalchemy_url(cfg.connection), **options)


def reflect(engine: sa.Engine, tc: TableConfig) -> sa.FromClause:
    """The reflected source table, or for a `query` table a subquery over its result columns."""
    if tc.query:
        with engine.connect() as conn:
            columns = list(conn.execute(probe_columns(tc)).keys())
        return query_source(tc, columns)
    return sa.Table(tc.table, sa.MetaData(), schema=tc.schema, autoload_with=engine)


def window(cfg: SourceConfig, tc: TableConfig, req: RunRequest, run_started: datetime, max_cursor):
    """(initial_value, end_value) for dlt's incremental, as the source represents them.

    incremental  state-driven; initial_value only applies to the very first run
    full         [initial_value, run start): end_value makes dlt leave the cursor state alone.
                 For an integer cursor "run start" is the cursor's MAX when the run starts, plus one.
    backfill     [window_start, window_end): stateless, like full
    """
    tz = cfg.connection.naive_timezone
    if req.mode == "backfill":
        return tc.cursor.bound(req.window_start, "window_start", tz), tc.cursor.bound(req.window_end, "window_end", tz)
    initial = tc.cursor.initial_cursor_value(tz)
    if req.mode != "full":
        return initial, None
    if tc.cursor.type == "timestamp":
        return initial, to_source_time(run_started, tz)
    top = max_cursor()
    return initial, max(initial, top + 1) if top is not None else initial


def make_resource(cfg: SourceConfig, tc: TableConfig, req: RunRequest, engine: sa.Engine, run_started: datetime):
    backend = cfg.backend(tc)
    if backend == "sqlalchemy":  # stopgap until the sqlalchemy backend lands
        raise NotImplementedError(
            f'table {tc.name}: backend "sqlalchemy" is not implemented yet; set "backend": "pyarrow"'
        )
    table = reflect(engine, tc)
    specs = column_specs(table)
    log.info("table %s", json.dumps({"table": tc.name, "backend": backend}))
    batch_size = cfg.batch_size(tc)
    # Without this hint dlt labels naive timestamps UTC without shifting them (prototype S5).
    hints = {name: {"timezone": False} for name in naive_timestamp_columns(table)}
    resource = dlt.resource(
        name=tc.name,
        write_disposition="append",  # never "replace": it deletes sibling mode= partitions
        primary_key=list(tc.primary_key) or None,
        columns=hints,
        schema_contract=SCHEMA_CONTRACT,
    )

    def extract(stmt: sa.Select):
        if backend == "connectorx":
            # ConnectorX takes SQL text, so values are inlined, as dlt's own sql_table does.
            sql = str(stmt.compile(engine, compile_kwargs={"literal_binds": True}))
            batches = read_connectorx(connectorx_url(cfg.connection), sql, specs, batch_size)
        else:
            batches = read_pyarrow(engine, stmt, specs, batch_size)
        for batch in batches:
            yield with_extracted_at(batch, run_started)

    if req.page is not None:  # one page of a planned snapshot: no incremental, so the cursor state is untouched

        @resource
        def page():
            start = tc.cursor.initial_cursor_value(cfg.connection.naive_timezone) if tc.cursor else None
            yield from extract(page_select(table, paged_rows(table, tc, start), tc, req.page.offset, req.page.limit))

        return page

    if tc.cursor is None:  # a snapshot table: everything, every time (mode=full only)

        @resource
        def snapshot():
            yield from extract(snapshot_select(table, tc))

        return snapshot

    def max_cursor():
        with engine.connect() as conn:
            return conn.execute(sa.select(sa.func.max(table.c[tc.cursor.column]))).scalar()

    initial, end = window(cfg, tc, req, run_started, max_cursor)

    @resource
    def rows(
        # The incremental MUST be the argument's default value: dlt binds state and
        # filtering only to an incremental found in the signature. Building it in
        # the body silently disables both.
        cursor=dlt.sources.incremental(  # noqa: B008
            tc.cursor.column,
            initial_value=initial,
            end_value=end,
            lag=tc.cursor.lag,  # dlt steps cursor.start_value back by this
            row_order="asc",
            on_cursor_value_missing="include" if tc.cursor.include_null_cursor else "raise",
        ),
    ):
        yield from extract(cursor_select(table, tc, cursor.start_value, cursor.end_value))

    return rows
