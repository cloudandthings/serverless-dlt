"""Build one dlt resource per configured table (DESIGN-db.md section 7.1)."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

import dlt
import sqlalchemy as sa

from .config import RunRequest, SourceConfig, TableConfig, to_source_time
from .errors import BackendChanged, PreflightError
from .extract import (
    column_specs,
    naive_timestamp_columns,
    prepare_engine,
    read_connectorx,
    read_pyarrow,
    with_extracted_at,
)
from .query import (
    cursor_select,
    keyset_select,
    literal_sql,
    page_select,
    paged_rows,
    probe_columns,
    query_source,
    snapshot_select,
)
from .urls import connectorx_sql, connectorx_url, sqlalchemy_url

log = logging.getLogger(__name__)

# A new source column lands; a type change on an existing column fails the run (section 7.2).
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}


@dataclass
class RunStats:
    """Counts the extraction adds up for the run's metrics."""

    null_cursor_rows: dict[str, int] = field(default_factory=dict)


def make_engine(cfg: SourceConfig) -> sa.Engine:
    """The SQLAlchemy engine: reflection, and extraction on the traditional backends."""
    options = {"isolation_level": cfg.connection.isolation_level} if cfg.connection.isolation_level else {}
    return prepare_engine(sa.create_engine(sqlalchemy_url(cfg.connection), **options))


def reflect(engine: sa.Engine, tc: TableConfig) -> sa.FromClause:
    """The reflected source table, or for a `query` table a subquery over its result columns."""
    if tc.query:
        with engine.connect() as conn:
            columns = list(conn.execute(probe_columns(tc)).keys())
        return query_source(tc, columns)
    return sa.Table(tc.table, sa.MetaData(), schema=tc.schema, autoload_with=engine)


def _check_partition_column(tc: TableConfig, table: sa.FromClause) -> None:
    """ConnectorX `partition_on` needs an integer column; fail before anything is extracted."""
    column = tc.partition.column
    if column not in table.c:
        raise PreflightError(f"table {tc.name}: partition column {column!r} is not a column of the table")
    if not isinstance(table.c[column].type, sa.Integer):
        raise PreflightError(
            f"table {tc.name}: partition column {column!r} is not an integer ({table.c[column].type}). "
            'Fix: partition on an integer column, or remove "partition"'
        )


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


def make_resource(
    cfg: SourceConfig,
    tc: TableConfig,
    req: RunRequest,
    engine: sa.Engine,
    run_started: datetime,
    table: sa.FromClause | None = None,  # the reflection preflight already did
    stats: RunStats | None = None,
    max_time_s: float | None = None,
):
    """The table's dlt resource. `max_time_s` bounds extraction (split loading, section 7.4).

    A keyset table checks the deadline itself, between pages; a stream table is
    cut by dlt's `add_limit(max_time=...)`. The caller never passes a budget for
    a ConnectorX stream table: cutting that stream short leaves its query running.
    """
    backend = cfg.backend(tc)
    if backend == "sqlalchemy":  # stopgap until the sqlalchemy backend lands
        raise NotImplementedError(
            f'table {tc.name}: backend "sqlalchemy" is not implemented yet; set "backend": "pyarrow"'
        )
    table = table if table is not None else reflect(engine, tc)
    specs = column_specs(table)
    if tc.partition:
        _check_partition_column(tc, table)
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
        # Before anything is read: a backend change would change bronze's column types (D6).
        # State only persists with a successful load, so "previous" is the last successful run's.
        state = dlt.current.resource_state()
        previous = state.get("backend")
        if previous is not None and previous != backend:
            raise BackendChanged(tc.name, previous, backend)
        state["backend"] = backend
        if backend == "connectorx":
            # ConnectorX takes SQL text, so values are inlined, as dlt's own sql_table does.
            sql = connectorx_sql(cfg.connection, literal_sql(stmt, engine.dialect))
            # ConnectorX puts the column into its own SQL as given: quote it for the engine (Postgres folds case)
            quoted = engine.dialect.identifier_preparer.quote(tc.partition.column) if tc.partition else None
            partition = (quoted, tc.partition.num) if tc.partition else None
            batches = read_connectorx(connectorx_url(cfg.connection), sql, specs, batch_size, tc.name, partition)
        else:
            batches = read_pyarrow(engine, stmt, specs, batch_size)
        for batch in batches:
            if stats is not None and tc.cursor is not None:
                nulls = batch.column(tc.cursor.column).null_count
                stats.null_cursor_rows[tc.name] = stats.null_cursor_rows.get(tc.name, 0) + nulls
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
        if tc.chunking.strategy == "keyset":
            yield from keyset_pages(cursor.start_value, cursor.end_value, cursor.last_value)
        else:
            yield from extract(cursor_select(table, tc, cursor.start_value, cursor.end_value))

    def keyset_pages(start, end, landed_up_to):
        """Page after page, each seeded by the last row of the one before; a short page is the last (7.4).

        The deadline is checked between pages only, so no query is ever left
        running. A page whose last cursor value is not past `landed_up_to` (the
        cursor in state) may hold only rows dlt drops as already landed, so the
        read never stops there: every run that stops early has landed new rows.
        """
        deadline = time.monotonic() + max_time_s if max_time_s is not None else None
        after = None
        while True:
            count, last = 0, None
            for batch in extract(keyset_select(table, tc, start, end, after)):
                count += batch.num_rows
                last = batch if batch.num_rows else last
                yield batch
            if count < tc.chunking.page_size:
                return
            after = tuple(last.column(name)[last.num_rows - 1].as_py() for name in tc.chunking.order_by)
            past_landed = landed_up_to is None or after[0] > landed_up_to
            if deadline is not None and past_landed and time.monotonic() >= deadline:
                log.info(json.dumps({"event": "time budget reached", "table": tc.name, "stopped": "between pages"}))
                return

    if max_time_s is not None and tc.chunking.strategy == "stream":
        return rows.add_limit(max_time=max_time_s)
    return rows
