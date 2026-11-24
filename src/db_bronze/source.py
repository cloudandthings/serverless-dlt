"""Build one dlt resource per configured table (DESIGN-db.md section 7.1)."""

from __future__ import annotations

import logging
from datetime import datetime

import dlt
import sqlalchemy as sa

from .config import SourceConfig, TableConfig
from .extract import column_specs, naive_timestamp_columns, read_pyarrow, with_extracted_at
from .query import cursor_select, probe_columns, query_source

log = logging.getLogger(__name__)

# A new source column lands; a type change on an existing column fails the run (section 7.2).
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}


def make_engine(cfg: SourceConfig) -> sa.Engine:
    return sa.create_engine(sa.URL.create("sqlite", database=cfg.connection.path))


def reflect(engine: sa.Engine, tc: TableConfig) -> sa.FromClause:
    """The reflected source table, or for a `query` table a subquery over its result columns."""
    if tc.query:
        with engine.connect() as conn:
            columns = list(conn.execute(probe_columns(tc)).keys())
        return query_source(tc, columns)
    return sa.Table(tc.table, sa.MetaData(), schema=tc.schema, autoload_with=engine)


def make_resource(cfg: SourceConfig, tc: TableConfig, engine: sa.Engine, run_started: datetime):
    table = reflect(engine, tc)
    specs = column_specs(table)
    batch_size = cfg.batch_size(tc)
    # Without this hint dlt labels naive timestamps UTC without shifting them (prototype S5).
    hints = {name: {"timezone": False} for name in naive_timestamp_columns(table)}
    initial = tc.cursor.initial_value if tc.cursor.type == "integer" else tc.cursor.initial_timestamp

    @dlt.resource(
        name=tc.name,
        write_disposition="append",  # never "replace": it deletes sibling mode= partitions
        primary_key=list(tc.primary_key) or None,
        columns=hints,
        schema_contract=SCHEMA_CONTRACT,
    )
    def rows(
        # The incremental MUST be the argument's default value: dlt binds state and
        # filtering only to an incremental found in the signature. Building it in
        # the body silently disables both.
        cursor=dlt.sources.incremental(  # noqa: B008
            tc.cursor.column,
            initial_value=initial,
            lag=tc.cursor.lag,  # dlt steps cursor.start_value back by this
            row_order="asc",
            on_cursor_value_missing="include" if tc.cursor.include_null_cursor else "raise",
        ),
    ):
        stmt = cursor_select(table, tc, cursor.start_value)
        for batch in read_pyarrow(engine, stmt, specs, batch_size):
            yield with_extracted_at(batch, run_started)

    return rows
