"""Build one dlt resource per configured table (DESIGN-db.md section 7.1)."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from urllib.parse import quote

import dlt
import sqlalchemy as sa

from .config import ConnectionConfig, SourceConfig, TableConfig
from .extract import column_specs, naive_timestamp_columns, read_connectorx, read_pyarrow, with_extracted_at

log = logging.getLogger(__name__)

# A new source column lands; a type change on an existing column fails the run (section 7.2).
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}


def make_engine(cfg: SourceConfig) -> sa.Engine:
    return sa.create_engine(sa.URL.create("sqlite", database=cfg.connection.path))


def connectorx_url(conn: ConnectionConfig) -> str:
    """The ConnectorX URL, built from the same discrete fields as the SQLAlchemy URL (section 5.1)."""
    return "sqlite://" + quote(os.path.abspath(conn.path))


def reflect(engine: sa.Engine, tc: TableConfig) -> sa.Table:
    return sa.Table(tc.table, sa.MetaData(), schema=tc.schema, autoload_with=engine)


def make_resource(cfg: SourceConfig, tc: TableConfig, engine: sa.Engine, run_started: datetime):
    table = reflect(engine, tc)
    specs = column_specs(table)
    backend = cfg.backend(tc)
    batch_size = cfg.batch_size(tc)
    # Without this hint dlt labels naive timestamps UTC without shifting them (prototype S5).
    hints = {name: {"timezone": False} for name in naive_timestamp_columns(table)}
    cursor_col = table.c[tc.cursor.column]
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
            row_order="asc",
        ),
    ):
        stmt = sa.select(table).where(cursor_col >= cursor.start_value).order_by(cursor_col)
        if backend == "connectorx":
            # ConnectorX takes SQL text, so values are inlined, as dlt's own sql_table does.
            sql = str(stmt.compile(engine, compile_kwargs={"literal_binds": True}))
            batches = read_connectorx(connectorx_url(cfg.connection), sql, specs, batch_size)
        else:
            batches = read_pyarrow(engine, stmt, specs, batch_size)
        for batch in batches:
            yield with_extracted_at(batch, run_started)

    return rows
