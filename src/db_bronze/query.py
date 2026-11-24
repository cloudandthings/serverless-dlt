"""Cursor queries, built with SQLAlchemy Core (DESIGN-db.md section 7.3).

Dialect differences (quoting, subquery aliases, booleans) come from the
dialect, not from string templates, so every statement compiles offline for
every dialect.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from .config import TableConfig


class _Timestamp(FunctionElement):
    """A timestamp as the source compares it.

    SQLite has no timestamp type: values are text in whatever format the writer
    used, and SQLAlchemy binds `YYYY-MM-DD HH:MM:SS.ffffff`. Comparing the text
    would put a stored `... 09:00:00` *before* a bound `... 09:00:00.000000`, so
    a row tying the cursor would be missed. `julianday()` compares instants.
    Everywhere else the value is compared as it is, so indexes stay usable.
    """

    inherit_cache = True


@compiles(_Timestamp)
def _timestamp_default(element, compiler, **kw):
    return compiler.process(list(element.clauses)[0], **kw)


@compiles(_Timestamp, "sqlite")
def _timestamp_sqlite(element, compiler, **kw):
    return f"julianday({compiler.process(list(element.clauses)[0], **kw)})"


def cursor_select(source: sa.FromClause, tc: TableConfig, start: Any) -> sa.Select:
    """`SELECT * FROM source WHERE cursor >= start AND <where> ORDER BY cursor`."""
    cursor_col = source.c[tc.cursor.column]

    def value(v):
        return _Timestamp(v) if tc.cursor.type == "timestamp" else v

    bound = value(cursor_col) >= value(sa.literal(start, cursor_col.type))
    stmt = sa.select(source).where(bound)
    if tc.where:
        stmt = stmt.where(sa.text(tc.where))
    return stmt.order_by(cursor_col)
