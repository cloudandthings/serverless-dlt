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
    return compiler.process(next(iter(element.clauses)), **kw)


@compiles(_Timestamp, "sqlite")
def _timestamp_sqlite(element, compiler, **kw):
    return f"julianday({compiler.process(next(iter(element.clauses)), **kw)})"


class _TimestampValue(FunctionElement):
    """A timestamp value compared against the cursor column, compiled like `_Timestamp` except on SQL Server.

    Inlined for SQL Server (ConnectorX, `literal_binds`), a text literal is
    converted to the column's type, and DATETIME and SMALLDATETIME reject text
    with more than 3 decimals (error 241): a microsecond cursor value or window
    bound would fail the query. Cast to DATETIME2 (DATETIMEOFFSET with an
    offset), the literal keeps its exact value and the comparison is made at
    DATETIME2 precision, as with a bound parameter (spike S2). Bound, it is left
    alone: pyodbc already binds a DATETIME2. Not a subclass of `_Timestamp`:
    Python would call the subclass's reflected operator first and swap the
    comparison's operands.
    """

    inherit_cache = True


@compiles(_TimestampValue)
def _timestamp_value_default(element, compiler, **kw):
    return _timestamp_default(element, compiler, **kw)


@compiles(_TimestampValue, "sqlite")
def _timestamp_value_sqlite(element, compiler, **kw):
    return _timestamp_sqlite(element, compiler, **kw)


@compiles(_TimestampValue, "mssql")
def _timestamp_value_mssql(element, compiler, **kw):
    value = next(iter(element.clauses))
    rendered = compiler.process(value, **kw)
    if not kw.get("literal_binds"):
        return rendered
    aware = getattr(getattr(value, "value", None), "tzinfo", None) is not None
    return f"CAST({rendered} AS {'DATETIMEOFFSET' if aware else 'DATETIME2'})"


def query_source(tc: TableConfig, columns: list[str]) -> sa.Subquery:
    """A `query` table as a subquery with the given result columns.

    Only the cursor column has a type (from `cursor.type`), so the driver hands
    back cursor values as timestamps or integers even on engines that report no
    type for a computed column. The other columns' types come from the values.
    """
    if tc.cursor is None:  # a snapshot table: every column's type comes from the values
        return sa.text(tc.query).columns(*(sa.column(c) for c in columns)).subquery("q")
    cursor_type = sa.DateTime() if tc.cursor.type == "timestamp" else sa.Integer()
    cols = [sa.column(c, cursor_type if c == tc.cursor.column else None) for c in columns]
    return sa.text(tc.query).columns(*cols).subquery("q")


def probe_columns(tc: TableConfig) -> sa.Select:
    """A statement that returns no rows, only the result column names of a `query` table."""
    return sa.select(sa.literal_column("*")).select_from(sa.text(tc.query).columns().subquery("q")).where(sa.false())


def _projection(source: sa.FromClause, tc: TableConfig) -> list:
    return [source.c[c] for c in tc.columns] if tc.columns else list(source.c)


def snapshot_select(source: sa.FromClause, tc: TableConfig) -> sa.Select:
    """`SELECT <columns> FROM source [WHERE <where>]`: a snapshot table, read whole."""
    stmt = sa.select(*_projection(source, tc)).select_from(source)
    return stmt.where(sa.text(tc.where)) if tc.where else stmt


def _comparable(column: sa.ColumnElement, tc: TableConfig) -> sa.ColumnElement:
    """The cursor column as the source compares it (see `_Timestamp`); other columns as they are."""
    if tc.cursor and tc.cursor.type == "timestamp" and column.name == tc.cursor.column:
        return _Timestamp(column)
    return column


def _literal(value: Any, column: sa.ColumnElement) -> sa.ColumnElement:
    """A value bound with the column's type, or with a type inferred from the value when the column has none."""
    return sa.literal(value, None if isinstance(column.type, sa.types.NullType) else column.type)


def _cursor_filtered(source: sa.FromClause, tc: TableConfig, start: Any, end: Any) -> sa.Select:
    """`SELECT <columns> FROM source WHERE (<cursor in [start, end)> [OR cursor IS NULL]) AND <where>`."""
    cursor_col = source.c[tc.cursor.column]
    cmp = _comparable(cursor_col, tc)
    bound = cmp >= _value(start, cursor_col, tc)
    if end is not None:
        bound = sa.and_(bound, cmp < _value(end, cursor_col, tc))
    if tc.cursor.include_null_cursor:
        bound = sa.or_(bound, cursor_col.is_(None))
    stmt = sa.select(*_projection(source, tc)).select_from(source).where(bound)
    return stmt.where(sa.text(tc.where)) if tc.where else stmt


def _value(value: Any, column: sa.ColumnElement, tc: TableConfig) -> sa.ColumnElement:
    """A bound value compared against `column`, wrapped the same way the column is."""
    literal = _literal(value, column)
    return _TimestampValue(literal) if isinstance(_comparable(column, tc), _Timestamp) else literal


def cursor_select(source: sa.FromClause, tc: TableConfig, start: Any, end: Any = None) -> sa.Select:
    """`SELECT <columns> FROM source WHERE (<cursor in [start, end)> [OR cursor IS NULL]) AND <where> ORDER BY cursor`.

    The window is half-open; `end` None leaves it open-ended (incremental mode).
    """
    return _cursor_filtered(source, tc, start, end).order_by(source.c[tc.cursor.column])


def keyset_select(
    source: sa.FromClause, tc: TableConfig, start: Any, end: Any, after: tuple | None
) -> sa.Select:
    """One keyset page (DESIGN-db.md 7.4): the cursor window, then rows after `after` in `order_by` order.

    `after` holds the `order_by` values of the previous page's last row, or None
    for the first page. The tie-break is spelled out, `c1 > v1 OR (c1 = v1 AND
    c2 > v2) ...`, because Oracle and MSSQL have no row-value comparison. The row
    limit comes from the dialect: `LIMIT`, `TOP` or `FETCH FIRST`. Each column is
    ordered by the same expression it is compared with, so the order and the
    seek always agree.
    """
    columns = [source.c[name] for name in tc.chunking.order_by]
    stmt = _cursor_filtered(source, tc, start, end)
    if after is not None:
        terms = []
        for i, column in enumerate(columns):
            equal = [_comparable(c, tc) == _value(v, c, tc) for c, v in zip(columns[:i], after, strict=False)]
            terms.append(sa.and_(*equal, _comparable(column, tc) > _value(after[i], column, tc)))
        stmt = stmt.where(sa.or_(*terms))
    return stmt.order_by(*(_comparable(c, tc) for c in columns)).limit(tc.chunking.page_size)


def paged_rows(source: sa.FromClause, tc: TableConfig, start: Any = None) -> sa.Select:
    """What a paged snapshot reads, unordered: everything for a snapshot table, `[start, …)` for a cursor table.

    A plan counts it and every page slices it, so both see the same rows. A
    cursor table has no upper bound: the last page is open-ended anyway.
    """
    if tc.cursor is None:
        return snapshot_select(source, tc)
    return _cursor_filtered(source, tc, start, None)


def count_select(rows: sa.Select) -> sa.Select:
    """`SELECT COUNT(*) FROM (<rows>)`."""
    return sa.select(sa.func.count()).select_from(rows.subquery("counted"))


def page_order(tc: TableConfig) -> tuple[str, ...]:
    """The columns a page is ordered by: keyset `order_by` if set, else the primary key. Empty: not pageable."""
    return tc.chunking.order_by or tc.primary_key


def page_select(source: sa.FromClause, rows: sa.Select, tc: TableConfig, offset: int, limit: int | None) -> sa.Select:
    """One page of `rows`: `ORDER BY <page_order> OFFSET :offset [FETCH :limit]`, in the dialect's own syntax.

    `limit` None is the open-ended last page.
    """
    stmt = rows.order_by(*(_comparable(source.c[c], tc) for c in page_order(tc))).offset(offset)
    return stmt.limit(limit) if limit is not None else stmt


def literal_sql(stmt: sa.Select, dialect: sa.Dialect) -> str:
    """`stmt` as SQL text with every value inlined, for ConnectorX (as dlt's own sql_table does).

    SQLite has no OFFSET without LIMIT, so SQLAlchemy adds `LIMIT -1` for an
    open-ended page, but as a bind parameter even under `literal_binds`
    (SQLAlchemy 2.1.9), which ConnectorX rejects. An explicit `LIMIT -1`
    (no limit, to SQLite) is inlined like any other value.
    """
    if dialect.name == "sqlite" and stmt._offset_clause is not None and stmt._limit_clause is None:
        stmt = stmt.limit(-1)
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
