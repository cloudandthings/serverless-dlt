"""Backends: run a SQLAlchemy statement and yield Arrow record batches (DESIGN-db.md section 7.2).

`connectorx`: ConnectorX `arrow_stream`; its record batches are yielded as they
come, with the types ConnectorX produces (D2).

`pyarrow`: a streaming SQLAlchemy cursor, with each batch of rows converted to
Arrow using the types from SQLAlchemy reflection, so a table's Arrow schema is
the same on every run and in every batch (D2).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime

import connectorx as cx
import pyarrow as pa
import sqlalchemy as sa

from .errors import ConnectorXFailed

EXTRACTED_AT = "_extracted_at"


def arrow_type(col_type: sa.types.TypeEngine) -> pa.DataType | None:
    """Arrow type for a reflected column type, or None to let pyarrow infer it from the values.

    Integers all map to int64: lossless for every engine's integer types.
    """
    if isinstance(col_type, sa.Boolean):
        return pa.bool_()
    if isinstance(col_type, sa.Integer):
        return pa.int64()
    if isinstance(col_type, sa.Float):
        return pa.float64()
    if isinstance(col_type, sa.Numeric):
        p, s = col_type.precision, col_type.scale
        if p is None:
            return None
        return pa.decimal128(p, s or 0) if p <= 38 else pa.decimal256(p, s or 0)
    if isinstance(col_type, sa.DateTime):
        return pa.timestamp("us", tz="UTC" if col_type.timezone else None)
    if isinstance(col_type, sa.Date):
        return pa.date32()
    if isinstance(col_type, sa.Time):
        return pa.time64("us")
    if isinstance(col_type, sa.Interval):
        return pa.duration("us")
    if isinstance(col_type, sa.String):
        return pa.string()
    if isinstance(col_type, sa.LargeBinary):
        return pa.binary()
    return None


@dataclass(frozen=True)
class ColumnSpec:
    """What reflection says about one column: its Arrow type (None: infer) and nullability."""

    type: pa.DataType | None
    nullable: bool


def column_specs(table: sa.FromClause) -> dict[str, ColumnSpec]:
    """Specs for a reflected table, or for a `query` subquery (whose columns carry no nullability)."""
    return {c.name: ColumnSpec(arrow_type(c.type), bool(getattr(c, "nullable", True))) for c in table.columns}


def naive_timestamp_columns(table: sa.FromClause) -> list[str]:
    """Columns the reflection reports as timestamps without a time zone."""
    return [c.name for c in table.columns if isinstance(c.type, sa.DateTime) and not c.type.timezone]


def read_pyarrow(
    engine: sa.Engine, stmt: sa.Select, specs: dict[str, ColumnSpec], batch_size: int
) -> Iterator[pa.RecordBatch]:
    """Stream `stmt` and yield one RecordBatch per `batch_size` rows."""
    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True, yield_per=batch_size).execute(stmt)
        names = list(result.keys())
        for rows in result.partitions():
            arrays, fields = [], []
            for name, values in zip(names, zip(*rows, strict=True), strict=True):
                spec = specs[name]
                array = pa.array(values, type=spec.type)
                arrays.append(array)
                fields.append(pa.field(name, array.type, nullable=spec.nullable))
            yield pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _is_connectorx_failure(exc: BaseException) -> bool:
    # pyo3_runtime.PanicException derives from BaseException and is not importable by name.
    return isinstance(exc, RuntimeError) or type(exc).__name__ == "PanicException"


def _guarded(url: str, sql: str, batch_size: int, table: str) -> Iterator[pa.RecordBatch]:
    """ConnectorX's call and its iteration, and nothing else, with its failures as ConnectorXFailed.

    The consumer's own errors never pass through here: a generator only sees its own exceptions.
    """
    try:
        yield from cx.read_sql(url, sql, return_type="arrow_stream", batch_size=batch_size)
    except BaseException as e:
        if _is_connectorx_failure(e):
            raise ConnectorXFailed(table, f"{type(e).__name__}: {e}") from e
        raise


def read_connectorx(
    url: str, sql: str, specs: dict[str, ColumnSpec], batch_size: int, table: str = "?"
) -> Iterator[pa.RecordBatch]:
    """Stream `sql` through ConnectorX and yield its record batches, typed as ConnectorX returns them.

    ConnectorX marks every column nullable, so nullability is taken from the
    reflection, as on the pyarrow backend. Only the flag changes, never a type.

    Callers read the stream to the end. Dropping it early returns at once, but
    ConnectorX's producer thread detaches and runs the query to the end with the
    connection open (prototype S1, section 7.4).
    """
    for batch in _guarded(url, sql, batch_size, table):
        schema = pa.schema(f.with_nullable(specs[f.name].nullable) for f in batch.schema)
        yield pa.RecordBatch.from_arrays(batch.columns, schema=schema)


def with_extracted_at(batch: pa.RecordBatch, extracted_at: datetime) -> pa.RecordBatch:
    """Append `_extracted_at` (timestamp[us, UTC], one value for the whole run)."""
    column = pa.array([extracted_at] * batch.num_rows, pa.timestamp("us", tz="UTC"))
    return batch.append_column(pa.field(EXTRACTED_AT, column.type), column)
