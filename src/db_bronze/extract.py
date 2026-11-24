"""Backends: run a SQLAlchemy statement and yield Arrow record batches (DESIGN-db.md section 7.2).

`pyarrow`: a streaming SQLAlchemy cursor, with each batch of rows converted to
Arrow using the types from SQLAlchemy reflection, so a table's Arrow schema is
the same on every run and in every batch (D2).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import sqlalchemy as sa

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


def column_specs(table: sa.Table) -> dict[str, ColumnSpec]:
    return {c.name: ColumnSpec(arrow_type(c.type), bool(c.nullable)) for c in table.columns}


def naive_timestamp_columns(table: sa.Table) -> list[str]:
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


def with_extracted_at(batch: pa.RecordBatch, extracted_at: datetime) -> pa.RecordBatch:
    """Append `_extracted_at` (timestamp[us, UTC], one value for the whole run)."""
    column = pa.array([extracted_at] * batch.num_rows, pa.timestamp("us", tz="UTC"))
    return batch.append_column(pa.field(EXTRACTED_AT, column.type), column)
