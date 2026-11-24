"""Backends: run a SQLAlchemy statement and yield Arrow record batches (DESIGN-db.md section 7.2).

`connectorx`: ConnectorX `arrow_stream`; its record batches are yielded as they
come, with the types ConnectorX produces (D2).

`pyarrow`: a streaming SQLAlchemy cursor, with each batch of rows converted to
Arrow using the types from SQLAlchemy reflection, so a table's Arrow schema is
the same on every run and in every batch (D2). Driver values Arrow cannot take
(UUIDs, network addresses, parsed JSON, MySQL SETs, Postgres bit strings,
intervals, time-with-time-zone, Oracle BFILE locators) and unconstrained NUMERIC
land as text, losslessly (DESIGN-db.md section 10.1).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import connectorx as cx
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, oracle
from sqlalchemy.dialects import postgresql as pg

from .errors import ConnectorXFailed

EXTRACTED_AT = "_extracted_at"


def _json_text(value: Any) -> str:
    """Compact JSON text. Decimals (Oracle's JSON numbers) are written exactly, as their own digits."""
    if isinstance(value, dict):
        return "{" + ",".join(f"{_json_text(str(k))}:{_json_text(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_json_text(v) for v in value) + "]"
    if isinstance(value, Decimal):
        return str(value)
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _bfile_text(lob: Any) -> str:
    """An Oracle BFILE is a locator of a file on the server: it lands as `<directory>/<file name>`."""
    return "/".join(lob.getfilename())


def _untyped(value: Any) -> Any:
    """A value of a column reflection could not type: Oracle's INTERVAL YEAR TO MONTH lands as its own text."""
    if type(value).__name__ == "IntervalYM":
        return f"{value.years}-{value.months}"
    return value


def _mysql_set_text(col_type: mysql.SET) -> Callable[[Any], str]:
    # SQLAlchemy hands a MySQL SET over as a Python set; MySQL's own text lists members in definition order.
    return lambda members: ",".join(v for v in col_type.values if v in members)


def _converter(col_type: sa.types.TypeEngine) -> Callable[[Any], Any] | None:
    """How to turn a driver value Arrow cannot take into text, or None when Arrow takes it as it is."""
    if isinstance(col_type, sa.JSON):
        return _json_text
    if isinstance(col_type, mysql.SET):
        return _mysql_set_text(col_type)
    if isinstance(col_type, (sa.Uuid, pg.INET, pg.CIDR, pg.BIT)):
        return str
    if isinstance(col_type, (sa.Interval, pg.INTERVAL, oracle.INTERVAL)):
        return str  # dlt 1.32 does not take Arrow durations
    if isinstance(col_type, oracle.BFILE):
        return _bfile_text
    if isinstance(col_type, sa.types.NullType):
        return _untyped
    if isinstance(col_type, sa.Time) and col_type.timezone:
        return lambda t: t.isoformat()  # time64 has no offset; text keeps it
    if isinstance(col_type, sa.Numeric) and not isinstance(col_type, sa.Float) and col_type.precision is None:
        return str  # unconstrained NUMERIC: no decimal type holds every value
    if isinstance(col_type, sa.Float):
        return float  # MySQL DOUBLE reflects as asdecimal=True and arrives as Decimal
    return None


def arrow_type(col_type: sa.types.TypeEngine) -> pa.DataType | None:
    """Arrow type for a reflected column type, or None to let pyarrow infer it from the values.

    Integers map to int64, lossless for every engine's integer types except
    MySQL's BIGINT UNSIGNED, which maps to uint64, and Oracle's NUMBER(19..38),
    which maps to decimal128(p, 0).
    """
    if isinstance(col_type, sa.types.NullType):
        return None
    if _converter(col_type) not in (None, float):
        return pa.string()
    if isinstance(col_type, sa.Boolean):
        return pa.bool_()
    if isinstance(col_type, sa.BigInteger) and getattr(col_type, "unsigned", False):
        return pa.uint64()
    if isinstance(col_type, oracle.NUMBER) and col_type.scale == 0 and (col_type.precision or 0) > 18:
        return pa.decimal128(col_type.precision, 0)  # Oracle NUMBER(19..38): wider than int64
    if isinstance(col_type, oracle.NUMBER) and (col_type.scale or 0) > 0:
        return pa.decimal128(col_type.precision, col_type.scale)  # oracle.NUMBER is an Integer as well
    if isinstance(col_type, sa.Integer):
        return pa.int64()
    if isinstance(col_type, sa.Float):
        return pa.float64()
    if isinstance(col_type, sa.Numeric):
        p, s = col_type.precision, col_type.scale
        return pa.decimal128(p, s or 0) if p <= 38 else pa.decimal256(p, s or 0)
    if isinstance(col_type, sa.DateTime):
        return pa.timestamp("us", tz="UTC" if col_type.timezone else None)
    if isinstance(col_type, sa.Date):
        return pa.date32()
    if isinstance(col_type, sa.Time):
        return pa.time64("us")
    if isinstance(col_type, sa.String):
        return pa.string()
    if isinstance(col_type, sa.LargeBinary):
        return pa.binary()
    return None


@dataclass(frozen=True)
class ColumnSpec:
    """What reflection says about one column: its Arrow type (None: infer), nullability, and value conversion."""

    type: pa.DataType | None
    nullable: bool
    convert: Callable[[Any], Any] | None = None

    def array(self, values: tuple) -> pa.Array:
        if self.convert is not None:
            values = [None if v is None else self.convert(v) for v in values]
        return pa.array(values, type=self.type)


def column_specs(table: sa.FromClause) -> dict[str, ColumnSpec]:
    """Specs for a reflected table, or for a `query` subquery (whose columns carry no nullability)."""
    return {
        c.name: ColumnSpec(arrow_type(c.type), bool(getattr(c, "nullable", True)), _converter(c.type))
        for c in table.columns
    }


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
                array = spec.array(values)
                arrays.append(array)
                fields.append(pa.field(name, array.type, nullable=spec.nullable))
            yield pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _is_connectorx_failure(exc: BaseException) -> bool:
    # pyo3_runtime.PanicException derives from BaseException and is not importable by name.
    return isinstance(exc, RuntimeError) or type(exc).__name__ == "PanicException"


def _guarded(
    url: str, sql: str, batch_size: int, table: str, partition: tuple[str, int] | None = None
) -> Iterator[pa.RecordBatch]:
    """ConnectorX's call and its iteration, and nothing else, with its failures as ConnectorXFailed.

    The consumer's own errors never pass through here: a generator only sees its own exceptions.
    """
    try:
        options = {"partition_on": partition[0], "partition_num": partition[1]} if partition else {}
        yield from cx.read_sql(url, sql, return_type="arrow_stream", batch_size=batch_size, **options)
    except BaseException as e:
        if _is_connectorx_failure(e):
            raise ConnectorXFailed(table, f"{type(e).__name__}: {e}") from e
        raise


def read_connectorx(
    url: str,
    sql: str,
    specs: dict[str, ColumnSpec],
    batch_size: int,
    table: str = "?",
    partition: tuple[str, int] | None = None,
    names: list[str] | None = None,
) -> Iterator[pa.RecordBatch]:
    """Stream `sql` through ConnectorX and yield its record batches, typed as ConnectorX returns them.

    ConnectorX marks every column nullable, so nullability is taken from the
    reflection, as on the pyarrow backend. Only the flag changes, never a type.

    `names` are the statement's column names as SQLAlchemy reports them, applied
    by position. ConnectorX reports the names the server gives, and Oracle gives
    a case-insensitive name in upper case (`ID`) where SQLAlchemy and the config
    say `id` (integration, Oracle), so both backends land the same names.

    Callers read the stream to the end. Dropping it early returns at once, but
    ConnectorX's producer thread detaches and runs the query to the end with the
    connection open (prototype S1, section 7.4).

    `partition` is (column, N): ConnectorX `partition_on` over N parallel
    connections. Batches of different partitions interleave, so rows do not
    arrive in cursor order (section 3.1).
    """
    for batch in _guarded(url, sql, batch_size, table, partition):
        fields = [f.with_name(n) for f, n in zip(batch.schema, names, strict=True)] if names else batch.schema
        schema = pa.schema(f.with_nullable(specs[f.name].nullable) for f in fields)
        yield pa.RecordBatch.from_arrays(batch.columns, schema=schema)


def with_extracted_at(batch: pa.RecordBatch, extracted_at: datetime) -> pa.RecordBatch:
    """Append `_extracted_at` (timestamp[us, UTC], one value for the whole run)."""
    column = pa.array([extracted_at] * batch.num_rows, pa.timestamp("us", tz="UTC"))
    return batch.append_column(pa.field(EXTRACTED_AT, column.type), column)
