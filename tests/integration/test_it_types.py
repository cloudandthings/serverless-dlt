"""db_bronze integration: the landed Arrow type of every column type, per engine and backend.

The tables below are the record DESIGN-db.md section 10.1 quotes. A change in
any landed type changes the bronze contract, so it fails here first.

Each matrix column is (DDL type, literal value, ConnectorX reads it). Columns
ConnectorX cannot read are deny-listed or high-scale decimals: the ConnectorX
run leaves them out with `columns`, the pyarrow run lands them.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

pytestmark = pytest.mark.integration

MATRIX = {
    "postgres": {
        "c_smallint": ("smallint", "1", True),
        "c_integer": ("integer", "2", True),
        "c_bigint": ("bigint", "3", True),
        "c_numeric_18_4": ("numeric(18,4)", "12345678901234.5678", True),
        "c_numeric_28_10": ("numeric(28,10)", "123456789012345678.0123456789", True),
        "c_numeric_38_12": ("numeric(38,12)", "12345678901234567890123456.123456789012", False),
        "c_numeric": ("numeric", "1.123456789012345678", False),
        "c_real": ("real", "1.5", True),
        "c_double": ("double precision", "2.5", True),
        "c_boolean": ("boolean", "true", True),
        "c_text": ("text", "'t'", True),
        "c_varchar": ("varchar(20)", "'v'", True),
        "c_char": ("char(5)", "'c'", True),
        "c_date": ("date", "'2026-11-20'", True),
        "c_timestamp": ("timestamp", "'2026-11-20 08:00:00.123456'", True),
        "c_timestamptz": ("timestamptz", "'2026-11-20 08:00:00+02'", True),
        "c_time": ("time", "'08:00:00.5'", True),
        "c_bytea": ("bytea", "'\\x0102'", True),
        "c_uuid": ("uuid", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", True),
        "c_json": ("json", """'{"a": 1}'""", True),
        "c_jsonb": ("jsonb", """'{"a": 1}'""", True),
        "c_inet": ("inet", "'10.0.0.1'", True),
        "c_int_array": ("integer[]", "'{1,2}'", False),  # deny-listed by preflight, though ConnectorX reads it
        "c_interval": ("interval", "'1 day 02:00:00'", False),
        "c_money": ("money", "'1.50'", False),
        "c_timetz": ("timetz", "'08:00:00+02'", False),
        "c_macaddr": ("macaddr", "'08:00:2b:01:02:03'", False),
        "c_xml": ("xml", "'<a/>'", False),
    },
    "mysql": {
        "c_tinyint": ("tinyint", "1", True),
        "c_smallint": ("smallint", "2", True),
        "c_mediumint": ("mediumint", "3", True),
        "c_int": ("int", "4", True),
        "c_bigint": ("bigint", "5", True),
        "c_bigint_unsigned": ("bigint unsigned", "18446744073709551615", True),
        "c_decimal_18_4": ("decimal(18,4)", "12345678901234.5678", True),
        "c_decimal_28_10": ("decimal(28,10)", "123456789012345678.0123456789", True),
        "c_decimal_38_12": ("decimal(38,12)", "12345678901234567890123456.123456789012", False),
        "c_decimal_65_30": ("decimal(65,30)", "1.123456789012345678901234567890", False),
        "c_float": ("float", "1.5", True),
        "c_double": ("double", "2.5", True),
        "c_bool": ("boolean", "true", True),
        "c_bit": ("bit(8)", "b'10101010'", True),
        "c_varchar": ("varchar(20)", "'v'", True),
        "c_char": ("char(5)", "'c'", True),
        "c_text": ("text", "'t'", True),
        "c_enum": ("enum('a','b')", "'a'", True),
        "c_set": ("set('x','y')", "'x,y'", True),
        "c_date": ("date", "'2026-11-20'", True),
        "c_datetime": ("datetime", "'2026-11-20 08:00:00'", True),
        "c_datetime6": ("datetime(6)", "'2026-11-20 08:00:00.123456'", True),
        "c_timestamp": ("timestamp null", "'2026-11-20 08:00:00'", True),
        "c_time": ("time", "'08:00:00'", True),
        "c_year": ("year", "2026", True),
        "c_binary": ("binary(4)", "x'01020304'", True),
        "c_varbinary": ("varbinary(8)", "x'0102'", True),
        "c_blob": ("blob", "x'0102'", True),
        "c_json": ("json", """'{"a": 1}'""", True),
        "c_geometry": ("geometry", "ST_GeomFromText('POINT(1 2)')", False),
    },
}

# The landed Parquet type of each column (`_extracted_at` aside), as `str(pyarrow type)`.
LANDED = {
    ("postgres", "pyarrow"): {
        "c_smallint": "int64", "c_integer": "int64", "c_bigint": "int64",
        "c_numeric_18_4": "decimal128(18, 4)", "c_numeric_28_10": "decimal128(28, 10)",
        "c_numeric_38_12": "decimal128(38, 12)", "c_numeric": "string",
        "c_real": "double", "c_double": "double", "c_boolean": "bool",
        "c_text": "string", "c_varchar": "string", "c_char": "string",
        "c_date": "date32[day]", "c_timestamp": "timestamp[us]", "c_timestamptz": "timestamp[us, tz=UTC]",
        "c_time": "time64[us]", "c_bytea": "binary", "c_uuid": "string", "c_json": "string", "c_jsonb": "string",
        "c_inet": "string", "c_int_array": "list<element: int64>",
        "c_interval": "string", "c_money": "string", "c_timetz": "string", "c_macaddr": "string", "c_xml": "string",
    },
    ("postgres", "connectorx"): {
        # ConnectorX's arrow_stream widens every integer to int64 and real to double on Postgres
        "c_smallint": "int64", "c_integer": "int64", "c_bigint": "int64",
        "c_numeric_18_4": "decimal128(38, 10)", "c_numeric_28_10": "decimal128(38, 10)",
        "c_real": "double", "c_double": "double", "c_boolean": "bool",
        "c_text": "string", "c_varchar": "string", "c_char": "string",
        "c_date": "date32[day]", "c_timestamp": "timestamp[us]", "c_timestamptz": "timestamp[us, tz=UTC]",
        "c_time": "time64[us]", "c_bytea": "large_binary", "c_uuid": "string", "c_json": "string", "c_jsonb": "string",
        "c_inet": "string",
    },
    ("mysql", "pyarrow"): {
        "c_tinyint": "int64", "c_smallint": "int64", "c_mediumint": "int64", "c_int": "int64", "c_bigint": "int64",
        "c_bigint_unsigned": "uint64",
        "c_decimal_18_4": "decimal128(18, 4)", "c_decimal_28_10": "decimal128(28, 10)",
        "c_decimal_38_12": "decimal128(38, 12)", "c_decimal_65_30": "decimal256(65, 30)",
        "c_float": "double", "c_double": "double", "c_bool": "int64", "c_bit": "int64",
        "c_varchar": "string", "c_char": "string", "c_text": "string", "c_enum": "string", "c_set": "string",
        "c_date": "date32[day]", "c_datetime": "timestamp[us]", "c_datetime6": "timestamp[us]",
        "c_timestamp": "timestamp[us]", "c_time": "time64[us]", "c_year": "int64",
        "c_binary": "binary", "c_varbinary": "binary", "c_blob": "binary", "c_json": "string", "c_geometry": "binary",
    },
    ("mysql", "connectorx"): {
        "c_tinyint": "int8", "c_smallint": "int16", "c_mediumint": "int32", "c_int": "int32", "c_bigint": "int64",
        "c_bigint_unsigned": "uint64",
        "c_decimal_18_4": "decimal128(38, 10)", "c_decimal_28_10": "decimal128(38, 10)",
        "c_float": "float", "c_double": "double", "c_bool": "int8", "c_bit": "large_binary",
        "c_varchar": "string", "c_char": "string", "c_text": "string", "c_enum": "string", "c_set": "string",
        "c_date": "date32[day]", "c_datetime": "timestamp[us]", "c_datetime6": "timestamp[us]",
        "c_timestamp": "timestamp[us]", "c_time": "time64[us]", "c_year": "int16",
        "c_binary": "large_binary", "c_varbinary": "large_binary", "c_blob": "large_binary", "c_json": "string",
    },
}

# Values that must survive exactly: the wide decimals, and what the pyarrow backend turns into text.
EXACT = {
    "postgres": {
        "c_numeric_28_10": Decimal("123456789012345678.0123456789"),
        "c_timestamp": datetime(2026, 11, 20, 8, 0, 0, 123456),
        "c_uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "c_json": '{"a":1}',
        "c_inet": "10.0.0.1",
    },
    "mysql": {
        "c_bigint_unsigned": 18446744073709551615,
        "c_decimal_28_10": Decimal("123456789012345678.0123456789"),
        "c_datetime6": datetime(2026, 11, 20, 8, 0, 0, 123456),
        "c_set": "x,y",
        "c_json": '{"a":1}',
    },
}
EXACT_PYARROW = {
    "postgres": {
        "c_numeric_38_12": Decimal("12345678901234567890123456.123456789012"),
        "c_numeric": "1.123456789012345678",
        "c_interval": "1 day, 2:00:00",
        "c_timetz": "08:00:00+02:00",
        "c_int_array": [1, 2],
    },
    "mysql": {
        "c_decimal_38_12": Decimal("12345678901234567890123456.123456789012"),
        "c_decimal_65_30": Decimal("1.123456789012345678901234567890"),
        "c_double": 2.5,
    },
}


def seed_matrix(source) -> dict[str, tuple[str, str, bool]]:
    matrix = MATRIX[source.dialect]
    columns = ", ".join(f"{name} {ddl}" for name, (ddl, _, _) in matrix.items())
    values = ", ".join(value for _, value, _ in matrix.values())
    source.execute(
        f"CREATE TABLE type_matrix (id integer PRIMARY KEY, {columns})",
        f"INSERT INTO type_matrix VALUES (1, {values})",
        "INSERT INTO type_matrix (id) VALUES (2)",  # every type NULL as well
    )
    return matrix


def test_landed_types_per_engine_and_backend(source, run, backend):
    matrix = seed_matrix(source)
    readable = [name for name, (_, _, cx_reads) in matrix.items() if backend == "pyarrow" or cx_reads]
    table = {"name": "type_matrix", "table": "type_matrix", "columns": ["id", *readable], "backend": backend}
    result = run({"source_id": "types", "mode": "full"}, table)
    assert result["rows"] == {"type_matrix": 2}
    schema = pq.read_schema(source.files("type_matrix")[0])
    landed = {f.name: str(f.type) for f in schema if f.name not in ("id", "_extracted_at")}
    assert landed == LANDED[(source.dialect, backend)]
    [row, empty] = sorted(source.landed("type_matrix"), key=lambda r: r["id"])
    exact = EXACT[source.dialect] | (EXACT_PYARROW[source.dialect] if backend == "pyarrow" else {})
    assert {name: row[name] for name in exact} == exact
    assert {name for name, value in empty.items() if value is not None} == {"id", "_extracted_at"}
