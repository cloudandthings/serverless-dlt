"""db_bronze integration: the landed Arrow type of every column type, per engine and backend.

The tables below are the record DESIGN-db.md section 10.1 quotes. A change in
any landed type changes the bronze contract, so it fails here first.

Each matrix column is (DDL type, literal value, ConnectorX reads it). Columns
ConnectorX cannot read are deny-listed or high-scale decimals: the ConnectorX
run leaves them out with `columns`, the pyarrow run lands them.
"""

from __future__ import annotations

from datetime import UTC, datetime, time
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
    "mssql": {
        "c_tinyint": ("tinyint", "1", True),
        "c_smallint": ("smallint", "2", True),
        "c_int": ("int", "3", True),
        "c_bigint": ("bigint", "4", True),
        "c_bit": ("bit", "1", True),
        "c_decimal_18_4": ("decimal(18,4)", "12345678901234.5678", True),
        "c_decimal_28_10": ("decimal(28,10)", "123456789012345678.0123456789", True),
        "c_decimal_38_12": ("decimal(38,12)", "12345678901234567890123456.123456789012", False),
        "c_decimal_38_0": ("decimal(38,0)", "12345678901234567890123456789012345678", False),
        "c_money": ("money", "922337203685477.5807", False),  # ConnectorX: a double, 19 digits do not fit
        "c_smallmoney": ("smallmoney", "214748.3647", True),
        "c_float": ("float", "2.5", True),
        "c_real": ("real", "1.5", True),
        "c_char": ("char(5)", "'c'", True),
        "c_varchar": ("varchar(20)", "'v'", True),
        "c_varchar_max": ("varchar(max)", "'vm'", True),
        "c_nchar": ("nchar(5)", "N'n'", True),
        "c_nvarchar": ("nvarchar(20)", "N'ünï'", True),
        "c_nvarchar_max": ("nvarchar(max)", "N'nm'", True),
        "c_text": ("text", "'t'", True),
        "c_ntext": ("ntext", "N'nt'", True),
        "c_date": ("date", "'2026-11-20'", True),
        "c_datetime": ("datetime", "'2026-11-20T08:00:00.123'", True),
        "c_datetime2": ("datetime2", "'2026-11-20T08:00:00.1234567'", True),
        "c_datetime2_6": ("datetime2(6)", "'2026-11-20T08:00:00.123456'", True),
        "c_smalldatetime": ("smalldatetime", "'2026-11-20T08:00:00'", True),
        # ConnectorX: the wrong instant (it applies the offset twice)
        "c_datetimeoffset": ("datetimeoffset", "'2026-11-20T08:00:00.123456+02:00'", False),
        "c_time": ("time", "'08:00:00.1234567'", True),
        "c_binary": ("binary(4)", "0x01020304", True),
        "c_varbinary": ("varbinary(8)", "0x0102", True),
        "c_varbinary_max": ("varbinary(max)", "0x0102", True),
        "c_image": ("image", "0x0102", True),
        "c_uniqueidentifier": ("uniqueidentifier", "'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'", True),
        "c_sql_variant": ("sql_variant", "CAST(42 AS int)", False),  # neither backend: see UNREADABLE
        "c_xml": ("xml", "'<a/>'", False),
        "c_hierarchyid": ("hierarchyid", "'/1/'", False),
        "c_geography": ("geography", "geography::Point(1, 2, 4326)", False),
        "c_geometry": ("geometry", "geometry::Point(1, 2, 0)", False),
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
    ("mssql", "pyarrow"): {
        "c_tinyint": "int64", "c_smallint": "int64", "c_int": "int64", "c_bigint": "int64", "c_bit": "bool",
        "c_decimal_18_4": "decimal128(18, 4)", "c_decimal_28_10": "decimal128(28, 10)",
        "c_decimal_38_12": "decimal128(38, 12)", "c_decimal_38_0": "decimal128(38, 0)",
        "c_money": "decimal128(19, 4)", "c_smallmoney": "decimal128(10, 4)", "c_float": "double", "c_real": "double",
        "c_char": "string", "c_varchar": "string", "c_varchar_max": "string", "c_nchar": "string",
        "c_nvarchar": "string", "c_nvarchar_max": "string", "c_text": "string", "c_ntext": "string",
        "c_date": "date32[day]", "c_datetime": "timestamp[us]", "c_datetime2": "timestamp[us]",
        "c_datetime2_6": "timestamp[us]", "c_smalldatetime": "timestamp[us]",
        "c_datetimeoffset": "timestamp[us, tz=UTC]", "c_time": "time64[us]",
        "c_binary": "binary", "c_varbinary": "binary", "c_varbinary_max": "binary", "c_image": "binary",
        "c_uniqueidentifier": "string", "c_xml": "string",
        "c_hierarchyid": "binary", "c_geography": "binary", "c_geometry": "binary",
    },
    ("mssql", "connectorx"): {
        # ConnectorX widens tinyint, smallint and int to int64 and real to double on SQL Server too
        "c_tinyint": "int64", "c_smallint": "int64", "c_int": "int64", "c_bigint": "int64", "c_bit": "bool",
        "c_decimal_18_4": "decimal128(38, 10)", "c_decimal_28_10": "decimal128(38, 10)",
        "c_smallmoney": "double", "c_float": "double", "c_real": "double",
        "c_char": "string", "c_varchar": "string", "c_varchar_max": "string", "c_nchar": "string",
        "c_nvarchar": "string", "c_nvarchar_max": "string", "c_text": "string", "c_ntext": "string",
        "c_date": "date32[day]", "c_datetime": "timestamp[us]", "c_datetime2": "timestamp[us]",
        "c_datetime2_6": "timestamp[us]", "c_smalldatetime": "timestamp[us]", "c_time": "time64[us]",
        "c_binary": "large_binary", "c_varbinary": "large_binary", "c_varbinary_max": "large_binary",
        "c_image": "large_binary", "c_uniqueidentifier": "string",
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
    "mssql": {
        "c_decimal_28_10": Decimal("123456789012345678.0123456789"),
        "c_datetime2": datetime(2026, 11, 20, 8, 0, 0, 123456),  # 100 ns truncated to the microsecond
        "c_datetime2_6": datetime(2026, 11, 20, 8, 0, 0, 123456),
        "c_time": time(8, 0, 0, 123456),
        "c_nvarchar": "\u00fcn\u00ef",
        "c_uniqueidentifier": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
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
    "mssql": {
        "c_decimal_38_12": Decimal("12345678901234567890123456.123456789012"),
        "c_money": Decimal("922337203685477.5807"),
        "c_datetime": datetime(2026, 11, 20, 8, 0, 0, 123000),  # ConnectorX: .123333, the 1/300 s DATETIME holds
        "c_datetimeoffset": datetime(2026, 11, 20, 6, 0, 0, 123456, tzinfo=UTC),  # the instant, in UTC
        "c_xml": "<a/>",
        "c_hierarchyid": b"X",  # SQL Server's serialization of /1/, as CAST(... AS varbinary) gives it
    },
}

# Columns no backend lands losslessly: preflight fails them on pyarrow as well. pyodbc reads a
# sql_variant only as the value's bytes, without its type.
UNREADABLE = {"mssql": {"c_sql_variant"}}


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
    unreadable = UNREADABLE.get(source.dialect, set())
    readable = [
        name for name, (_, _, cx_reads) in matrix.items()
        if (backend == "pyarrow" or cx_reads) and name not in unreadable
    ]
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


def test_the_preflight_fails_exactly_the_columns_a_backend_cannot_land(source, run, backend):
    """Every deny-listed type and high-scale decimal on ConnectorX; on pyarrow only what no backend reads."""
    matrix = seed_matrix(source)
    table = {"name": "type_matrix", "table": "type_matrix", "backend": backend}
    report = run({"source_id": "types", "mode": "validate"}, table)
    [checked] = report["tables"]
    failed = {finding.split(":")[0].removeprefix("column ") for finding in checked["findings"]}
    unreadable = UNREADABLE.get(source.dialect, set())
    cannot = {name for name, (_, _, cx_reads) in matrix.items() if not cx_reads} if backend == "connectorx" else set()
    assert failed == cannot | unreadable
    assert source.files("type_matrix") == []
