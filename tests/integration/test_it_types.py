"""db_bronze integration: the landed Arrow type of every column type, per engine and backend.

The tables below are the record DESIGN-db.md section 10.1 quotes. A change in
any landed type changes the bronze contract, so it fails here first.

Each matrix column is (DDL type, literal value, ConnectorX reads it). Columns
ConnectorX cannot read are deny-listed or high-scale decimals: the ConnectorX
run leaves them out with `columns`, the pyarrow run lands them.
"""

from __future__ import annotations

from datetime import UTC, datetime
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
    "oracle": {
        "c_number_10": ("number(10)", "1234567890", True),
        "c_number_18": ("number(18)", "123456789012345678", True),
        "c_number_38": ("number(38)", "12345678901234567890123456789012345678", False),  # see FAILS_LOUDLY
        "c_integer": ("integer", "42", True),
        "c_number_18_4": ("number(18,4)", "12345678901234.5678", True),
        "c_number_28_10": ("number(28,10)", "123456789012345678.0123456789", True),
        "c_number_38_12": ("number(38,12)", "12345678901234567890123456.123456789012", False),
        "c_number": ("number", "1.123456789012345678", False),
        "c_float": ("float", "1.5", True),
        "c_binary_float": ("binary_float", "1.5", True),
        "c_binary_double": ("binary_double", "2.5", True),
        "c_varchar2": ("varchar2(20)", "'v'", True),
        "c_nvarchar2": ("nvarchar2(20)", "'nv'", True),
        "c_char": ("char(5)", "'c'", True),
        "c_nchar": ("nchar(5)", "'nc'", True),
        "c_clob": ("clob", "'cl'", True),
        "c_blob": ("blob", "hextoraw('0102')", True),
        "c_date": ("date", "timestamp '2026-11-20 08:00:01'", True),
        "c_timestamp": ("timestamp", "timestamp '2026-11-20 08:00:00.123456'", True),
        "c_timestamp_3": ("timestamp(3)", "timestamp '2026-11-20 08:00:00.123'", True),
        "c_timestamptz": ("timestamp with time zone", "timestamp '2026-11-20 08:00:00 +02:00'", False),
        "c_boolean": ("boolean", "true", False),  # 23ai
        "c_json": ("json", """'{"a": 1}'""", False),  # 23ai
        "c_raw": ("raw(16)", "hextoraw('0102')", False),
        "c_rowid": ("rowid", "chartorowid('AAAAAAAAAAAAAAAAAA')", False),
        "c_interval_ds": ("interval day to second", "interval '1 02:00:00' day to second", False),
        "c_interval_ym": ("interval year to month", "interval '1-2' year to month", False),
        "c_tsltz": ("timestamp with local time zone", "timestamp '2026-11-20 08:00:00 +02:00'", False),
        "c_nclob": ("nclob", "'ncl'", False),
        "c_bfile": ("bfile", "bfilename('DATA_PUMP_DIR', 'x.txt')", False),
        "c_xmltype": ("xmltype", "xmltype('<a/>')", False),
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
    ("oracle", "pyarrow"): {
        "c_number_10": "int64", "c_number_18": "int64", "c_number_38": "decimal128(38, 0)", "c_integer": "int64",
        "c_number_18_4": "decimal128(18, 4)", "c_number_28_10": "decimal128(28, 10)",
        "c_number_38_12": "decimal128(38, 12)", "c_number": "string",
        "c_float": "double", "c_binary_float": "double", "c_binary_double": "double",
        "c_varchar2": "string", "c_nvarchar2": "string", "c_char": "string", "c_nchar": "string",
        "c_clob": "string", "c_blob": "binary",
        "c_date": "timestamp[us]", "c_timestamp": "timestamp[us]", "c_timestamp_3": "timestamp[us]",
        "c_timestamptz": "timestamp[us, tz=UTC]", "c_boolean": "bool", "c_json": "string",
        "c_raw": "binary", "c_rowid": "string", "c_interval_ds": "string", "c_interval_ym": "string",
        "c_tsltz": "timestamp[us]", "c_nclob": "string", "c_bfile": "string", "c_xmltype": "string",
    },
    ("oracle", "connectorx"): {
        "c_number_10": "int64", "c_number_18": "int64", "c_integer": "int64",
        "c_number_18_4": "decimal128(38, 10)", "c_number_28_10": "decimal128(38, 10)",
        "c_float": "double", "c_binary_float": "double", "c_binary_double": "double",
        "c_varchar2": "string", "c_nvarchar2": "string", "c_char": "string", "c_nchar": "string",
        "c_clob": "string", "c_blob": "large_binary",
        "c_date": "timestamp[us]", "c_timestamp": "timestamp[us]", "c_timestamp_3": "timestamp[us]",
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
    "oracle": {
        "c_number_18": 123456789012345678,
        "c_number_28_10": Decimal("123456789012345678.0123456789"),
        "c_date": datetime(2026, 11, 20, 8, 0, 1),
        "c_timestamp": datetime(2026, 11, 20, 8, 0, 0, 123456),
        "c_char": "c    ",
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
    "oracle": {
        "c_number_38": Decimal("12345678901234567890123456789012345678"),
        "c_number_38_12": Decimal("12345678901234567890123456.123456789012"),
        "c_number": "1.123456789012345678",
        "c_timestamptz": datetime(2026, 11, 20, 6, 0, tzinfo=UTC),  # the instant, not the wall time
        "c_tsltz": datetime(2026, 11, 20, 6, 0),  # in the session time zone: UTC, as on Lambda
        "c_json": '{"a":1}',
        "c_raw": b"\x01\x02",
        "c_interval_ds": "1 day, 2:00:00",
        "c_interval_ym": "1-2",
        "c_bfile": "DATA_PUMP_DIR/x.txt",
        "c_boolean": True,
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


# Columns ConnectorX reads until a value does not fit: the read then fails loudly, so preflight lets them through.
FAILS_LOUDLY = {"oracle": {"c_number_38": "PosOverflow"}}  # NUMBER(p, 0) is read as int64


def test_every_column_connectorx_cannot_read_fails_the_preflight(source, run):
    """The deny lists and decimal rules, confirmed: exactly the matrix's unreadable columns are findings."""
    matrix = seed_matrix(source)
    table = {"name": "type_matrix", "table": "type_matrix", "backend": "connectorx"}
    report = run({"source_id": "types", "mode": "validate"}, table)
    flagged = {finding.split(":")[0].removeprefix("column ") for finding in report["tables"][0]["findings"]}
    unreadable = {name for name, (_, _, cx_reads) in matrix.items() if not cx_reads}
    assert flagged == unreadable - set(FAILS_LOUDLY.get(source.dialect, {}))


def test_a_value_connectorx_cannot_hold_fails_the_run_loudly(source, run):
    loud = FAILS_LOUDLY.get(source.dialect)
    if not loud:
        pytest.skip(f"no such column type on {source.dialect}")
    seed_matrix(source)
    for column, reason in loud.items():
        table = {"name": "loud", "table": "type_matrix", "columns": ["id", column], "backend": "connectorx"}
        with pytest.raises(Exception) as failure:
            run({"source_id": "types", "mode": "full"}, table)
        assert type(failure.value).__name__ == "ConnectorXFailed" and reason in str(failure.value)
        assert source.files("loud") == []
