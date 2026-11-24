"""db_bronze integration: the landed Arrow type of every column type, per engine and backend.

The tables below are the record DESIGN-db.md section 10.1 quotes. A change in
any landed type changes the bronze contract, so it fails here first.

Each matrix column is (DDL type, literal value, ConnectorX reads it). Columns
ConnectorX cannot read are deny-listed or high-scale decimals: the ConnectorX
run leaves them out with `columns`, the pyarrow run lands them.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

import connectorx as cx
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
        "c_bit": ("bit(3)", "B'101'", True),
        "c_int_array": ("integer[]", "'{1,2}'", True),
        "c_text_array": ("text[]", "'{a,b}'", True),
        "c_numeric_array": ("numeric(10,2)[]", "'{1.25}'", True),
        "c_interval": ("interval", "'1 day 02:00:00'", False),
        "c_money": ("money", "'1.50'", False),
        "c_timetz": ("timetz", "'08:00:00+02'", False),
        "c_macaddr": ("macaddr", "'08:00:2b:01:02:03'", False),
        "c_macaddr8": ("macaddr8", "'08:00:2b:01:02:03:04:05'", False),
        "c_xml": ("xml", "'<a/>'", False),
        "c_cidr": ("cidr", "'10.0.0.0/8'", False),
        "c_varbit": ("varbit", "B'101'", False),
        "c_point": ("point", "'(1,2)'", False),
        "c_line": ("line", "'{1,2,3}'", False),
        "c_lseg": ("lseg", "'[(1,2),(3,4)]'", False),
        "c_box": ("box", "'((1,2),(3,4))'", False),
        "c_path": ("path", "'((1,2),(3,4))'", False),
        "c_polygon": ("polygon", "'((1,2),(3,4),(5,6))'", False),
        "c_circle": ("circle", "'<(1,2),3>'", False),
        "c_tsvector": ("tsvector", "'a b'", False),
        "c_tsquery": ("tsquery", "'a & b'", False),
        "c_quoted_char": ('"char"', "'a'", False),
        "c_pg_lsn": ("pg_lsn", "'0/16B3748'", False),
        "c_composite": ("it_pair", "ROW(1, 'a')", False),
        "c_numeric_unconstrained_array": ("numeric[]", "'{1.123456789012345678}'", False),
        "c_char_array": ("char(2)[]", "'{ab}'", False),
        "c_uuid_array": ("uuid[]", "'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'", False),
        "c_timestamp_array": ("timestamp[]", "'{2026-11-20 08:00:00}'", False),
        "c_jsonb_array": ("jsonb[]", """ARRAY['{"a": 1}'::jsonb]""", False),
    },
    "mysql": {
        "c_tinyint": ("tinyint", "1", True),
        "c_smallint": ("smallint", "2", True),
        "c_mediumint": ("mediumint", "3", True),
        "c_int": ("int", "4", True),
        "c_bigint": ("bigint", "5", True),
        "c_bigint_unsigned": ("bigint unsigned", "18446744073709551615", True),
        "c_tinyint_unsigned": ("tinyint unsigned", "255", True),
        "c_int_unsigned": ("int unsigned", "4294967295", True),
        "c_decimal_18_4": ("decimal(18,4)", "12345678901234.5678", True),
        "c_decimal_28_10": ("decimal(28,10)", "123456789012345678.0123456789", True),
        "c_decimal_38_12": ("decimal(38,12)", "12345678901234567890123456.123456789012", False),
        "c_decimal_65_30": ("decimal(65,30)", "1.123456789012345678901234567890", False),
        "c_decimal_65_0": ("decimal(65,0)", "12345678901234567890123456789012345", False),
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
        "c_point": ("point", "ST_GeomFromText('POINT(1 2)')", False),
        "c_linestring": ("linestring", "ST_GeomFromText('LINESTRING(1 2, 3 4)')", False),
        "c_polygon": ("polygon", "ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))')", False),
        "c_multipoint": ("multipoint", "ST_GeomFromText('MULTIPOINT(1 2)')", False),
        "c_multilinestring": ("multilinestring", "ST_GeomFromText('MULTILINESTRING((1 2, 3 4))')", False),
        "c_multipolygon": ("multipolygon", "ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))')", False),
        "c_geometrycollection": ("geometrycollection", "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))')", False),
    },
}

# Statements a matrix needs before its table: the types its columns use.
SETUP = {"postgres": ["CREATE TYPE it_pair AS (a integer, b text)"]}

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
        "c_inet": "string", "c_bit": "string",
        "c_int_array": "list<element: int64>", "c_text_array": "list<element: string>",
        "c_numeric_array": "list<element: decimal128(10, 2)>",
        "c_interval": "string", "c_money": "string", "c_timetz": "string", "c_macaddr": "string",
        "c_macaddr8": "string", "c_xml": "string", "c_cidr": "string", "c_varbit": "string",
        "c_point": "string", "c_line": "string", "c_lseg": "string", "c_box": "string", "c_path": "string",
        "c_polygon": "string", "c_circle": "string", "c_tsvector": "string", "c_tsquery": "string",
        "c_quoted_char": "string", "c_pg_lsn": "string", "c_composite": "string",
        "c_numeric_unconstrained_array": "list<element: string>", "c_char_array": "list<element: string>",
        "c_uuid_array": "list<element: string>", "c_timestamp_array": "list<element: timestamp[us]>",
        "c_jsonb_array": "list<element: string>",
    },
    ("postgres", "connectorx"): {
        # ConnectorX's arrow_stream widens every integer to int64 and real to double on Postgres
        "c_smallint": "int64", "c_integer": "int64", "c_bigint": "int64",
        "c_numeric_18_4": "decimal128(38, 10)", "c_numeric_28_10": "decimal128(38, 10)",
        "c_real": "double", "c_double": "double", "c_boolean": "bool",
        "c_text": "string", "c_varchar": "string", "c_char": "string",
        "c_date": "date32[day]", "c_timestamp": "timestamp[us]", "c_timestamptz": "timestamp[us, tz=UTC]",
        "c_time": "time64[us]", "c_bytea": "large_binary", "c_uuid": "string", "c_json": "string", "c_jsonb": "string",
        "c_inet": "string", "c_bit": "large_binary",
        "c_int_array": "large_list<element: int32>", "c_text_array": "large_list<element: string>",
        "c_numeric_array": "large_list<element: decimal128(38, 10)>",
    },
    ("mysql", "pyarrow"): {
        "c_tinyint": "int64", "c_smallint": "int64", "c_mediumint": "int64", "c_int": "int64", "c_bigint": "int64",
        "c_bigint_unsigned": "uint64", "c_tinyint_unsigned": "int64", "c_int_unsigned": "int64",
        "c_decimal_18_4": "decimal128(18, 4)", "c_decimal_28_10": "decimal128(28, 10)",
        "c_decimal_38_12": "decimal128(38, 12)", "c_decimal_65_30": "decimal256(65, 30)",
        "c_decimal_65_0": "decimal256(65, 0)",
        "c_float": "double", "c_double": "double", "c_bool": "int64", "c_bit": "int64",
        "c_varchar": "string", "c_char": "string", "c_text": "string", "c_enum": "string", "c_set": "string",
        "c_date": "date32[day]", "c_datetime": "timestamp[us]", "c_datetime6": "timestamp[us]",
        "c_timestamp": "timestamp[us]", "c_time": "time64[us]", "c_year": "int64",
        "c_binary": "binary", "c_varbinary": "binary", "c_blob": "binary", "c_json": "string", "c_geometry": "binary",
        "c_point": "binary", "c_linestring": "binary", "c_polygon": "binary", "c_multipoint": "binary",
        "c_multilinestring": "binary", "c_multipolygon": "binary", "c_geometrycollection": "binary",
    },
    ("mysql", "connectorx"): {
        "c_tinyint": "int8", "c_smallint": "int16", "c_mediumint": "int32", "c_int": "int32", "c_bigint": "int64",
        "c_bigint_unsigned": "uint64", "c_tinyint_unsigned": "uint8", "c_int_unsigned": "uint32",
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
        "c_int_array": [1, 2],
        "c_text_array": ["a", "b"],
        "c_numeric_array": [Decimal("1.25")],
    },
    "mysql": {
        "c_bigint_unsigned": 18446744073709551615,
        "c_int_unsigned": 4294967295,
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
        "c_bit": "101",
        "c_cidr": "10.0.0.0/8",
        "c_varbit": "101",
        "c_point": "(1,2)",
        "c_quoted_char": "a",
        "c_composite": "(1,a)",
        "c_numeric_unconstrained_array": ["1.123456789012345678"],
        "c_uuid_array": ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"],
        "c_timestamp_array": [datetime(2026, 11, 20, 8, 0)],
        "c_jsonb_array": ['{"a":1}'],
    },
    "mysql": {
        "c_decimal_65_0": Decimal("12345678901234567890123456789012345"),
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
        *SETUP.get(source.dialect, []),
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


def denied(matrix: dict[str, tuple[str, str, bool]]) -> set[str]:
    return {name for name, (_, _, cx_reads) in matrix.items() if not cx_reads}


def test_every_column_connectorx_cannot_read_fails_the_preflight_naming_each_one(source, run):
    matrix = seed_matrix(source)
    table = {"name": "type_matrix", "table": "type_matrix", "backend": "connectorx"}
    with pytest.raises(Exception) as failure:
        run({"source_id": "types", "mode": "full"}, table)
    assert type(failure.value).__name__ == "PreflightError"
    assert set(re.findall(r"column (\w+):", str(failure.value))) == denied(matrix)
    assert source.files("type_matrix") == []


def test_every_denied_column_really_fails_or_loses_data_on_connectorx(source):
    """The deny list is not broader than it needs to be: ConnectorX raises on each column, or reads it wrong."""
    matrix = seed_matrix(source)
    url = source.connectorx_url()
    wrong = {}
    for name in sorted(denied(matrix)):
        sql = f"SELECT {name} FROM type_matrix WHERE id = 1"
        with source.sa.connect() as conn:
            exact = conn.exec_driver_sql(sql).scalar_one()
        try:
            [read] = cx.read_sql(url, sql, return_type="arrow_stream", batch_size=10).read_all().column(0).to_pylist()
        except BaseException as e:  # PanicException derives from BaseException
            if not (isinstance(e, RuntimeError) or type(e).__name__ == "PanicException"):
                raise
            continue
        if read == exact:
            wrong[name] = read
    assert wrong == {}, "ConnectorX reads these exactly, so the preflight should not deny them"
