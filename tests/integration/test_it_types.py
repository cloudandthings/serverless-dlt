"""db_bronze integration: the landed Arrow type of every column type, per engine and backend.

The tables below are the record DESIGN-db.md section 10.1 quotes. A change in
any landed type changes the bronze contract, so it fails here first.

Each matrix column is (DDL type, literal value, ConnectorX reads it). Columns
ConnectorX cannot read are deny-listed or high-scale decimals: the ConnectorX
run leaves them out with `columns`, the pyarrow run lands them.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, time
from decimal import Decimal

import connectorx as cx
import pyarrow.parquet as pq
import pytest

from .conftest import connectorx_skip_reason

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
    # ConnectorX does not read Db2 at all (the third field is False throughout): pyarrow only.
    "db2": {
        "c_smallint": ("smallint", "1", False),
        "c_integer": ("integer", "2", False),
        "c_bigint": ("bigint", "3", False),
        "c_decimal_18_4": ("decimal(18,4)", "12345678901234.5678", False),
        "c_decimal_31_12": ("decimal(31,12)", "1234567890123456789.123456789012", False),
        "c_decimal_31_0": ("decimal(31,0)", "1234567890123456789012345678901", False),
        "c_decfloat16": ("decfloat(16)", "1.234567890123456", False),
        # a numeric literal holds 31 digits at most
        "c_decfloat34": ("decfloat(34)", "DECFLOAT('1.234567890123456789012345678901234', 34)", False),
        "c_real": ("real", "1.5", False),
        "c_double": ("double", "2.5", False),
        "c_boolean": ("boolean", "true", False),
        "c_char": ("char(5)", "'c'", False),
        "c_varchar": ("varchar(20)", "'v'", False),
        "c_clob": ("clob", "'cl'", False),
        "c_graphic": ("graphic(5)", "'g'", False),
        "c_vargraphic": ("vargraphic(20)", "'\u00fcn\u00ef'", False),
        "c_dbclob": ("dbclob", "'db'", False),
        "c_char_bit": ("char(4) for bit data", "x'01020304'", False),
        "c_varchar_bit": ("varchar(8) for bit data", "x'0102'", False),
        "c_binary": ("binary(4)", "bx'01020304'", False),
        "c_varbinary": ("varbinary(8)", "bx'0102'", False),
        "c_blob": ("blob", "blob(x'0102')", False),
        "c_date": ("date", "'2026-11-20'", False),
        "c_time": ("time", "'08:00:01'", False),
        "c_timestamp": ("timestamp", "'2026-11-20 08:00:00.123456'", False),
        "c_timestamp_0": ("timestamp(0)", "'2026-11-20 08:00:01'", False),
        "c_timestamp_12": ("timestamp(12)", "'2026-11-20 08:00:00.123456789012'", False),
        "c_xml": ("xml", "xmlparse(document '<a/>')", False),
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
    ("db2", "pyarrow"): {
        "c_smallint": "int64", "c_integer": "int64", "c_bigint": "int64",
        "c_decimal_18_4": "decimal128(18, 4)", "c_decimal_31_12": "decimal128(31, 12)",
        "c_decimal_31_0": "decimal128(31, 0)", "c_decfloat16": "string", "c_decfloat34": "string",
        "c_real": "double", "c_double": "double", "c_boolean": "bool",
        "c_char": "string", "c_varchar": "string", "c_clob": "string", "c_graphic": "string",
        "c_vargraphic": "string", "c_dbclob": "string", "c_char_bit": "binary", "c_varchar_bit": "binary",
        "c_binary": "binary", "c_varbinary": "binary", "c_blob": "binary",
        "c_date": "date32[day]", "c_time": "time64[us]", "c_timestamp": "timestamp[us]",
        "c_timestamp_0": "timestamp[us]", "c_timestamp_12": "timestamp[us]", "c_xml": "string",
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
    "mssql": {
        "c_decimal_28_10": Decimal("123456789012345678.0123456789"),
        "c_datetime2": datetime(2026, 11, 20, 8, 0, 0, 123456),  # 100 ns truncated to the microsecond
        "c_datetime2_6": datetime(2026, 11, 20, 8, 0, 0, 123456),
        "c_time": time(8, 0, 0, 123456),
        "c_nvarchar": "\u00fcn\u00ef",
        "c_uniqueidentifier": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    },
    "oracle": {
        "c_number_18": 123456789012345678,
        "c_number_28_10": Decimal("123456789012345678.0123456789"),
        "c_date": datetime(2026, 11, 20, 8, 0, 1),
        "c_timestamp": datetime(2026, 11, 20, 8, 0, 0, 123456),
        "c_char": "c    ",
    },
    "db2": {
        "c_decimal_31_12": Decimal("1234567890123456789.123456789012"),  # through a float, it would not be
        "c_decimal_31_0": Decimal("1234567890123456789012345678901"),
        "c_decfloat34": "1.234567890123456789012345678901234",
        "c_double": 2.5,
        "c_char": "c    ",
        "c_vargraphic": "\u00fcn\u00ef",
        "c_char_bit": b"\x01\x02\x03\x04",
        "c_varchar_bit": b"\x01\x02",
        "c_binary": b"\x01\x02\x03\x04",
        "c_timestamp_12": datetime(2026, 11, 20, 8, 0, 0, 123456),  # picoseconds truncated to the microsecond
        "c_xml": '\ufeff<?xml version="1.0" encoding="UTF-16" ?><a/>',  # as the driver serializes it
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
    "mssql": {
        "c_decimal_38_12": Decimal("12345678901234567890123456.123456789012"),
        "c_money": Decimal("922337203685477.5807"),
        "c_datetime": datetime(2026, 11, 20, 8, 0, 0, 123000),  # ConnectorX: .123333, the 1/300 s DATETIME holds
        "c_datetimeoffset": datetime(2026, 11, 20, 6, 0, 0, 123456, tzinfo=UTC),  # the instant, in UTC
        "c_xml": "<a/>",
        "c_hierarchyid": b"X",  # SQL Server's serialization of /1/, as CAST(... AS varbinary) gives it
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
    "db2": {},  # pyarrow only: EXACT holds them
}

# Columns no backend lands losslessly: preflight fails them on pyarrow as well. pyodbc reads a
# sql_variant only as the value's bytes, without its type.
UNREADABLE = {"mssql": {"c_sql_variant"}}


# Types newer than the oldest server a matrix runs on, by the server version that has them.
SINCE = {"oracle": {"c_boolean": (23,), "c_json": (23,)}}


def server_version(source) -> tuple[int, ...]:
    with source.sa.connect():
        return source.sa.dialect.server_version_info


def seed_matrix(source) -> dict[str, tuple[str, str, bool]]:
    version = server_version(source)
    since = SINCE.get(source.dialect, {})
    matrix = {name: column for name, column in MATRIX[source.dialect].items() if version >= since.get(name, ())}
    columns = ", ".join(f"{name} {ddl}" for name, (ddl, _, _) in matrix.items())
    values = ", ".join(value for _, value, _ in matrix.values())
    source.execute(
        *SETUP.get(source.dialect, []),
        f"CREATE TABLE type_matrix (id integer NOT NULL PRIMARY KEY, {columns})",  # Db2: a key is NOT NULL
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
    assert landed == {name: t for name, t in LANDED[(source.dialect, backend)].items() if name in matrix}
    [row, empty] = sorted(source.landed("type_matrix"), key=lambda r: r["id"])
    exact = EXACT[source.dialect] | (EXACT_PYARROW[source.dialect] if backend == "pyarrow" else {})
    exact = {name: value for name, value in exact.items() if name in matrix}
    assert {name: row[name] for name in exact} == exact
    assert {name for name, value in empty.items() if value is not None} == {"id", "_extracted_at"}


# Columns ConnectorX reads until a value does not fit: the read then fails loudly, so preflight lets them through.
FAILS_LOUDLY = {"oracle": {"c_number_38": "PosOverflow"}}  # NUMBER(p, 0) is read as int64


def connectorx_here(source) -> None:
    if reason := connectorx_skip_reason(source.dialect):
        pytest.skip(reason)


def test_the_preflight_fails_exactly_the_columns_a_backend_cannot_land(source, run, backend):
    """Every deny-listed type and high-scale decimal on ConnectorX; on pyarrow only what no backend reads."""
    matrix = seed_matrix(source)
    table = {"name": "type_matrix", "table": "type_matrix", "backend": backend}
    report = run({"source_id": "types", "mode": "validate"}, table)
    [checked] = report["tables"]
    failed = {finding.split(":")[0].removeprefix("column ") for finding in checked["findings"]}
    unreadable = UNREADABLE.get(source.dialect, set())
    cannot = denied(matrix) - set(FAILS_LOUDLY.get(source.dialect, {})) if backend == "connectorx" else set()
    assert failed == cannot | unreadable
    assert source.files("type_matrix") == []


def denied(matrix: dict[str, tuple[str, str, bool]]) -> set[str]:
    return {name for name, (_, _, cx_reads) in matrix.items() if not cx_reads}


@pytest.mark.connectorx
def test_every_column_connectorx_cannot_read_fails_the_preflight_naming_each_one(source, run):
    matrix = seed_matrix(source)
    table = {"name": "type_matrix", "table": "type_matrix", "backend": "connectorx"}
    with pytest.raises(Exception) as failure:
        run({"source_id": "types", "mode": "full"}, table)
    assert type(failure.value).__name__ == "PreflightError"
    flagged = set(re.findall(r"column (\w+):", str(failure.value)))
    assert flagged == denied(matrix) - set(FAILS_LOUDLY.get(source.dialect, {}))
    assert source.files("type_matrix") == []


def test_every_denied_column_really_fails_or_loses_data_on_connectorx(source):
    """The deny list is not broader than it needs to be: ConnectorX raises on each column, or reads it wrong."""
    connectorx_here(source)
    matrix = seed_matrix(source)
    url = source.connectorx_url()
    wrong = {}
    for name in sorted(denied(matrix)):
        sql = f"SELECT {name} FROM type_matrix WHERE id = 1"
        try:
            [read] = cx.read_sql(url, sql, return_type="arrow_stream", batch_size=10).read_all().column(0).to_pylist()
        except BaseException as e:  # PanicException derives from BaseException
            if not (isinstance(e, RuntimeError) or type(e).__name__ == "PanicException"):
                raise
            continue
        with source.sa.connect() as conn:  # only now: some denied types (sql_variant) pyodbc cannot read either
            exact = conn.exec_driver_sql(sql).scalar_one()
        if read == exact:
            wrong[name] = read
    assert wrong == {}, "ConnectorX reads these exactly, so the preflight should not deny them"


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
