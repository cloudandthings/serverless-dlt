"""JSON Schemas for both config documents (schemas/*.config.schema.json) agree with config.py.

config.py is authoritative. The schemas mirror it for editors and CI; these tests fail when the two
drift: a dataclass field or enum the schema lacks, or a config one accepts and the other rejects.
The few rules JSON Schema cannot express are listed in PYTHON_ONLY and asserted as such.
"""

from __future__ import annotations

import copy
import dataclasses
import itertools
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

import api_bronze.config as api_config
import db_bronze.config as db_config
from db_bronze import dialects

ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = ROOT / "schemas"
EXAMPLES = ROOT / "examples"


def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS / name).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


API = _validator("api_bronze.config.schema.json")
DB = _validator("db_bronze.config.schema.json")


def schema_ok(validator: Draft202012Validator, data: dict) -> bool:
    return validator.is_valid(data)


def python_ok(module: Any, data: dict) -> bool:
    try:
        module.SourceConfig.from_dict(copy.deepcopy(data))
    except module.ConfigError:
        return False
    return True


def errors(validator: Draft202012Validator, data: dict) -> list[str]:
    return [f"{list(e.absolute_path)}: {e.message}" for e in validator.iter_errors(data)]


# --- the schemas cover every field and enum config.py knows ---------------------------------------


def _props(validator: Draft202012Validator, ref: str) -> set[str]:
    node = validator.schema if ref == "#" else validator.schema["$defs"][ref.removeprefix("#/$defs/")]
    return set(node["properties"])


@pytest.mark.parametrize(
    ("validator", "ref", "cls"),
    [
        (API, "#", api_config.SourceConfig),
        (API, "#/$defs/client", api_config.ClientConfig),
        (API, "#/$defs/resource", api_config.ResourceConfig),
        (API, "#/$defs/incremental", api_config.IncrementalConfig),
        (DB, "#", db_config.SourceConfig),
        (DB, "#/$defs/connection", db_config.ConnectionConfig),
        (DB, "#/$defs/auth", db_config.AuthConfig),
        (DB, "#/$defs/defaults", db_config.Defaults),
        (DB, "#/$defs/table", db_config.TableConfig),
        (DB, "#/$defs/cursor", db_config.CursorConfig),
        (DB, "#/$defs/chunking", db_config.ChunkingConfig),
        (DB, "#/$defs/partition", db_config.PartitionConfig),
    ],
    ids=lambda x: x if isinstance(x, str) else getattr(x, "__qualname__", None),
)
def test_schema_properties_match_dataclass_fields(validator, ref, cls):
    assert _props(validator, ref) == {f.name for f in dataclasses.fields(cls)}


def _enum(validator: Draft202012Validator, *path: str) -> tuple:
    node: Any = validator.schema
    for key in path:
        node = node[key]
    return tuple(v for v in node["enum"] if v is not None)


def test_schema_enums_match_code():
    s = API.schema
    assert _enum(API, "properties", "parquet_compression") == api_config.PARQUET_CODECS
    assert _enum(API, "$defs", "incremental", "properties", "cursor_type") == api_config.CURSOR_TYPES
    assert _enum(API, "$defs", "incremental", "properties", "param_format") == api_config.PARAM_FORMATS
    assert s["$defs"]["name"]["pattern"] == api_config._NAME.pattern

    d = DB.schema["$defs"]
    assert _enum(DB, "properties", "parquet_compression") == db_config.PARQUET_CODECS
    assert _enum(DB, "$defs", "defaults", "properties", "backend") == db_config.BACKENDS
    assert _enum(DB, "$defs", "table", "properties", "backend") == db_config.BACKENDS
    assert _enum(DB, "$defs", "cursor", "properties", "type") == db_config.CURSOR_TYPES
    assert _enum(DB, "$defs", "chunking", "properties", "strategy") == db_config.CHUNKING_STRATEGIES
    assert _enum(DB, "$defs", "auth", "properties", "type") == db_config.AUTH_TYPES
    assert _enum(DB, "$defs", "isolationLevel") == dialects.ISOLATION_LEVELS
    assert d["name"]["pattern"] == db_config._NAME.pattern
    assert d["connection"]["properties"]["dialect"]["pattern"] == db_config._DIALECT.pattern
    not_cx = d["if.dialectNotConnectorx"]["properties"]["connection"]["properties"]["dialect"]["not"]["enum"]
    assert tuple(not_cx) == dialects.CONNECTORX_DIALECTS


# --- shipped examples --------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["vendor_orders.json", "vendor_countries.json"])
def test_api_examples_valid(name):
    data = json.loads((EXAMPLES / name).read_text())
    assert errors(API, data) == []


def test_db_example_valid():
    data = json.loads((EXAMPLES / "core_banking.json").read_text())
    assert errors(DB, data) == []


# --- api_bronze: schema and config.py agree ----------------------------------------------------

API_BASE = {
    "pipeline_name": "vendor_orders",
    "dataset_name": "vendor",
    "bucket_url": "s3://b/bronze",
    "client": {
        "base_url": "https://api.example/",
        "paginator": {"type": "json_link", "next_url_path": "next"},
        "auth": {"type": "bearer", "token": {"secret_id": "s", "key": "token", "format": "Bearer {}"}},
        "headers": {"Accept": "application/json", "X-Key": {"env": "KEY"}},
    },
    "resources": [
        {
            "name": "orders_raw",
            "path": "orders",
            "records_path": "$.data",
            "id_path": "id",
            "incremental": {
                "cursor_path": "updatedAt",
                "initial_value": "2020-01-01T00:00:00Z",
                "start_param": "since",
                "cursor_type": "epoch_ms",
                "param_format": "epoch_s",
                "sorted_ascending": True,
            },
        },
        {"name": "countries_raw", "path": "countries", "records_path": "$", "paginator": {"type": "single_page"}},
    ],
}


def _set(data: dict, path: str, value: Any) -> dict:
    """A copy of `data` with `path` (dotted; list indexes as digits) set to `value`, or removed for DEL."""
    out = copy.deepcopy(data)
    *parents, last = path.split(".")
    node: Any = out
    for key in parents:
        node = node[int(key)] if isinstance(node, list) else node[key]
    if value is DEL:
        del node[last]
    else:
        node[int(last) if isinstance(node, list) else last] = value
    return out


DEL = object()
# config.py rejects it but JSON Schema cannot express the rule (uniqueness by property, comparing two
# values, IANA zone lookup, calendar ranges in a timestamp): the schema accepts.
PYTHON_ONLY = "python_only"

API_CASES = [
    ("pipeline_name", "Orders", False),
    ("dataset_name", "1vendor", False),
    ("typo_key", 1, False),
    ("parquet_compression", "lz4", False),
    ("parquet_compression", "none", True),
    ("quarantine_url", None, True),
    ("resources", [], False),
    ("client", DEL, False),
    ("client.paginator", {"type": "auto"}, False),
    ("client.paginator", {"next_url_path": "next"}, False),
    ("client.paginator", {"type": "offset", "limit": 100, "offset_param": "o"}, True),
    ("client.paginator", {"type": "page_number", "page_param": "p", "total_path": None}, True),
    ("client.paginator", {"type": "header_cursor", "cursor_key": "X-Next"}, True),
    ("client.auth", None, True),
    ("client.auth", {"type": "http_basic", "username": "u", "password": {"env": "PW"}}, True),
    ("client.auth", {"type": "api_key", "name": "X-Key", "location": "query", "api_key": {"secret_id": "s"}}, True),
    ("client.headers", {"A": "b"}, True),
    ("resources.0.name", "Orders", False),
    ("resources.0.records_path", "data.items", True),
    ("resources.0.records_path", "", True),
    ("resources.0.records_path", "data[0]", False),
    ("resources.0.records_path", "data.*", False),
    ("resources.0.records_path", "a..b", False),
    ("resources.0.incremental", None, True),
    ("resources.0.incremental.cursor_type", "unix", False),
    ("resources.0.incremental.param_format", "iso8601_z", True),
    ("resources.0.incremental.param_format", "rfc1123", False),
    ("resources.0.incremental.initial_value", "2020-01-01", True),
    ("resources.0.incremental.initial_value", "2020-01-01T00:00:00+02:00", True),
    ("resources.0.incremental.initial_value", "2020-01-01T00:00:00.123456", True),
    ("resources.0.incremental.initial_value", "yesterday", False),
    ("resources.0.incremental.initial_value", "2020-13-45", PYTHON_ONLY),
    ("resources.0.incremental.start_param", DEL, False),
    ("resources.0.incremental.extra", 1, False),
]


@pytest.mark.parametrize(("path", "value", "valid"), API_CASES, ids=lambda x: repr(x)[:40])
def test_api_schema_agrees_with_config(path, value, valid):
    data = _set(API_BASE, path, value)
    assert python_ok(api_config, data) is (valid is True)
    assert schema_ok(API, data) is (valid is not False), errors(API, data)


def test_api_duplicate_resource_names_python_only():
    data = _set(API_BASE, "resources.1.name", "orders_raw")
    assert not python_ok(api_config, data)
    assert schema_ok(API, data)  # uniqueness by property is beyond JSON Schema


# --- db_bronze: schema and config.py agree ----------------------------------------------------

DB_BASE = json.loads((EXAMPLES / "core_banking.json").read_text())

DB_CASES = [
    ("typo_key", 1, False),
    ("tables", [], False),
    ("max_time_s", 0, False),
    ("defaults", {"backend": "sqlalchemy"}, False),
    ("defaults", {"batch_size": 0}, False),
    ("defaults", {}, True),
    ("connection.port", 70000, False),
    ("connection.port", {"env": "PORT"}, True),
    ("connection.host", "", False),
    ("connection.host", {"secret_id": "x", "format": "{}"}, False),
    ("connection.database", DEL, False),
    ("connection.service_name", "ORCL", False),
    ("connection.path", "/tmp/x.db", False),
    ("connection.dialect", "Postgres", False),
    ("connection.dialect", "postgresql+psycopg", True),
    ("connection.isolation_level", "CHAOS", False),
    ("connection.isolation_level", None, True),
    ("connection.driver_options", {"Encrypt": "yes", "Timeout": 30, "Key": {"env": "K"}}, True),
    ("connection.driver_options", {"Encrypt": True}, False),
    ("connection.auth", {"type": "password", "principal": "x"}, False),
    ("connection.auth", {"principal": None}, True),
    ("connection.auth", {"type": "kerberos", "principal": "p", "keytab": {"secret_id": "k"}, "krb5_conf": "/e"}, True),
    ("connection.auth", {"type": "kerberos", "keytab": "plain"}, False),
    ("connection.naive_timezone", None, False),
    ("connection.naive_timezone", "Mars/Olympus", PYTHON_ONLY),
    ("tables.0.name", "Transactions", False),
    ("tables.0.table", DEL, False),
    ("tables.0.table", "", False),
    ("tables.0.query", "SELECT 1", False),
    ("tables.1.columns", ["BranchCode", "_extracted_at"], False),
    ("tables.1.columns", [""], False),
    ("tables.1.batch_size", 0, False),
    ("tables.1.backend", "sqlalchemy", False),
    ("tables.1.cursor", {"column": "Id", "type": "integer", "initial_value": 0, "lag": 1.5}, False),
    ("tables.1.cursor", {"column": "Id", "type": "integer", "initial_value": "0"}, False),
    ("tables.1.cursor", {"column": "Id", "type": "integer", "initial_value": 0, "lag": 10}, True),
    ("tables.1.cursor", {"column": "At", "type": "timestamp", "initial_value": 0}, False),
    ("tables.1.cursor", {"column": "At", "type": "timestamp", "initial_value": "2020-01-01", "lag": -1}, False),
    ("tables.1.cursor", {"column": "A", "type": "integer", "initial_value": 0, "include_null_cursor": 0}, False),
    ("tables.1.cursor", {"column": "_extracted_at", "type": "integer", "initial_value": 0}, False),
    ("tables.0.primary_key", None, False),  # lag and keyset need it
    ("tables.0.cursor.lag", 0, True),
    ("tables.0.chunking", {"strategy": "stream", "page_size": 5}, False),
    ("tables.0.chunking", {"strategy": "stream", "order_by": []}, True),
    ("tables.0.chunking", {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"]}, False),
    ("tables.0.chunking", {"strategy": "keyset", "page_size": 5}, False),
    ("tables.0.chunking", {"strategy": "hash"}, False),
    ("tables.0.chunking.order_by", ["TransactionId", "UpdatedAt"], PYTHON_ONLY),  # must start with the cursor
    ("tables.0.columns", ["UpdatedAt"], PYTHON_ONLY),  # order_by outside columns
    ("tables.0.partition", {"column": "TransactionId", "num": 4}, False),  # keyset
    ("tables.0.partition", {"column": "TransactionId", "num": 0}, False),
    ("tables.1.partition", {"column": "BranchCode", "num": 4}, False),  # pyarrow; and mssql + isolation_level
    ("tables.1", {"name": "q", "query": "SELECT 1", "schema": "dbo"}, False),
    ("tables.1", {"name": "q", "query": "SELECT 1 AS x"}, True),
    ("tables.1.name", "transactions", PYTHON_ONLY),  # duplicate
]


@pytest.mark.parametrize(("path", "value", "valid"), DB_CASES, ids=lambda x: repr(x)[:40])
def test_db_schema_agrees_with_config(path, value, valid):
    data = _set(DB_BASE, path, value)
    assert python_ok(db_config, data) is (valid is True)
    assert schema_ok(DB, data) is (valid is not False), errors(DB, data)


def test_db_null_chunking_rejected_by_schema():
    assert not schema_ok(DB, _set(DB_BASE, "tables.0.chunking", None))


@pytest.mark.xfail(strict=True, raises=AttributeError, reason='config.py: "chunking": null is not a ConfigError')
def test_db_null_chunking_is_a_config_error():
    assert not python_ok(db_config, _set(DB_BASE, "tables.0.chunking", None))


# Every combination of the knobs that decide a table's backend and what that backend allows
# (dialect, defaults/table backend, isolation level, auth, partition, max_time_s, chunking):
# schema and config.py must give the same verdict on each.

_ADDRESS = {
    "sqlite": {"path": "/tmp/x.db"},
    "oracle": {"host": "h", "service_name": "s"},
    "snowflake": {},  # a dialect db_bronze has no entry for
}


def _db_matrix():
    names = [*dialects.DIALECTS, "snowflake"]
    levels = [None, *dialects.ISOLATION_LEVELS]
    backends = [None, "connectorx", "pyarrow"]
    for dialect, default_be, table_be, level, kerberos, partition, max_time, keyset in itertools.product(
        names, backends, backends, levels, (False, True), (False, True), (False, True), (False, True)
    ):
        table = {"name": "t", "table": "T", "primary_key": ["id"], "backend": table_be}
        table["cursor"] = {"column": "id", "type": "integer", "initial_value": 0}
        if keyset:
            table["chunking"] = {"strategy": "keyset", "order_by": ["id"], "page_size": 10}
        if partition:
            table["partition"] = {"column": "id", "num": 2}
        connection = {"dialect": dialect, **_ADDRESS.get(dialect, {"host": "h", "database": "d"})}
        connection["isolation_level"] = level
        if kerberos:
            connection["auth"] = {"type": "kerberos"}
        yield {
            "pipeline_name": "p",
            "dataset_name": "d",
            "bucket_url": "s3://b",
            "max_time_s": 60 if max_time else None,
            "defaults": {"backend": default_be},
            "connection": connection,
            "tables": [table],
        }


def test_db_schema_agrees_with_config_on_backend_rules():
    disagreements = []
    for data in _db_matrix():
        py, sc = python_ok(db_config, data), schema_ok(DB, data)
        if py != sc:
            disagreements.append((py, sc, data))
    assert not disagreements, f"{len(disagreements)} disagree; first: {disagreements[0]}"
