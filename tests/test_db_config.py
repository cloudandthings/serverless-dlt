"""db_bronze: config schema and static validation (DESIGN-db.md sections 5 and 6.1). Offline: no database."""

from __future__ import annotations

import copy

import pytest

from db_bronze import ConfigError, SourceConfig

# The section 5 example. One deviation: the example sets `isolation_level` on an mssql connection whose
# `transactions` table resolves to connectorx, which validation rejects until spike S3 says which engines
# can honour it. Here the transactions table is pinned to pyarrow.
EXAMPLE = {
    "pipeline_name": "core_banking",
    "dataset_name": "core_banking",
    "bucket_url": "s3://bronze-bucket/bronze",
    "parquet_compression": "zstd",
    "max_time_s": None,
    "connection": {
        "dialect": "mssql",
        "host": "sql01.corp.example",
        "port": 1433,
        "database": "CoreBanking",
        "username": {"secret_id": "ingest/core_banking", "key": "username"},
        "password": {"secret_id": "ingest/core_banking", "key": "password"},
        "auth": {"type": "password"},
        "driver_options": {"Encrypt": "yes"},
        "isolation_level": "READ COMMITTED",
        "naive_timezone": "Africa/Johannesburg",
    },
    "defaults": {"backend": None, "batch_size": 50000},
    "tables": [
        {
            "name": "transactions",
            "schema": "dbo",
            "table": "Transactions",
            "primary_key": ["TransactionId"],
            "columns": None,
            "where": "BranchCode <> 'TEST'",
            "cursor": {
                "column": "UpdatedAt",
                "type": "timestamp",
                "initial_value": "2020-01-01T00:00:00Z",
                "lag": 300,
                "include_null_cursor": False,
            },
            "chunking": {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 200000},
            "backend": "pyarrow",
        },
        {
            "name": "branches",
            "schema": "dbo",
            "table": "Branches",
            "primary_key": ["BranchCode"],
            "backend": "pyarrow",
        },
    ],
}


def example(**changes) -> dict:
    return copy.deepcopy(EXAMPLE) | changes


def test_the_section_5_example_loads():
    cfg = SourceConfig.from_dict(example())
    conn = cfg.connection
    assert (conn.dialect, conn.host, conn.port, conn.database) == ("mssql", "sql01.corp.example", 1433, "CoreBanking")
    assert conn.password == {"secret_id": "ingest/core_banking", "key": "password"}  # resolved at run time
    assert conn.auth.type == "password"
    assert conn.driver_options == {"Encrypt": "yes"}
    assert conn.isolation_level == "READ COMMITTED"
    assert conn.naive_timezone == "Africa/Johannesburg"
    assert cfg.max_time_s is None
    tx, branches = cfg.tables
    assert tx.where == "BranchCode <> 'TEST'"
    assert tx.cursor.lag == 300 and tx.cursor.include_null_cursor is False
    assert tx.chunking.strategy == "keyset"
    assert tx.chunking.order_by == ("UpdatedAt", "TransactionId")
    assert tx.chunking.page_size == 200000
    assert branches.cursor is None
    assert branches.chunking.strategy == "stream"


CONNECTIONS = {
    "mssql": {"dialect": "mssql", "host": "db.example", "database": "core"},
    "postgres": {"dialect": "postgres", "host": "db.example", "database": "core"},
    "mysql": {"dialect": "mysql", "host": "db.example", "database": "core"},
    "oracle": {"dialect": "oracle", "host": "db.example", "service_name": "CORE"},
    "db2": {"dialect": "db2", "host": "db.example", "database": "core"},
    "sqlite": {"dialect": "sqlite", "path": "/data/core.sqlite"},
    "snowflake": {"dialect": "snowflake", "host": "acct.example", "database": "core"},
}
CONNECTORX_DIALECTS = ["mssql", "postgres", "mysql", "oracle", "sqlite"]


def config(dialect: str = "sqlite", connection: dict | None = None, defaults: dict | None = None, **table) -> dict:
    """A one-table config: an incremental `transactions` table, with `table` overriding its keys."""
    tx = {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
    }
    return {
        "pipeline_name": "core_banking",
        "dataset_name": "core_banking",
        "bucket_url": "s3://bronze/bronze",
        "connection": CONNECTIONS[dialect] | (connection or {}),
        "defaults": defaults or {},
        "tables": [{k: v for k, v in (tx | table).items() if v is not None}],
    }


def rejected(cfg: dict) -> str:
    with pytest.raises(ConfigError) as e:
        SourceConfig.from_dict(cfg)
    return str(e.value)


@pytest.mark.parametrize("dialect", ["db2", "snowflake"])
@pytest.mark.parametrize("where", ["table", "defaults"])
def test_connectorx_on_an_engine_it_does_not_support_is_rejected(dialect, where):
    if where == "table":
        cfg = config(dialect, backend="connectorx")
    else:
        cfg = config(dialect, defaults={"backend": "connectorx"})
    message = rejected(cfg)
    assert "ConnectorX does not support" in message
    assert 'set "backend": "pyarrow"' in message
