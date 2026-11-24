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
