"""db_bronze: opt-in partitioned ConnectorX reads (DESIGN-db.md sections 3.1, 5.2, 6.1), against SQLite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_db_incremental import SourceDB, files, ids, landed

from db_bronze import ConfigError
from db_bronze.handler import handler

TRANSACTIONS = {
    "name": "transactions",
    "table": "Transactions",
    "primary_key": ["TransactionId"],
    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
    "backend": "connectorx",
}
PARTITION = {"column": "TransactionId", "num": 3}
ROWS = [(i, f"2026-03-{i:02d} 08:00:00", "JHB", i * 10) for i in range(1, 21)]


@pytest.fixture
def db(tmp_path: Path) -> SourceDB:
    source = SourceDB(tmp_path / "source.sqlite")
    source.insert(*ROWS)
    return source


@pytest.fixture
def invoke(db: SourceDB, bucket: Path, tmp_path: Path, pipelines_dir: str, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())

    def run(table: dict | None = None, **event) -> dict:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(db.path)},
            "tables": [table or TRANSACTIONS],
        }
        (config_dir / "core.json").write_text(json.dumps(cfg))
        return handler({"source_id": "core", **event}, None)

    return run


def test_partitioned_full_lands_the_same_rows_as_unpartitioned(bucket, invoke):
    invoke(mode="full")
    expected = ids(landed(bucket))
    assert expected == list(range(1, 21))
    for f in files(bucket):
        f.unlink()
    result = invoke(TRANSACTIONS | {"partition": PARTITION}, mode="full")
    assert result["rows"] == {"transactions": 20}
    assert ids(landed(bucket)) == expected


def test_partitioned_backfill_lands_the_same_rows_as_unpartitioned(bucket, invoke):
    window = {"mode": "backfill", "window_start": "2026-03-05T00:00:00Z", "window_end": "2026-03-15T00:00:00Z"}
    invoke(**window)
    expected = ids(landed(bucket))
    assert expected == list(range(5, 15))
    for f in files(bucket):
        f.unlink()
    result = invoke(TRANSACTIONS | {"partition": PARTITION}, **window)
    assert result["rows"] == {"transactions": 10}
    assert ids(landed(bucket)) == expected


def test_partition_in_an_incremental_run_fails_before_extraction(bucket, invoke):
    with pytest.raises(ConfigError, match=r"table transactions: partition reads.*cursor order.*incremental"):
        invoke(TRANSACTIONS | {"partition": PARTITION})
    assert files(bucket) == []


def test_partition_on_pyarrow_is_rejected_when_the_config_loads(invoke):
    with pytest.raises(ConfigError, match="partition is a ConnectorX read"):
        invoke(TRANSACTIONS | {"backend": "pyarrow", "partition": PARTITION}, mode="full")


def test_partition_with_keyset_is_rejected_when_the_config_loads(invoke):
    keyset = {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 10}
    with pytest.raises(ConfigError, match="partition reads cannot be chunked"):
        invoke(TRANSACTIONS | {"partition": PARTITION, "chunking": keyset}, mode="full")


def test_non_integer_partition_column_fails_the_preflight(bucket, invoke):
    from db_bronze.errors import PreflightError

    with pytest.raises(PreflightError, match=r"table transactions: partition column 'Branch Code' is not an integer"):
        invoke(TRANSACTIONS | {"partition": {"column": "Branch Code", "num": 2}}, mode="full")
    assert files(bucket) == []


def test_partitioned_read_hands_partition_on_to_connectorx(invoke, monkeypatch):
    import connectorx as cx

    calls = []
    real = cx.read_sql

    def spy(*args, **kwargs):
        calls.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(cx, "read_sql", spy)
    invoke(TRANSACTIONS | {"partition": PARTITION}, mode="full")
    assert [(c.get("partition_on"), c.get("partition_num")) for c in calls] == [('"TransactionId"', 3)]
    calls.clear()
    invoke(mode="full")
    assert [c.get("partition_on") for c in calls] == [None]
