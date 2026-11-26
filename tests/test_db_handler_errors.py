"""db_bronze: a broken config or an unimplemented auth fails the invocation before anything lands."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from db_bronze.handler import handler


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.sqlite"
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE "Transactions" ("TransactionId" INTEGER PRIMARY KEY, "UpdatedAt" DATETIME)')
        con.execute("INSERT INTO \"Transactions\" VALUES (1, '2026-11-20 08:00:00')")
    return path


@pytest.fixture
def invoke(source: Path, bucket: Path, tmp_path: Path, pipelines_dir: str, monkeypatch):
    """Invoke the handler with config "core" from CONFIG_URI, built from the given overrides."""
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    configs = tmp_path / "configs"
    configs.mkdir()
    monkeypatch.setenv("CONFIG_URI", configs.as_uri())

    def run(connection: dict | None = None, **table) -> dict:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(source)} | (connection or {}),
            "tables": [
                {
                    "name": "transactions",
                    "table": "Transactions",
                    "primary_key": ["TransactionId"],
                    "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00"},
                    "backend": "pyarrow",
                }
                | table
            ],
        }
        (configs / "core.json").write_text(json.dumps(cfg))
        return handler({"source_id": "core"}, None)

    return run


def nothing_landed(bucket: Path) -> bool:
    return not any(p.is_file() for p in bucket.rglob("*"))


def error_type(exc: BaseException) -> str:
    """What Lambda reports as `errorType`: the class name of the exception the handler raised."""
    return type(exc).__name__


def test_the_fixture_config_lands(invoke, bucket):
    assert invoke()["rows"] == {"transactions": 1}
    assert not nothing_landed(bucket)


@pytest.mark.parametrize(
    "overrides",
    [
        {"chunking": {"strategy": "keyset", "page_size": 10}},
        {"cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01", "lag": 60},
         "primary_key": []},
        {"name": "Transactions"},
    ],
    ids=["keyset-without-order_by", "lag-without-primary_key", "bad-name"],
)
def test_an_invalid_config_fails_with_config_error_and_nothing_lands(invoke, bucket, overrides):
    with pytest.raises(Exception) as failure:
        invoke(**overrides)
    assert error_type(failure.value) == "ConfigError"
    assert nothing_landed(bucket)


def test_a_config_that_is_not_json_fails_with_config_error(invoke, tmp_path, bucket):
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "core.json").write_text("{not json")
    with pytest.raises(Exception) as failure:
        handler({"source_id": "core"}, None)
    assert error_type(failure.value) == "ConfigError"


def test_kerberos_fails_at_run_start_with_not_implemented_and_nothing_lands(invoke, bucket):
    kerberos = {"type": "kerberos", "principal": "svc_ingest@CORP.EXAMPLE", "keytab": {"secret_id": "ingest/keytab"}}
    with pytest.raises(Exception) as failure:
        invoke(connection={"auth": kerberos})
    assert error_type(failure.value) == "NotImplementedError"
    assert "docs/db/kerberos.md" in str(failure.value)
    assert nothing_landed(bucket)


def test_a_snapshot_table_in_an_incremental_run_fails_with_config_error(invoke, bucket):
    with pytest.raises(Exception) as failure:
        invoke(cursor=None)
    assert error_type(failure.value) == "ConfigError"
    assert 'run it with "mode": "full"' in str(failure.value)
    assert nothing_landed(bucket)
