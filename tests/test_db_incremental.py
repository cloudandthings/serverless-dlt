"""db_bronze: handler-level incremental runs against SQLite into a file:// bucket."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from db_bronze import ConfigError
from db_bronze.handler import handler

DDL = """
CREATE TABLE "Transactions" (
    "TransactionId" INTEGER PRIMARY KEY,
    "UpdatedAt" DATETIME NOT NULL,
    "Branch Code" TEXT,
    "amountCents" INTEGER
)
"""


class SourceDB:
    """A SQLite source database the tests write rows into between runs."""

    def __init__(self, path: Path) -> None:
        self.path = path
        with self.connect() as con:
            con.execute(DDL)

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def insert(self, *rows: tuple) -> None:
        with self.connect() as con:
            con.executemany('INSERT INTO "Transactions" VALUES (?, ?, ?, ?)', rows)

    def execute(self, sql: str, *params) -> None:
        with self.connect() as con:
            con.execute(sql, params)


@pytest.fixture
def db(tmp_path: Path) -> SourceDB:
    return SourceDB(tmp_path / "source.sqlite")


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "configs"
    path.mkdir()
    monkeypatch.setenv("CONFIG_URI", path.as_uri())
    return path


@pytest.fixture
def invoke(db: SourceDB, bucket: Path, config_dir: Path, pipelines_dir: str, monkeypatch):
    """Write the config for `source_id` "core" and return a function that invokes the handler."""
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    table = {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
        "backend": "pyarrow",
    }

    def write_config(**table_overrides) -> None:
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "connection": {"dialect": "sqlite", "path": str(db.path)},
            "tables": [table | table_overrides],
        }
        (config_dir / "core.json").write_text(json.dumps(cfg))

    def run(**table_overrides) -> dict:
        write_config(**table_overrides)
        return handler({"source_id": "core"}, None)

    return run


def files(bucket: Path, table: str = "transactions") -> list[Path]:
    root = bucket / "core_banking" / table
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def landed(bucket: Path, table: str = "transactions") -> list[dict]:
    return [r for f in files(bucket, table) for r in pq.read_table(f).to_pylist()]


def ids(rows: list[dict]) -> list[int]:
    return sorted(r["TransactionId"] for r in rows)


def test_incremental_runs_land_only_new_rows(db, bucket, invoke):
    db.insert(
        (1, "2026-11-20 08:00:00", "JHB", 100),
        (2, "2026-11-20 09:00:00", "CPT", 200),
        (3, "2026-11-20 10:00:00", "DBN", 300),
    )
    r1 = invoke()
    assert r1["rows"] == {"transactions": 3}
    assert ids(landed(bucket)) == [1, 2, 3]
    today = datetime.now(UTC)
    assert {f.parent.relative_to(bucket).as_posix() for f in files(bucket)} == {
        f"core_banking/transactions/mode=incremental/year={today:%Y}/month={today:%m}/day={today:%d}"
    }

    db.insert((4, "2026-11-20 11:00:00", "JHB", 400), (5, "2026-11-21 08:00:00", "PTA", 500))
    run1_files = files(bucket)
    r2 = invoke()
    assert r2["rows"] == {"transactions": 2}
    assert set(run1_files) <= set(files(bucket))  # append: run 1's files are still there
    assert ids(landed(bucket)) == [1, 2, 3, 4, 5]

    r3 = invoke()
    assert r3["is_empty"]
    assert ids(landed(bucket)) == [1, 2, 3, 4, 5]


def test_column_names_land_exactly_as_in_the_source(db, bucket, invoke, caplog):
    from dlt.destinations import path_utils

    path_utils._log_unsafe_prefix_separators.cache_clear()  # dlt logs it once per process
    dlt_logger = logging.getLogger("dlt")  # does not propagate, so caplog needs its own handler
    dlt_logger.addHandler(caplog.handler)
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    try:
        invoke()
    finally:
        dlt_logger.removeHandler(caplog.handler)
    [f] = files(bucket)
    assert pq.read_schema(f).names == ["TransactionId", "UpdatedAt", "Branch Code", "amountCents", "_extracted_at"]
    assert "warn_unsafe_layout_separators" not in caplog.text


@pytest.mark.parametrize("name", ["Transactions", "core/transactions", "1tx", "tx-raw"])
def test_table_names_must_be_lowercase_identifiers(invoke, name):
    with pytest.raises(ConfigError, match="must match"):
        invoke(name=name)


def test_naive_source_timestamp_lands_naive_and_unchanged(db, bucket, invoke):
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    invoke()
    [f] = files(bucket)
    assert pq.read_schema(f).field("UpdatedAt").type == pa.timestamp("us")
    assert [r["UpdatedAt"] for r in landed(bucket)] == [datetime(2026, 11, 20, 8, 0)]


def test_extracted_at_is_utc_and_one_value_per_run(db, bucket, invoke):
    db.insert(*[(i, f"2026-11-20 08:00:{i:02}", "JHB", i) for i in range(1, 6)])
    before = datetime.now(UTC)
    invoke(batch_size=2)
    after = datetime.now(UTC)
    for f in files(bucket):
        assert pq.read_schema(f).field("_extracted_at").type == pa.timestamp("us", tz="UTC")
    [extracted_at] = {r["_extracted_at"] for r in landed(bucket)}
    assert before <= extracted_at <= after


def chain(exc: BaseException) -> list[str]:
    names = []
    while exc is not None:
        names.append(type(exc).__name__)
        exc = exc.__cause__ or exc.__context__
    return names


def test_new_source_column_lands(db, bucket, invoke):
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    invoke()
    db.execute('ALTER TABLE "Transactions" ADD COLUMN "Narrative" TEXT')
    db.execute('INSERT INTO "Transactions" VALUES (2, \'2026-11-21 08:00:00\', \'CPT\', 200, \'salary\')')
    invoke()
    [r1, r2] = sorted(landed(bucket), key=lambda r: r["TransactionId"])
    assert "Narrative" not in r1 and r2["Narrative"] == "salary"


def test_type_change_on_existing_column_fails_the_run(db, bucket, invoke):
    db.insert((1, "2026-11-20 08:00:00", "JHB", 100))
    invoke()
    db.execute('ALTER TABLE "Transactions" RENAME TO "Old"')
    db.execute(DDL.replace('"amountCents" INTEGER', '"amountCents" TEXT'))
    db.execute('INSERT INTO "Transactions" VALUES (2, \'2026-11-21 08:00:00\', \'CPT\', \'two hundred\')')
    with pytest.raises(Exception) as failure:
        invoke()
    assert "DataValidationError" in chain(failure.value)
    assert ids(landed(bucket)) == [1]


def test_rows_arrive_in_batches_of_batch_size(db, bucket, invoke, monkeypatch):
    # dlt concatenates buffered batches into one file; a one-item file limit makes each batch its own file
    monkeypatch.setenv("DATA_WRITER__FILE_MAX_ITEMS", "1")
    db.insert(*[(i, f"2026-11-20 08:00:{i:02}", "JHB", i) for i in range(1, 6)])
    invoke(batch_size=2)
    assert sorted(pq.ParquetFile(f).metadata.num_rows for f in files(bucket)) == [1, 2, 2]


def test_integer_cursor(db, bucket, invoke):
    cursor = {"column": "TransactionId", "type": "integer", "initial_value": 2}
    db.insert(*[(i, "2026-11-20 08:00:00", "JHB", i) for i in range(1, 4)])
    invoke(cursor=cursor)
    assert ids(landed(bucket)) == [2, 3]
    db.insert((4, "2026-11-20 08:00:00", "JHB", 4))
    invoke(cursor=cursor)
    assert ids(landed(bucket)) == [2, 3, 4]


def test_nullability_comes_from_reflection(db, bucket, invoke):
    db.insert((1, "2026-11-20 08:00:00", None, 100))
    invoke()
    [f] = files(bucket)
    schema = pq.read_schema(f)
    assert not schema.field("TransactionId").nullable and not schema.field("UpdatedAt").nullable
    assert schema.field("Branch Code").nullable and schema.field("amountCents").nullable
