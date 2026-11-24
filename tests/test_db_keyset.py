"""db_bronze: keyset chunking and the time budget, handler-level against SQLite (DESIGN-db.md 7.4)."""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest
import sqlalchemy as sa
from test_db_incremental import SourceDB, ids, landed

import db_bronze.extract
from db_bronze.handler import handler

KEYSET = {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 7}


@pytest.fixture
def db(tmp_path: Path) -> SourceDB:
    return SourceDB(tmp_path / "source.sqlite")


@pytest.fixture(params=["pyarrow", "connectorx"])
def backend(request) -> str:
    """Every handler-level scenario runs on every implemented backend."""
    return request.param


@pytest.fixture
def invoke(db: SourceDB, bucket: Path, tmp_path: Path, pipelines_dir: str, backend: str, monkeypatch):
    """Return a function that writes the config for source_id "core" and invokes the handler.

    The table is keyset-chunked unless `table` overrides `chunking`.
    """
    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())

    def run(context=None, max_time_s: float | None = None, **table) -> dict:
        tx = {
            "name": "transactions",
            "table": "Transactions",
            "primary_key": ["TransactionId"],
            "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
            "backend": backend,
            "chunking": KEYSET,
        }
        cfg = {
            "pipeline_name": "core_banking",
            "dataset_name": "core_banking",
            "bucket_url": bucket.as_uri(),
            "max_time_s": max_time_s,
            "connection": {"dialect": "sqlite", "path": str(db.path)},
            "tables": [tx | table],
        }
        (config_dir / "core.json").write_text(json.dumps(cfg))
        return handler({"source_id": "core"}, context)

    return run


@pytest.fixture
def pages(monkeypatch) -> list[str]:
    """The SQL of every page query the source receives, on either backend."""
    seen: list[str] = []

    def record(sql: str) -> None:
        if "LIMIT" in sql:
            seen.append(sql)

    def on_execute(conn, cursor, statement, parameters, context, executemany):
        record(statement)

    sa.event.listen(sa.Engine, "before_cursor_execute", on_execute)
    read_sql = db_bronze.extract.cx.read_sql

    def spy(conn, query, **kwargs):
        record(query)
        return read_sql(conn, query, **kwargs)

    monkeypatch.setattr(db_bronze.extract.cx, "read_sql", spy)
    yield seen
    sa.event.remove(sa.Engine, "before_cursor_execute", on_execute)


def insert_with_ties(db: SourceDB, first_id: int, count: int, day: int = 20) -> list[int]:
    """`count` rows in groups of 10 sharing one UpdatedAt, inserted in shuffled key order."""
    rows = [
        (i, f"2026-11-{day} {8 + (i - first_id) // 10:02d}:00:00", "JHB", i * 100)
        for i in range(first_id, first_id + count)
    ]
    random.Random(first_id).shuffle(rows)
    db.insert(*rows)
    return sorted(r[0] for r in rows)


def test_a_table_many_pages_long_with_many_ties_lands_completely_once(db, bucket, invoke, pages):
    expected = insert_with_ties(db, 1, 53)  # 8 pages of 7; most page boundaries fall inside a tie
    result = invoke()
    assert result["rows"] == {"transactions": 53}
    assert len(pages) == 8  # 7 full pages, then a short one ends the read
    assert ids(landed(bucket)) == expected

    more = insert_with_ties(db, 54, 20, day=21)
    assert invoke()["rows"] == {"transactions": 20}
    assert ids(landed(bucket)) == expected + more
