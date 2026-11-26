"""db_bronze: keyset chunking and the time budget, handler-level against SQLite (docs/db/design.md Chunking)."""

from __future__ import annotations

import json
import random
from datetime import datetime
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


def cursor_state(bucket: Path, pipelines_dir: str):
    """The `transactions` cursor as the next cold-started run restores it from bronze."""
    import dlt
    from dlt.destinations import filesystem

    pipe = dlt.pipeline(
        pipeline_name="core_banking",
        dataset_name="core_banking",
        pipelines_dir=str(Path(pipelines_dir) / "inspect"),
        destination=filesystem(bucket_url=bucket.as_uri()),
    )
    pipe.sync_destination()
    return pipe.state["sources"]["core_banking"]["resources"]["transactions"]["incremental"]["UpdatedAt"]["last_value"]


def test_a_tiny_budget_stops_between_pages_and_looping_until_empty_lands_every_row_once(
    db, bucket, invoke, pages, pipelines_dir
):
    expected = insert_with_ties(db, 1, 53)
    first = invoke(max_time_s=1e-6)
    assert first["time_limited"]
    assert first["rows"] == {"transactions": 7} and len(pages) == 1  # one whole page, then the deadline
    assert ids(landed(bucket)) == [1, 2, 3, 4, 5, 6, 7]  # all at 08:00
    assert cursor_state(bucket, pipelines_dir) == datetime(2026, 11, 20, 8, 0)

    runs = [first]
    while not runs[-1]["is_empty"]:
        runs.append(invoke(max_time_s=1e-6))
        assert len(runs) < 30
    assert len(runs) > 5  # really was cut short, run after run
    assert ids(landed(bucket)) == expected  # every row, none twice
    assert cursor_state(bucket, pipelines_dir) == datetime(2026, 11, 20, 13, 0)


@pytest.mark.parametrize("backend", ["pyarrow"])
def test_a_traditional_stream_table_is_split_loaded_by_dlts_time_limit(db, bucket, invoke):
    expected = insert_with_ties(db, 1, 53)
    stream = {"chunking": {"strategy": "stream"}, "batch_size": 12}
    runs = [invoke(max_time_s=1e-6, **stream)]
    while not runs[-1]["is_empty"]:
        runs.append(invoke(max_time_s=1e-6, **stream))
        assert len(runs) < 30
    assert runs[0]["time_limited"] and runs[0]["rows"] == {"transactions": 12}  # one batch, then the limit
    assert ids(landed(bucket)) == expected


class LambdaContext:
    def __init__(self, remaining_s: float) -> None:
        self.remaining_s = remaining_s

    def get_remaining_time_in_millis(self) -> int:
        return int(self.remaining_s * 1000)


STREAM = {"chunking": {"strategy": "stream"}}


@pytest.mark.parametrize("backend", ["connectorx"])
def test_a_connectorx_stream_table_is_not_time_limited_under_the_lambda_budget_and_the_log_says_so(
    db, bucket, invoke, caplog
):
    expected = insert_with_ties(db, 1, 53)
    with caplog.at_level("WARNING", logger="db_bronze"):
        result = invoke(context=LambdaContext(900), **STREAM)
    assert not result["time_limited"]
    assert ids(landed(bucket)) == expected
    [skipped] = [r.getMessage() for r in caplog.records if "time budget skipped" in r.getMessage()]
    assert '"table": "transactions"' in skipped and "keyset" in skipped


@pytest.mark.parametrize("table", [STREAM | {"backend": "pyarrow"}, {}], ids=["pyarrow-stream", "keyset"])
def test_other_tables_get_the_budget_derived_from_lambdas_remaining_time(db, bucket, invoke, table):
    insert_with_ties(db, 1, 20)
    assert invoke(context=LambdaContext(900), **table)["time_limited"]
    assert not invoke(**table)["time_limited"]  # no context, no configured max_time_s: no budget
