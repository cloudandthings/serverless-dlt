"""db_bronze integration: keyset chunking and the time budget on real engines (DESIGN-db.md 7.4)."""

from __future__ import annotations

import random
from datetime import datetime
from pathlib import Path

import pytest

from .conftest import fails

pytestmark = pytest.mark.integration

CURSOR = {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"}
KEYSET = {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 7}
STREAM = {"strategy": "stream"}

# Whether closing the traditional backend's streaming cursor early ends the query (S1, DESIGN-db.md 7.4).
# Where it does not, a `stream` table is never split-loaded and validation rejects `max_time_s` with it.
TRADITIONAL_STREAM_STOPS_EARLY = {"postgres": True, "mysql": False}


@pytest.fixture
def tx(backend: str) -> dict:
    return {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": CURSOR,
        "backend": backend,
        "chunking": KEYSET,
    }


def insert_with_ties(source, first_id: int, count: int, day: int = 20) -> list[int]:
    """`count` rows in groups of 10 sharing one UpdatedAt, inserted in shuffled key order."""
    rows = [
        (i, f"2026-11-{day} {8 + (i - first_id) // 10:02d}:00:00", "JHB", i * 100)
        for i in range(first_id, first_id + count)
    ]
    random.Random(first_id).shuffle(rows)
    source.transactions(*rows)
    return sorted(r[0] for r in rows)


def ids(source) -> list[int]:
    return source.landed_ids("transactions", "TransactionId")


def test_a_table_many_pages_long_with_many_ties_lands_completely_once(source, run, tx):
    expected = insert_with_ties(source, 1, 53)  # 8 pages of 7; most page boundaries fall inside a tie
    assert run({"source_id": "core"}, tx)["rows"] == {"transactions": 53}
    assert ids(source) == expected
    more = insert_with_ties(source, 54, 20, day=21)
    assert run({"source_id": "core"}, tx)["rows"] == {"transactions": 20}
    assert ids(source) == expected + more


def cursor_state(bucket: Path, pipelines_dir: str):
    """The `transactions` cursor as the next cold-started run restores it from bronze."""
    import dlt
    from dlt.destinations import filesystem

    pipe = dlt.pipeline(
        pipeline_name="it",
        dataset_name="it",
        pipelines_dir=str(Path(pipelines_dir) / "inspect"),
        destination=filesystem(bucket_url=bucket.as_uri()),
    )
    pipe.sync_destination()
    return pipe.state["sources"]["it"]["resources"]["transactions"]["incremental"]["UpdatedAt"]["last_value"]


def test_a_tiny_budget_stops_between_pages_and_looping_until_empty_lands_every_row_once(
    source, run, tx, bucket, pipelines_dir
):
    expected = insert_with_ties(source, 1, 53)
    first = run({"source_id": "core"}, tx, max_time_s=1e-6)
    assert first["time_limited"]
    assert first["rows"] == {"transactions": 7}  # one whole page, then the deadline
    assert ids(source) == [1, 2, 3, 4, 5, 6, 7]  # all at 08:00
    assert cursor_state(bucket, pipelines_dir) == datetime(2026, 11, 20, 8, 0)

    runs = [first]
    while not runs[-1]["is_empty"]:
        runs.append(run({"source_id": "core"}, tx, max_time_s=1e-6))
        assert len(runs) < 30
    assert len(runs) > 5  # really was cut short, run after run
    assert ids(source) == expected  # every row, none twice
    assert cursor_state(bucket, pipelines_dir) == datetime(2026, 11, 20, 13, 0)


@pytest.mark.parametrize("backend", ["pyarrow"])
def test_a_traditional_stream_table_is_split_loaded_only_where_closing_it_ends_the_query(source, run, tx):
    expected = insert_with_ties(source, 1, 53)
    stream = tx | {"chunking": STREAM, "batch_size": 12}
    if not TRADITIONAL_STREAM_STOPS_EARLY[source.dialect]:
        with fails("ConfigError", r"max_time_s.*keyset"):
            run({"source_id": "core"}, stream, max_time_s=1e-6)
        return
    runs = [run({"source_id": "core"}, stream, max_time_s=1e-6)]
    while not runs[-1]["is_empty"]:
        runs.append(run({"source_id": "core"}, stream, max_time_s=1e-6))
        assert len(runs) < 30
    assert runs[0]["time_limited"] and runs[0]["rows"] == {"transactions": 12}  # one batch, then the limit
    assert ids(source) == expected


class LambdaContext:
    def __init__(self, remaining_s: float) -> None:
        self.remaining_s = remaining_s

    def get_remaining_time_in_millis(self) -> int:
        return int(self.remaining_s * 1000)


def test_a_stream_table_that_cannot_stop_early_is_not_time_limited_under_the_lambda_budget(source, run, tx, backend):
    if backend == "pyarrow" and TRADITIONAL_STREAM_STOPS_EARLY[source.dialect]:
        pytest.skip("this backend's stream stops early, so it gets the budget")
    expected = insert_with_ties(source, 1, 53)
    result = run({"source_id": "core"}, tx | {"chunking": STREAM}, context=LambdaContext(900))
    assert not result["time_limited"] and result["is_empty"] is False
    assert ids(source) == expected


def test_a_keyset_table_gets_the_budget_derived_from_lambdas_remaining_time(source, run, tx):
    insert_with_ties(source, 1, 20)
    assert run({"source_id": "core"}, tx, context=LambdaContext(900))["time_limited"]
