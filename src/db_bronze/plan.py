"""`{"mode": "plan"}`: split work that does not fit one invocation, for a Step Function (DESIGN-db.md D8, 7.5).

Nothing is extracted and nothing is written.

snapshot  `COUNT(*)` per table, then pages `[{table, offset, limit}]` for page events
          (`mode=full` with `page`), the last page of each table open-ended, plus a fresh
          `snapshot_id` and the `partition_date` every page lands under.

OFFSET paging is not consistent across invocations: rows that change while a
snapshot runs can be duplicated or missed, and deep offsets get slower. Prefer
keyset chunking wherever a unique sortable key exists.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from .config import ConfigError, RunRequest, SourceConfig, TableConfig
from .manifest import new_snapshot_id
from .query import count_select, paged_rows, snapshot_select
from .source import make_engine, reflect


def initial_value(cfg: SourceConfig, tc: TableConfig):
    """Where a paged snapshot of a cursor table starts, as the source represents it (None: snapshot table)."""
    return tc.cursor.initial_cursor_value(cfg.connection.naive_timezone) if tc.cursor else None


def plan_pages(table: str, count: int, page_size: int) -> list[dict[str, Any]]:
    """Pages of `page_size` rows covering `count` rows; the last one (at least one) is open-ended."""
    offsets = range(0, max(count, 1), page_size)
    return [
        {"table": table, "offset": offset, "limit": page_size if offset + page_size < count else None}
        for offset in offsets
    ]


def window_bounds(low, high, *, windows: int | None = None, size=None) -> list[tuple[Any, Any]]:
    """Half-open, contiguous windows from `low` to just past `high`: `windows` of them, or each `size` wide.

    Just past `high` is `high + 1` for an integer cursor, and `high + 1 minute`
    for a timestamp: the source compares the bound at its column's resolution,
    and a finer step can round back down to `high` and miss the last rows
    (SQL Server smalldatetime is to the minute; SQLite's julianday() to ~50µs).
    """
    step = 1 if isinstance(low, int) else timedelta(minutes=1)
    end = high + step
    if windows is not None:
        span = high - low
        starts = [low + (span * i // windows if isinstance(low, int) else span * i / windows) for i in range(windows)]
    else:
        width = size if isinstance(low, int) else timedelta(seconds=size)
        starts = []
        while (start := low + width * len(starts)) < end:
            starts.append(start)
    starts = sorted(set(starts))  # a span narrower than the window count collapses equal starts
    return list(zip(starts, [*starts[1:], end], strict=True))


def to_utc(value, naive_timezone: str):
    """A source cursor value as the API boundary carries it: integers as they are, timestamps as UTC ISO-8601."""
    if not isinstance(value, datetime):
        return value
    aware = value if value.tzinfo else value.replace(tzinfo=ZoneInfo(naive_timezone))
    return aware.astimezone(UTC).isoformat()


def plan_windows(cfg: SourceConfig, req: RunRequest, tc: TableConfig, conn, source) -> list[dict[str, Any]]:
    if tc.cursor.type == "integer" and req.window_size is not None and not isinstance(req.window_size, int):
        raise ConfigError(f"table {tc.name}: an integer cursor needs an integer window_size: {req.window_size!r}")
    cursor = source.c[tc.cursor.column]
    rows = snapshot_select(source, tc).subquery("bounded")
    low, high = conn.execute(sa.select(sa.func.min(rows.c[cursor.name]), sa.func.max(rows.c[cursor.name]))).one()
    if low is None:
        return []
    tz = cfg.connection.naive_timezone
    return [
        {"table": tc.name, "window_start": to_utc(start, tz), "window_end": to_utc(end, tz)}
        for start, end in window_bounds(low, high, windows=req.windows, size=req.window_size)
    ]


def make_plan(cfg: SourceConfig, req: RunRequest, tables: tuple[TableConfig, ...]) -> dict[str, Any]:
    now = datetime.now(UTC)
    result: dict[str, Any] = {"pipeline_name": cfg.pipeline_name, "mode": "plan", "plan": req.plan}
    engine = make_engine(cfg)
    try:
        pages, windows = [], []
        with engine.connect() as conn:
            for tc in tables:
                source = reflect(engine, tc)
                if req.plan == "backfill":
                    windows += plan_windows(cfg, req, tc, conn, source)
                    continue
                count = conn.execute(count_select(paged_rows(source, tc, initial_value(cfg, tc)))).scalar_one()
                pages += plan_pages(tc.name, count, req.page_size)
    finally:
        engine.dispose()
    if req.plan == "backfill":
        return result | {"windows": windows}
    return result | {"snapshot_id": new_snapshot_id(now), "partition_date": f"{now:%Y-%m-%d}", "pages": pages}

