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

from datetime import UTC, datetime
from typing import Any

from .config import RunRequest, SourceConfig, TableConfig
from .manifest import new_snapshot_id
from .query import count_select, paged_rows
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


def make_plan(cfg: SourceConfig, req: RunRequest, tables: tuple[TableConfig, ...]) -> dict[str, Any]:
    now = datetime.now(UTC)
    engine = make_engine(cfg)
    try:
        pages = []
        with engine.connect() as conn:
            for tc in tables:
                source = reflect(engine, tc)
                count = conn.execute(count_select(paged_rows(source, tc, initial_value(cfg, tc)))).scalar_one()
                pages += plan_pages(tc.name, count, req.page_size)
    finally:
        engine.dispose()
    return {
        "pipeline_name": cfg.pipeline_name,
        "mode": "plan",
        "plan": req.plan,
        "snapshot_id": new_snapshot_id(now),
        "partition_date": f"{now:%Y-%m-%d}",
        "pages": pages,
    }

