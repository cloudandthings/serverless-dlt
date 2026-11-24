"""The portable core: `run_pipeline(config, request)`. The Lambda handler is a thin wrapper.

Every invocation is treated as a cold start: the local working directory is
dropped and state is restored from bronze, as in `api_bronze`.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from datetime import UTC, datetime
from typing import Any

import dlt
from dlt.common.schema import Schema
from dlt.destinations import filesystem

from .config import ConfigError, RunRequest, SourceConfig, TableConfig
from .manifest import new_snapshot_id, write_manifest
from .plan import make_plan
from .source import make_engine, make_resource

log = logging.getLogger(__name__)

LAYOUT = "{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"


def bronze_destination(cfg: SourceConfig, req: RunRequest):
    """Filesystem destination with the bronze layout.

    The date in the path is the load date, except for timestamp backfills, where
    it is the window start, so a backfill window is self-describing.
    `direct` naming lets `/` appear in table names, which dlt warns makes the
    layout ambiguous. Bronze table names are validated lowercase identifiers,
    so the warning is turned off.
    """
    return filesystem(
        bucket_url=cfg.bucket_url,
        layout=LAYOUT,
        extra_placeholders={"mode": req.mode},
        current_datetime=req.partition_datetime,  # None: the load date
        warn_unsafe_layout_separators=False,
    )


def bronze_schema(cfg: SourceConfig) -> Schema:
    """The schema for the first run: `direct` naming keeps source column names exactly.

    Later runs use the schema restored from bronze, which carries the same naming.
    """
    return Schema(cfg.pipeline_name, normalizers={"names": "direct"})


def check_run_start(cfg: SourceConfig, req: RunRequest, tables: tuple[TableConfig, ...]) -> None:
    """Fail before anything is touched when the run cannot be done (DESIGN-db.md section 7.6)."""
    if cfg.connection.auth.type == "kerberos":
        raise NotImplementedError(
            'connection.auth.type "kerberos" is reserved but not implemented yet; see docs/KERBEROS.md '
            'for what adding it involves, or use {"type": "password"}'
        )
    by_cursor = req.mode in ("incremental", "backfill") or (req.mode == "plan" and req.plan == "backfill")
    what = f"plan={req.plan}" if req.mode == "plan" else f"{req.mode!r} mode"
    for tc in tables:
        if tc.cursor is None and by_cursor:
            raise ConfigError(
                f'table {tc.name} has no cursor, so it is a snapshot table; run it with "mode": "full", '
                f'give it a cursor to run in {what}, or leave it out with "tables": [...]'
            )


def run_pipeline(cfg: SourceConfig, req: RunRequest, *, pipelines_dir: str | None = None) -> dict[str, Any]:
    """Run one invocation. Raises on any failure (cursor state then stays where it was)."""
    tables = cfg.tables_for(req)
    check_run_start(cfg, req, tables)
    if req.mode == "plan":
        return make_plan(cfg, req, tables)
    os.environ["DATA_WRITER__COMPRESSION"] = cfg.parquet_compression

    run_started = datetime.now(UTC)
    pipe = dlt.pipeline(
        pipeline_name=cfg.pipeline_name,  # STABLE: part of the state identity
        dataset_name=cfg.dataset_name,  # STABLE: part of the state identity
        destination=bronze_destination(cfg, req),
        pipelines_dir=pipelines_dir,
    )
    pipe = pipe.drop()  # every invocation is a cold start: state comes from bronze
    engine = make_engine(cfg)
    try:
        resources = [make_resource(cfg, tc, req, engine, run_started) for tc in tables]
        info = pipe.run(resources, loader_file_format="parquet", schema=bronze_schema(cfg))
    finally:
        engine.dispose()
    load_ids = list(info.loads_ids)
    snapshot_id = req.snapshot_id  # a page: the plan's; finalize writes the manifest
    if req.mode == "full" and req.page is None:
        # Only now, after every load succeeded: a full load without a manifest is ignored by silver (section 12).
        snapshot_id = new_snapshot_id(run_started)
        for tc in tables:
            write_manifest(cfg.bucket_url, cfg.dataset_name, tc.name, snapshot_id, load_ids)
    norm = pipe.last_trace.last_normalize_info
    rows = {t: n for t, n in (norm.row_counts if norm else {}).items() if not t.startswith("_dlt")}
    return {
        "pipeline_name": cfg.pipeline_name,
        "mode": req.mode,
        "window_start": req.window_start,
        "window_end": req.window_end,
        "page": dataclasses.asdict(req.page) if req.page else None,
        "snapshot_id": snapshot_id,
        "load_ids": load_ids,
        "rows": rows,
        "is_empty": not any(rows.values()),
        "started_at": run_started.isoformat(),
    }
