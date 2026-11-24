"""The portable core: `run_pipeline(config, request)`. The Lambda handler is a thin wrapper.

Every invocation is treated as a cold start: the local working directory is
dropped and state is restored from bronze, as in `api_bronze`.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

import dlt
from dlt.common.schema import Schema
from dlt.destinations import filesystem

from .config import RunRequest, SourceConfig
from .source import make_engine, make_resource

log = logging.getLogger(__name__)

LAYOUT = "{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"


def bronze_destination(cfg: SourceConfig, req: RunRequest):
    """Filesystem destination with the bronze layout.

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


def run_pipeline(cfg: SourceConfig, req: RunRequest, *, pipelines_dir: str | None = None) -> dict[str, Any]:
    """Run one invocation. Raises on any failure (cursor state then stays where it was)."""
    tables = cfg.tables_for(req)
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
    norm = pipe.last_trace.last_normalize_info
    rows = {t: n for t, n in (norm.row_counts if norm else {}).items() if not t.startswith("_dlt")}
    return {
        "pipeline_name": cfg.pipeline_name,
        "mode": req.mode,
        "window_start": req.window_start,
        "window_end": req.window_end,
        "load_ids": list(info.loads_ids),
        "rows": rows,
        "is_empty": not any(rows.values()),
        "started_at": run_started.isoformat(),
    }
