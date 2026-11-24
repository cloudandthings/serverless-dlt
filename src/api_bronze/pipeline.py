"""The portable core: `run_pipeline(config, request)`. The Lambda handler is a thin wrapper.

Every invocation is treated as a cold start: the local working directory is
dropped and state is restored from bronze. A warm Lambda container therefore
behaves exactly like a cold one, and a half-finished package left in /tmp by a
failed invocation can never be replayed instead of a fresh extract.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

import dlt
from dlt.destinations import filesystem

from .config import ConfigError, RunRequest, SourceConfig
from .source import make_resource

log = logging.getLogger(__name__)

LAYOUT = "{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"


def bronze_destination(cfg: SourceConfig, req: RunRequest):
    """Filesystem destination with the bronze layout.

    The date in the path is the load date, except for backfills, where it is the
    window start so a backfill window is self-describing and re-runnable.
    `{table_name}` must come first: dlt rejects anything but `{schema_name}`
    before it.
    """
    return filesystem(
        bucket_url=cfg.bucket_url,
        layout=LAYOUT,
        extra_placeholders={"mode": req.mode},
        current_datetime=req.window_start if req.mode == "backfill" else None,
    )


def run_pipeline(
    cfg: SourceConfig, req: RunRequest, *, pipelines_dir: str | None = None, max_time_s: float | None = None
) -> dict[str, Any]:
    """Run one invocation. Raises on any failure (cursor state then stays where it was).

    `max_time_s` (the caller's time budget) and `cfg.max_time_s` bound extraction
    for split loading; the smaller wins. It applies in incremental mode only, and
    only when every resource is `sorted_ascending`. Callers loop while the
    result's `is_empty` is false.
    """
    if req.mode in ("incremental", "backfill"):
        missing = [r.name for r in cfg.resources if r.incremental is None]
        if missing:
            raise ConfigError(
                f"mode={req.mode} needs `incremental` on every resource; missing on {missing}. "
                "Endpoints without a cursor run as mode=full."
            )
    # Parquet codec for the files dlt writes (dlt's default is snappy; zstd is ~4x smaller here)
    os.environ["DATA_WRITER__COMPRESSION"] = cfg.parquet_compression

    run_started = datetime.now(UTC)
    pipe = dlt.pipeline(
        pipeline_name=cfg.pipeline_name,  # STABLE: part of the state identity
        dataset_name=cfg.dataset_name,  # STABLE: part of the state identity
        destination=bronze_destination(cfg, req),
        pipelines_dir=pipelines_dir,
    )
    pipe = pipe.drop()  # every invocation is a cold start: state comes from bronze
    resources = [make_resource(cfg, rc, req, run_started) for rc in cfg.resources]

    limits = [x for x in (max_time_s, cfg.max_time_s) if x is not None]
    limited = False
    if limits and req.mode == "incremental":
        if all(rc.incremental.sorted_ascending for rc in cfg.resources):
            resources = [r.add_limit(max_time=min(limits)) for r in resources]
            limited = True
        elif cfg.max_time_s is not None:
            log.warning(
                "max_time_s ignored: split loading needs sorted_ascending on every resource, "
                "otherwise a cut-off run would skip older records on later pages"
            )

    info = pipe.run(resources, loader_file_format="parquet")
    norm = pipe.last_trace.last_normalize_info
    rows = {t: n for t, n in (norm.row_counts if norm else {}).items() if not t.startswith("_dlt")}
    return {
        "pipeline_name": cfg.pipeline_name,
        "mode": req.mode,
        "window_start": req.window_start.isoformat() if req.window_start else None,
        "window_end": req.window_end.isoformat() if req.window_end else None,
        "load_ids": list(info.loads_ids),
        "rows": rows,
        "is_empty": not any(rows.values()),
        "time_limited": limited,
        "started_at": run_started.isoformat(),
    }
