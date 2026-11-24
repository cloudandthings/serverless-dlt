"""AWS Lambda entry point for db_bronze. Thin on purpose: all logic is in `pipeline.run_pipeline`.

Event:
    {"source_id": "core_banking"}           incremental (default)
    {"source_id": ..., "mode": "full"}      [initial_value, run start); snapshot tables in full;
                                            writes _manifests/<name>/<snapshot_id>.json on success
    {"source_id": ..., "mode": "backfill", "window_start": ..., "window_end": ...}
                                            [start, end), ISO-8601 or integer to match the cursor
    {"source_id": ..., "mode": "validate"}  preflight only: per-table report, nothing extracted or written
    {"source_id": ..., "mode": "plan", "plan": "snapshot", "page_size": N}
                                            no extract: pages [{table, offset, limit}] per table (the last
                                            open-ended), plus a fresh snapshot_id and partition_date
    {"source_id": ..., "mode": "full", "tables": [t], "page": {"offset": ..., "limit": ...},
     "snapshot_id": ..., "partition_date": "YYYY-MM-DD"}
                                            one page of a planned snapshot, ordered by the primary key
                                            (or keyset order_by), under mode=full/<partition_date>; no manifest
    {"source_id": ..., "mode": "plan", "plan": "backfill", "windows": N | "window_size": S}
                                            no extract: the cursor's [MIN, MAX] as half-open windows
                                            [{table, window_start, window_end}], timestamps in UTC
    {"source_id": ..., "mode": "finalize", "table": t, "snapshot_id": ..., "load_ids": [...]}
                                            writes _manifests/<t>/<snapshot_id>.json (examples/db_snapshot.asl.json)
    "tables": [...]                         on any event: run only these tables
    {"config": {...}}                       inline config (tests, ad hoc)
Unknown event keys fail with ConfigError. Paged and parallel runs use their own
config with a pipeline_name of their own, never the incremental's.

Config lookup when `source_id` is given: `${CONFIG_URI}/${source_id}.json`, where
CONFIG_URI is any fsspec URL (`s3://config-bucket/sources`, `file:///opt/config`).

Environment:
    CONFIG_URI          where source configs live (required with source_id)
    SAFETY_MARGIN_S     seconds kept free before the Lambda timeout (default 120)
    LOG_LEVEL           default INFO

An incremental run gets a time budget from Lambda's remaining time minus the
safety margin (never under MIN_BUDGET_S), and `max_time_s` in the config caps it
further. Keyset tables stop between pages, traditional stream tables are cut by
dlt; ConnectorX stream tables are never cut (DESIGN-db.md 7.4). The result
reports `time_limited` and `is_empty`: the orchestrator re-invokes while
`is_empty` is false.
"""

from __future__ import annotations

import json
import logging
import os
import posixpath

import fsspec

from . import metrics
from .config import RunRequest, SourceConfig
from .errors import DbBronzeError
from .pipeline import run_pipeline

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), force=True)
log = logging.getLogger("db_bronze")

MIN_BUDGET_S = 60.0


def load_config(event: dict) -> SourceConfig:
    if "config" in event:
        return SourceConfig.from_dict(event["config"])
    source_id = event.get("source_id")
    if not source_id or "/" in source_id or source_id.startswith("."):
        raise ValueError(f"event needs a plain source_id or an inline config, got {source_id!r}")
    uri = posixpath.join(os.environ["CONFIG_URI"].rstrip("/"), f"{source_id}.json")
    with fsspec.open(uri, "rb") as f:
        return SourceConfig.from_json(f.read())


def handler(event: dict, context) -> dict:
    cfg = load_config(event)
    req = RunRequest.from_event(event)
    budget = None
    if context is not None and req.mode == "incremental":
        margin = float(os.environ.get("SAFETY_MARGIN_S", "120"))
        budget = max(MIN_BUDGET_S, context.get_remaining_time_in_millis() / 1000 - margin)
    log.info(json.dumps({"event": "run start", "pipeline": cfg.pipeline_name, "mode": req.mode}))
    try:
        result = run_pipeline(cfg, req, max_time_s=budget)
    except Exception as e:
        # dlt wraps errors raised in a resource in PipelineStepFailed; surface ours as the Lambda errorType
        known = _find_in_chain(e, DbBronzeError)
        if known is None:
            raise
        _report(cfg, known)
        if known is e:
            raise
        raise known from e
    for table, count in result.get("rows", {}).items():
        metrics.emit(
            cfg.pipeline_name,
            table,
            RowsExtracted=count,
            NullCursorRows=result.get("null_cursor_rows", {}).get(table, 0),
        )
    log.info(json.dumps({"event": "run done", **result}))
    return result


def _report(cfg: SourceConfig, error: DbBronzeError) -> None:
    log.error("run failed %s", json.dumps({"error": type(error).__name__, "message": str(error)}))
    if error.metric:
        metrics.emit(cfg.pipeline_name, error.table or "", **{error.metric: 1})


def _find_in_chain(exc: BaseException, kind: type[BaseException]) -> BaseException | None:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, kind):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return None
