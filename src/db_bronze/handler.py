"""AWS Lambda entry point for db_bronze. Thin on purpose: all logic is in `pipeline.run_pipeline`.

Event:
    {"source_id": "core_banking"}           incremental (default)
    {"source_id": ..., "mode": "full"}      [initial_value, run start); snapshot tables in full;
                                            writes _manifests/<name>/<snapshot_id>.json on success
    {"source_id": ..., "mode": "backfill", "window_start": ..., "window_end": ...}
                                            [start, end), ISO-8601 or integer to match the cursor
    {"source_id": ..., "mode": "validate"}  preflight only: per-table report, nothing extracted or written
    "tables": [...]                         on any event: run only these tables
    {"config": {...}}                       inline config (tests, ad hoc)

Config lookup when `source_id` is given: `${CONFIG_URI}/${source_id}.json`, where
CONFIG_URI is any fsspec URL (`s3://config-bucket/sources`, `file:///opt/config`).

Environment:
    CONFIG_URI          where source configs live (required with source_id)
    LOG_LEVEL           default INFO
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
    log.info(json.dumps({"event": "run start", "pipeline": cfg.pipeline_name, "mode": req.mode}))
    try:
        result = run_pipeline(cfg, req)
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
