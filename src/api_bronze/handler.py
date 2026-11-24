"""AWS Lambda entry point. Thin on purpose: all logic is in `pipeline.run_pipeline`,
so the same image runs unchanged as an ECS/Fargate task if a source outgrows 15 minutes.

Event:
    {"source_id": "vendor_orders"}                                     incremental (default)
    {"source_id": "vendor_orders", "mode": "full"}
    {"source_id": "vendor_orders", "mode": "backfill",
     "window_start": "2025-03-01T00:00:00Z", "window_end": "2025-04-01T00:00:00Z"}
    {"config": {...}, "mode": ...}                                     inline config (tests, ad hoc)

Config lookup when `source_id` is given: `${CONFIG_URI}/${source_id}.json`, where
CONFIG_URI is any fsspec URL (`s3://config-bucket/sources`, `file:///opt/config`).

Environment:
    CONFIG_URI          where source configs live (required with source_id)
    SAFETY_MARGIN_S     seconds kept free before the Lambda timeout (default 120)
    LOG_LEVEL           default INFO

Returns the run summary; the orchestrator re-invokes while `is_empty` is false.
Any failure raises, so the invocation is marked failed and the Lambda Errors
metric fires; cursor state does not move. A broken page raises BrokenPageError
itself (errorType "BrokenPageError"), its message naming the quarantined body.
"""

from __future__ import annotations

import json
import logging
import os
import posixpath

import fsspec

# module scope: imported during the init phase, not on every invocation
from .config import RunRequest, SourceConfig
from .envelope import BrokenPageError
from .pipeline import run_pipeline

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), force=True)
log = logging.getLogger("api_bronze")

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
    log.info(
        "run start %s",
        json.dumps(
            {
                "pipeline": cfg.pipeline_name,
                "mode": req.mode,
                "window_start": event.get("window_start"),
                "window_end": event.get("window_end"),
            }
        ),
    )
    try:
        result = run_pipeline(cfg, req, max_time_s=budget)
    except Exception as e:
        broken = _find_in_chain(e, BrokenPageError)
        if broken is not None:
            # surface it as the Lambda errorType (dlt wraps it in PipelineStepFailed),
            # so alarms and log filters can tell "vendor sent garbage" from other failures
            log.error("broken page, run failed, cursor held: %s", broken)
            raise broken from e
        raise
    log.info("run done %s", json.dumps(result))
    return result


def _find_in_chain(exc: BaseException, kind: type[BaseException]) -> BaseException | None:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, kind):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return None
