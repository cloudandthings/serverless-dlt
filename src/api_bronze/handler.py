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

from bronze_common.handler import find_in_chain, lambda_budget, load_config

# module scope: imported during the init phase, not on every invocation
from .config import RunRequest, SourceConfig
from .envelope import BrokenPageError
from .pipeline import run_pipeline

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), force=True)
log = logging.getLogger("api_bronze")

def handler(event: dict, context) -> dict:
    cfg = load_config(event, SourceConfig)
    req = RunRequest.from_event(event)
    budget = lambda_budget(context) if req.mode == "incremental" else None
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
        broken = find_in_chain(e, BrokenPageError)
        if broken is not None:
            # surface it as the Lambda errorType (dlt wraps it in PipelineStepFailed),
            # so alarms and log filters can tell "vendor sent garbage" from other failures
            log.error("broken page, run failed, cursor held: %s", broken)
            raise broken from e
        raise
    log.info("run done %s", json.dumps(result))
    return result
