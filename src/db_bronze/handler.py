"""AWS Lambda entry point for db_bronze. Thin on purpose: all logic is in `pipeline.run_pipeline`.

Event:
    {"source_id": "core_banking"}           incremental (default)
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

from .config import RunRequest, SourceConfig
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
    log.info("run start %s", json.dumps({"pipeline": cfg.pipeline_name, "mode": req.mode}))
    result = run_pipeline(cfg, req)
    log.info("run done %s", json.dumps(result))
    return result
