"""AWS Lambda entry point for db_bronze. Thin on purpose: all logic is in `pipeline.run_pipeline`.

Event:
    {"source_id": "core_banking"}           incremental (default)
    {"source_id": ..., "mode": "full"}      [initial_value, run start); snapshot tables in full;
                                            writes _manifests/<name>/<snapshot_id>.json on success
    {"source_id": ..., "mode": "backfill", "window_start": ..., "window_end": ...}
                                            [start, end), ISO-8601 or integer to match the cursor
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
