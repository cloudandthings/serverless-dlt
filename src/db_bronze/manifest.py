"""Snapshot manifests: which loads make up one complete `mode=full` snapshot of a table (DESIGN-db.md 12).

A `full` load counts for silver only if a manifest lists it. A single-invocation
full run writes one manifest per table on success; paged snapshots get theirs
from `finalize`, in the same format:

    <bucket_url>/<dataset>/_manifests/<name>/<snapshot_id>.json
    {"table": <name>, "snapshot_id": ..., "load_ids": [...], "written_at": <ISO-8601 UTC>}
"""

from __future__ import annotations

import json
import posixpath
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime

import fsspec

from .config import ConfigError, RunRequest, SourceConfig


def new_snapshot_id(now: datetime) -> str:
    """Unique and sortable by time: `20261124T093000Z-<8 hex>`."""
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def manifest_url(bucket_url: str, dataset: str, table: str, snapshot_id: str) -> str:
    return posixpath.join(bucket_url.rstrip("/"), dataset, "_manifests", table, f"{snapshot_id}.json")


def write_manifest(bucket_url: str, dataset: str, table: str, snapshot_id: str, load_ids: Iterable[str]) -> str:
    """Write the manifest and return its URL."""
    url = manifest_url(bucket_url, dataset, table, snapshot_id)
    body = {
        "table": table,
        "snapshot_id": snapshot_id,
        "load_ids": list(load_ids),
        "written_at": datetime.now(UTC).isoformat(),
    }
    fs, path = fsspec.core.url_to_fs(url)
    fs.makedirs(posixpath.dirname(path), exist_ok=True)  # local filesystems need it; a no-op on object stores
    fs.pipe_file(path, json.dumps(body, indent=2).encode())
    return url


def finalize(cfg: SourceConfig, req: RunRequest) -> dict:
    """`{"mode": "finalize"}`: write the manifest that makes a paged snapshot's loads count for silver."""
    known = [t.name for t in cfg.tables]
    if req.table not in known:
        raise ConfigError(f'mode=finalize: unknown table {req.table!r}; this config has {known}')
    url = write_manifest(cfg.bucket_url, cfg.dataset_name, req.table, req.snapshot_id, req.load_ids)
    return {
        "pipeline_name": cfg.pipeline_name,
        "mode": "finalize",
        "table": req.table,
        "snapshot_id": req.snapshot_id,
        "load_ids": list(req.load_ids),
        "manifest": url,
    }
