"""Snapshot manifests: which loads make up one complete `mode=full` snapshot of a table (DESIGN-db.md 12).

A `full` load counts for silver only if a manifest lists it. A single-invocation
full run writes one manifest per table on success; paged snapshots get theirs
from `finalize` (ticket 09), in the same format:

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
