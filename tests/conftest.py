"""Shared fixtures: an in-process fake REST API and helpers to inspect bronze."""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import pyarrow.parquet as pq
import pytest

# route: (query, headers, base_url) -> (status, body bytes)
Route = Callable[[dict[str, list[str]], dict[str, str], str], tuple[int, bytes]]


@contextlib.contextmanager
def serve(routes: dict[str, Route]):
    base: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            url = urlparse(self.path)
            route = routes.get(url.path.strip("/"))
            if route is None:
                self.send_error(404)
                return
            # links are built from the Host the client used, like a real API behind a proxy
            host = self.headers.get("Host")
            status, body = route(parse_qs(url.query), dict(self.headers), f"http://{host}" if host else base[0])
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    base.append(f"http://127.0.0.1:{server.server_port}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield base[0]
    finally:
        server.shutdown()


def utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    return (dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt).astimezone(UTC)


class EventsAPI:
    """GET /events?updated_since=&updated_before=&page=N. `since` exclusive, `before` exclusive.

    Records sorted by updatedAt (ascending), `page_size` per page, json_link pagination
    under `pagination.next`. `override[page]` serves exact bytes instead; `fail_on_page`
    serves a 500; `require_auth` checks the Authorization header.
    """

    def __init__(self, page_size: int = 3) -> None:
        self.page_size = page_size
        self.events: dict[str, dict] = {}
        self.override: dict[int, bytes] = {}
        self.fail_on_page: int | None = None
        self.require_auth: str | None = None
        self.served: list[bytes] = []
        self.queries: list[dict[str, str]] = []

    def upsert(self, *events: dict) -> None:
        for e in events:
            self.events[e["id"]] = e

    def __call__(self, q, headers, base_url):
        if self.require_auth and headers.get("Authorization") != self.require_auth:
            return 401, b'{"error": "unauthorised"}'
        flat = {k: v[0] for k, v in q.items()}
        page = int(flat.get("page", "1"))
        if page == 1:
            self.queries.append(flat)
        if self.fail_on_page == page:
            return 500, b'{"error": "upstream timeout"}'
        if page in self.override:
            body = self.override[page].replace(b"{next}", self._next(flat, page, base_url, True).encode())
            self.served.append(body)
            return 200, body
        since = utc(flat["updated_since"]) if "updated_since" in flat else None
        before = utc(flat["updated_before"]) if "updated_before" in flat else None
        rows = sorted(self.events.values(), key=lambda e: utc(e["updatedAt"]))
        rows = [
            e
            for e in rows
            if (since is None or utc(e["updatedAt"]) > since) and (before is None or utc(e["updatedAt"]) < before)
        ]
        chunk = rows[(page - 1) * self.page_size : page * self.page_size]
        more = page * self.page_size < len(rows) or (page + 1) in self.override
        body = json.dumps({"data": chunk, "pagination": {"next": None}}, indent=1).encode()
        body = body.replace(b'"next": null', b'"next": ' + self._next(flat, page, base_url, more).encode())
        self.served.append(body)
        return 200, body

    @staticmethod
    def _next(flat, page, base_url, more: bool) -> str:
        if not more:
            return "null"
        return json.dumps(f"{base_url}/events?{urlencode(flat | {'page': page + 1})}")


def event(i: int, updated_at: str, **extra) -> dict:
    return {
        "id": f"EV-{i:03}",
        "updatedAt": updated_at,
        "status": "open",
        "detail": {"amount": {"value": f"{i * 10}.00", "currency": "ZAR"}, "tags": ["t"]},
        **extra,
    }


def make_config(bucket: Path, base_url: str, **overrides) -> dict:
    resource = {
        "name": "events_raw",
        "path": "events",
        "records_path": "data",
        "id_path": "id",
        "incremental": {
            "cursor_path": "updatedAt",
            "initial_value": "2026-01-01T00:00:00Z",
            "start_param": "updated_since",
            "end_param": "updated_before",
        },
    }
    resource.update(overrides.pop("resource", {}))
    cfg = {
        "pipeline_name": "test_events",
        "dataset_name": "vendor",
        "bucket_url": bucket.as_uri(),
        "client": {"base_url": base_url, "paginator": {"type": "json_link", "next_url_path": "pagination.next"}},
        "resources": [resource],
    }
    cfg.update(overrides)
    return cfg


@pytest.fixture
def bucket(tmp_path: Path) -> Path:
    path = tmp_path / "bronze"
    path.mkdir()
    return path


@pytest.fixture
def pipelines_dir(tmp_path: Path) -> str:
    return str(tmp_path / "pipelines")


@pytest.fixture
def api():
    events = EventsAPI()
    with serve({"events": events}) as base_url:
        events.base_url = base_url
        yield events


def data_files(bucket: Path, table: str = "events_raw") -> list[Path]:
    root = bucket / "vendor" / table
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def bronze_rows(bucket: Path, table: str = "events_raw") -> list[dict]:
    rows = []
    for f in data_files(bucket, table):
        for r in pq.read_table(f).to_pylist():
            rows.append(r | {"_file": f})
    return rows


def quarantined(bucket: Path) -> list[Path]:
    return sorted((bucket / "_quarantine").rglob("*.body"))


def cursor_state(bucket: Path, pipelines_dir: str, pipeline_name: str = "test_events", table: str = "events_raw"):
    """Cursor as a cold-started pipeline would restore it from bronze."""
    import dlt
    from dlt.destinations import filesystem

    pipe = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name="vendor",
        pipelines_dir=pipelines_dir,
        destination=filesystem(bucket_url=bucket.as_uri()),
    ).drop()
    pipe.sync_destination()
    try:
        inc = pipe.state["sources"][pipe.default_schema_name]["resources"][table]["incremental"]
        return inc["updated_at_utc"]["last_value"]
    except (KeyError, TypeError):
        return None


def root_cause(exc: BaseException) -> BaseException:
    seen = exc
    while seen.__cause__ or seen.__context__:
        seen = seen.__cause__ or seen.__context__
        if type(seen).__name__ == "BrokenPageError":
            return seen
    return seen
