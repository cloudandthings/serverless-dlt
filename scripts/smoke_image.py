"""Smoke-test the built Lambda image locally through the Runtime Interface Emulator.

Starts the fake API on the host, runs the image with bronze and config mounted
as local folders, invokes the function like Lambda would, and checks bronze.

    docker build -f docker/Dockerfile.api --platform linux/arm64 -t api-bronze:test .
    uv run python scripts/smoke_image.py api-bronze:test
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parents[1] / "tests"))
from conftest import EventsAPI, event, serve

PORT = 9123


def invoke(payload: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/2015-03-31/functions/function/invocations",
        data=json.dumps(payload).encode(),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.status, json.loads(r.read() or b"null")


def main(image: str) -> None:
    # inside the project, not $TMPDIR: Docker Desktop does not share macOS /var/folders by default
    (root := Path(__file__).parents[1] / ".smoke").mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="run-", dir=root))
    (work / "bronze").mkdir()
    (work / "config").mkdir()
    api = EventsAPI()
    with serve({"events": api}) as base_url:
        port = base_url.rsplit(":", 1)[1]
        cfg = {
            "pipeline_name": "smoke_events",
            "dataset_name": "vendor",
            "bucket_url": "file:///bronze",
            "client": {
                "base_url": f"http://host.docker.internal:{port}",
                "paginator": {"type": "json_link", "next_url_path": "pagination.next"},
                "auth": {"type": "bearer", "token": {"env": "VENDOR_TOKEN"}},
            },
            "resources": [
                {
                    "name": "events_raw",
                    "path": "events",
                    "records_path": "data",
                    "id_path": "id",
                    "incremental": {
                        "cursor_path": "updatedAt",
                        "initial_value": "2026-01-01T00:00:00Z",
                        "start_param": "updated_since",
                    },
                }
            ],
        }
        (work / "config" / "smoke_events.json").write_text(json.dumps(cfg))
        api.require_auth = "Bearer smoke-token"
        container = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "-p",
                f"127.0.0.1:{PORT}:8080",
                "--add-host",
                "host.docker.internal:host-gateway",
                "-v",
                f"{work / 'bronze'}:/bronze",
                "-v",
                f"{work / 'config'}:/config:ro",
                "-e",
                "CONFIG_URI=file:///config",
                "-e",
                "VENDOR_TOKEN=smoke-token",
                "-e",
                "AWS_LAMBDA_FUNCTION_TIMEOUT=900",
                image,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        try:
            api.upsert(
                event(1, "2026-11-20T08:00:00Z"),
                event(2, "2026-11-20T09:00:00Z"),
                event(3, "2026-11-20T10:00:00Z"),
                event(4, "2026-11-20T11:00:00Z"),
            )
            for attempt in range(60):  # the emulator takes a moment to listen
                try:
                    _, r1 = invoke({"source_id": "smoke_events"})
                    break
                except OSError:
                    if attempt == 59:
                        raise
                    time.sleep(0.5)
            print("run 1:", r1)
            assert r1.get("rows") == {"events_raw": 4}, r1
            _, r2 = invoke({"source_id": "smoke_events"})
            print("run 2:", r2)
            assert r2["is_empty"], r2
            assert api.queries[-1]["updated_since"] == "2026-11-20T11:00:00+00:00"

            api.override[1] = b"<html>maintenance</html>"
            _, r3 = invoke({"source_id": "smoke_events"})
            print("run 3 (broken page):", {k: str(v)[:110] for k, v in r3.items()})
            assert r3.get("errorType") == "BrokenPageError", r3
            assert list((work / "bronze" / "_quarantine").rglob("*.body"))

            files = sorted((work / "bronze" / "vendor" / "events_raw").rglob("*.parquet"))
            rows = [r for f in files for r in pq.read_table(f).to_pylist()]
            assert sorted(r["record_id"] for r in rows) == ["EV-001", "EV-002", "EV-003", "EV-004"]
            served = b"\n".join(api.served).decode()
            assert all(r["payload"] in served for r in rows)  # verbatim slices
            print(f"OK: {len(rows)} verbatim rows in {files[0].relative_to(work)}")
        finally:
            logs = subprocess.run(["docker", "logs", container], capture_output=True, text=True)
            subprocess.run(["docker", "stop", container], capture_output=True)
            if sys.exc_info()[0] is not None:
                print(logs.stdout[-3000:], logs.stderr[-3000:])


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "api-bronze:test")
