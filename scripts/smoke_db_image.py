"""Smoke-test the built db_bronze image (DESIGN-db.md section 10, image layer).

1. Proves the drivers inside the image: pyodbc lists ODBC Driver 18, oracledb is in thick
   mode after the handler import, ibm_db imports, and ConnectorX can load libclntsh in the
   same process (spike S6).
2. Runs the image under the Lambda Runtime Interface Emulator with a SQLite source and the
   config mounted as local folders, invokes the function like Lambda would on both backends,
   and checks that Parquet lands.

    mise run docker:smoke --db
    uv run python scripts/smoke_db_image.py db-bronze:test
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

PORT = 9124
PLATFORM = "linux/amd64"

# Runs inside the image, from the task root, the way the Lambda runtime imports the handler.
DRIVER_PROBE = r"""
import json
import db_bronze.handler  # enters oracledb thick mode from ORACLE_CLIENT_LIB_DIR
import connectorx as cx, ibm_db, ibm_db_sa, oracledb, psycopg, pymysql, pyodbc

# ConnectorX loads libclntsh on its first Oracle connection. With nothing listening the error
# must come from the network (ORA-), not from loading the library (DPI-1047).
try:
    cx.read_sql("oracle://smoke:smoke@127.0.0.1:1521/nosuch", "SELECT 1 FROM dual", return_type="arrow")
    cx_oracle = "connected?"
except BaseException as e:  # PanicException derives from BaseException
    cx_oracle = str(e)
print(json.dumps({
    "odbc_drivers": pyodbc.drivers(),
    "oracledb_thin": oracledb.is_thin_mode(),
    "oracle_client": ".".join(map(str, oracledb.clientversion())),
    "ibm_db": ibm_db.__version__ if hasattr(ibm_db, "__version__") else "imported",
    "psycopg_impl": psycopg.pq.__impl__,
    "connectorx_oracle": cx_oracle,
}))
"""


def probe_drivers(image: str) -> None:
    out = subprocess.run(
        ["docker", "run", "--rm", "--platform", PLATFORM, "-w", "/var/task", "--entrypoint", "python3", image,
         "-c", DRIVER_PROBE],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"driver probe failed:\n{out.stderr[-3000:]}")
    found = json.loads(out.stdout.strip().splitlines()[-1])
    print("drivers:", json.dumps(found, indent=2))
    assert "ODBC Driver 18 for SQL Server" in found["odbc_drivers"], found
    assert found["oracledb_thin"] is False, found
    assert found["psycopg_impl"] == "binary", found
    assert "DPI-1047" not in found["connectorx_oracle"], found
    assert "ORA-" in found["connectorx_oracle"], found


def invoke(payload: dict) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/2015-03-31/functions/function/invocations",
        data=json.dumps(payload).encode(),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.loads(r.read() or b"null")


def first_invoke(payload: dict) -> dict:
    for attempt in range(120):  # the emulator takes a moment to listen, longer under emulation
        try:
            return invoke(payload)
        except OSError:
            if attempt == 119:
                raise
            time.sleep(0.5)
    raise AssertionError("unreachable")


def seed(path: Path) -> None:
    with sqlite3.connect(path) as con:
        con.execute(
            'CREATE TABLE "Transactions" ("TransactionId" INTEGER PRIMARY KEY, "UpdatedAt" DATETIME NOT NULL,'
            ' "Branch Code" TEXT, "amountCents" INTEGER)'
        )
        con.executemany(
            'INSERT INTO "Transactions" VALUES (?, ?, ?, ?)',
            [(1, "2026-11-20 08:00:00", "JHB", 100), (2, "2026-11-20 09:00:00", "CPT", 200),
             (3, "2026-11-20 10:00:00", "DBN", 300)],
        )


def run_handler(image: str) -> None:
    # inside the project, not $TMPDIR: Docker Desktop does not share macOS /var/folders by default
    (root := Path(__file__).parents[1] / ".smoke").mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="db-run-", dir=root))
    for d in ("bronze", "config", "data"):
        (work / d).mkdir()
    seed(work / "data" / "source.sqlite")
    table = {
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
    }
    cfg = {
        "pipeline_name": "smoke_core",
        "dataset_name": "core_banking",
        "bucket_url": "file:///bronze",
        "connection": {"dialect": "sqlite", "path": "/data/source.sqlite"},
        "tables": [
            {"name": "transactions_cx", "backend": "connectorx", **table},
            {"name": "transactions_pa", "backend": "pyarrow", **table},
        ],
    }
    (work / "config" / "smoke_core.json").write_text(json.dumps(cfg))
    container = subprocess.run(
        ["docker", "run", "-d", "--platform", PLATFORM, "-p", f"127.0.0.1:{PORT}:8080",
         "-v", f"{work / 'bronze'}:/bronze", "-v", f"{work / 'config'}:/config:ro", "-v", f"{work / 'data'}:/data",
         "-e", "CONFIG_URI=file:///config", "-e", "AWS_LAMBDA_FUNCTION_TIMEOUT=900",
         # The emulator is a Go binary; under QEMU (amd64 on an arm64 host) Go's async preemption
         # signals intermittently crash it ("fatal error: wirep: already in go").
         "-e", "GODEBUG=asyncpreemptoff=1", image],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    try:
        r1 = first_invoke({"source_id": "smoke_core"})
        print("run 1:", r1)
        assert r1.get("rows") == {"transactions_cx": 3, "transactions_pa": 3}, r1
        r2 = invoke({"source_id": "smoke_core"})
        print("run 2:", r2)
        assert r2.get("is_empty") is True, r2

        for name in ("transactions_cx", "transactions_pa"):
            files = sorted((work / "bronze" / "core_banking" / name).rglob("*.parquet"))
            rows = [r for f in files for r in pq.read_table(f).to_pylist()]
            assert sorted(r["TransactionId"] for r in rows) == [1, 2, 3], rows
            assert all(r["_extracted_at"] is not None for r in rows), rows
            print(f"OK: {len(rows)} rows in {files[0].relative_to(work)}")
    finally:
        # no --rm on the container: a container that died early must still have its logs here
        state = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}} exit={{.State.ExitCode}} "
                                "oom={{.State.OOMKilled}}", container], capture_output=True, text=True)
        logs = subprocess.run(["docker", "logs", container], capture_output=True, text=True)
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        if sys.exc_info()[0] is not None:
            print("container:", state.stdout.strip())
            print(logs.stdout[-3000:], logs.stderr[-3000:])


def main(image: str) -> None:
    probe_drivers(image)
    run_handler(image)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "db-bronze:test")
