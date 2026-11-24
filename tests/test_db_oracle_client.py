"""db_bronze: Oracle always runs thick (DESIGN-db.md D10), initialised when the handler module is imported.

The image sets ORACLE_CLIENT_LIB_DIR to its Instant Client. Without it (local runs, these
tests) oracledb stays in thin mode. Each case runs in a fresh interpreter because thick mode
is process-wide and cannot be undone. The image smoke test (`docker:smoke --db`) proves the
real thing: thick mode with Instant Client inside the built image.
"""

from __future__ import annotations

import os
import subprocess
import sys

PROBE = "import db_bronze.handler, oracledb; print('thin' if oracledb.is_thin_mode() else 'thick')"


def import_handler(**env: str) -> subprocess.CompletedProcess:
    base = {k: v for k, v in os.environ.items() if k != "ORACLE_CLIENT_LIB_DIR"}
    return subprocess.run([sys.executable, "-c", PROBE], env=base | env, capture_output=True, text=True)


def test_without_an_instant_client_dir_oracledb_stays_thin():
    probe = import_handler()
    assert probe.returncode == 0, probe.stderr
    assert probe.stdout.strip() == "thin"


def test_an_instant_client_dir_that_cannot_load_fails_the_import(tmp_path):
    # a broken image must fail at cold start, never fall back to thin mode
    probe = import_handler(ORACLE_CLIENT_LIB_DIR=str(tmp_path))
    assert probe.returncode != 0
    assert "DPI-1047" in probe.stderr  # oracledb tried to load the Oracle Client library from that dir
