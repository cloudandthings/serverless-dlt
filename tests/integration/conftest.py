"""Integration harness: real source engines in docker-compose (DESIGN-db.md section 10).

Every test here is marked `integration`, which the default pytest run deselects
(pyproject `addopts`), so `mise run test` never needs Docker. `mise run
test:integration` (or `pytest -m integration`) selects them. The session fixture
brings the compose stack up itself (idempotent) and skips everything when Docker
is not available.

Each test gets a fresh database on its engine, dropped afterwards, and runs the
handler against it into a `file://` bucket, the same seam as the SQLite suite.
Later engines join by adding a service to compose.yaml and an `Engine` below.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

COMPOSE_FILE = Path(__file__).with_name("compose.yaml")
PASSWORD = "bronze-it"


@dataclass(frozen=True)
class Engine:
    """One engine of the compose stack, and how to reach it from the host."""

    dialect: str
    service: str
    port: int
    admin_user: str
    admin_database: str
    sqlalchemy_driver: str

    def url(self, database: str) -> sa.URL:
        return sa.URL.create(
            self.sqlalchemy_driver,
            username=self.admin_user,
            password=PASSWORD,
            host="127.0.0.1",
            port=self.port,
            database=database,
        )

    def connection(self, database: str) -> dict:
        """The config's `connection` block for `database`."""
        return {
            "dialect": self.dialect,
            "host": "127.0.0.1",
            "port": self.port,
            "database": database,
            "username": self.admin_user,
            "password": PASSWORD,
        }


ENGINES = {
    "postgres": Engine(
        "postgres", "postgres", int(os.environ.get("DB_BRONZE_IT_PG_PORT", "55432")), "postgres", "postgres",
        "postgresql+psycopg",
    ),
    "mysql": Engine(
        "mysql", "mysql", int(os.environ.get("DB_BRONZE_IT_MYSQL_PORT", "53306")), "root", "mysql", "mysql+pymysql",
    ),
}


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="session")
def compose() -> None:
    """The engines, up and healthy. Skips the integration suite when Docker is not available."""
    if not _docker_available():
        pytest.skip("integration tests need Docker")
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait", *(e.service for e in ENGINES.values())],
        check=True,
        capture_output=True,
        timeout=600,
    )


class SourceDB:
    """A fresh database on one engine, plus helpers to seed it and read what landed in bronze."""

    def __init__(self, engine: Engine, database: str, bucket: Path) -> None:
        self.engine = engine
        self.dialect = engine.dialect
        self.database = database
        self.bucket = bucket
        self.sa = sa.create_engine(engine.url(database))
        self.metadata = sa.MetaData()

    def connection(self, **overrides) -> dict:
        return self.engine.connection(self.database) | overrides

    def create(self, define: Callable[[sa.MetaData], sa.Table]) -> sa.Table:
        table = define(self.metadata)
        table.create(self.sa)
        return table

    def insert(self, table: sa.Table, rows: list[dict]) -> None:
        with self.sa.begin() as conn:
            conn.execute(table.insert(), rows)

    def execute(self, *statements: str) -> None:
        with self.sa.begin() as conn:
            for statement in statements:
                conn.exec_driver_sql(statement)

    def files(self, table: str) -> list[Path]:
        root = self.bucket / "it" / table
        return sorted(root.rglob("*.parquet")) if root.exists() else []

    def landed(self, table: str) -> list[dict]:
        return [r for f in self.files(table) for r in pq.read_table(f).to_pylist()]

    def landed_ids(self, table: str, column: str) -> list:
        return sorted(r[column] for r in self.landed(table))


@pytest.fixture(params=list(ENGINES))
def source(request, compose, bucket: Path):
    """A fresh, empty database on each engine in turn; dropped when the test ends."""
    engine = ENGINES[request.param]
    database = f"it_{uuid.uuid4().hex[:12]}"
    admin = sa.create_engine(engine.url(engine.admin_database), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.exec_driver_sql(f"CREATE DATABASE {database}")
    db = SourceDB(engine, database, bucket)
    try:
        yield db
    finally:
        db.sa.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE {database}" + (" WITH (FORCE)" if engine.dialect == "postgres" else ""))
        admin.dispose()


@pytest.fixture(params=["pyarrow", "connectorx"])
def backend(request) -> str:
    """Every handler-level scenario runs on both backends."""
    return request.param


@pytest.fixture
def run(source: SourceDB, bucket: Path, tmp_path: Path, pipelines_dir: str, monkeypatch):
    """Invoke the handler with an inline config of the given tables against `source`."""
    from db_bronze.handler import handler

    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())

    def invoke(event: dict, *tables: dict, connection: dict | None = None, **settings) -> dict:
        cfg = {
            "pipeline_name": "it",
            "dataset_name": "it",
            "bucket_url": bucket.as_uri(),
            "connection": source.connection(**(connection or {})),
            "tables": list(tables),
            **settings,
        }
        (config_dir / f"{event['source_id']}.json").write_text(json.dumps(cfg))
        return handler(event, None)

    return invoke
