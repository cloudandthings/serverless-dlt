"""Integration harness: real source engines in docker-compose (DESIGN-db.md section 10).

Every test here is marked `integration`, which the default pytest run deselects
(pyproject `addopts`), so `mise run test` never needs Docker. `mise run
test:integration` (or `pytest -m integration`) selects them. The session fixture
brings the compose stack up itself (idempotent) and skips everything when Docker
is not available.

Each test gets a fresh database on its engine (on Oracle a fresh user, i.e. a
schema), dropped afterwards, and runs the handler against it into a `file://`
bucket, the same seam as the SQLite suite. Later engines join by adding a service
to compose.yaml and an `Engine` below.

Environment:
    DB_BRONZE_IT_ENGINES   comma-separated engines to run (default: every engine this
                           process can drive). The others' tests skip.
    DB_BRONZE_IT_NETWORK   "compose": the tests run in a container on the compose network
                           (inside the db-bronze image, `mise run test:integration-image`),
                           so engines are reached by service name and the stack is not
                           brought up from here.

Oracle runs only where oracledb can enter thick mode (ORACLE_CLIENT_LIB_DIR set, as in
the image; D10), because ConnectorX needs the same Instant Client.
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

# Before any Oracle connection: with ORACLE_CLIENT_LIB_DIR set, the handler module puts oracledb
# into thick mode, process-wide, exactly as a Lambda cold start does (D10).
import db_bronze.handler  # noqa: F401

COMPOSE_FILE = Path(__file__).with_name("compose.yaml")
PASSWORD = "bronze-it"


IN_NETWORK = os.environ.get("DB_BRONZE_IT_NETWORK") == "compose"


@dataclass(frozen=True)
class Engine:
    """One engine of the compose stack, and how to reach it from the host (or from the compose network)."""

    dialect: str
    service: str
    host_port: int
    container_port: int
    admin_user: str
    admin_database: str
    sqlalchemy_driver: str

    @property
    def host(self) -> str:
        return self.service if IN_NETWORK else "127.0.0.1"

    @property
    def port(self) -> int:
        return self.container_port if IN_NETWORK else self.host_port

    def unavailable(self) -> str | None:
        """Why this process cannot drive the engine, or None."""
        return None

    def url(self, database: str) -> sa.URL:
        return sa.URL.create(
            self.sqlalchemy_driver,
            username=self.admin_user,
            password=PASSWORD,
            host=self.host,
            port=self.port,
            database=database,
        )

    def admin(self) -> sa.Engine:
        return sa.create_engine(self.url(self.admin_database), isolation_level="AUTOCOMMIT")

    def connection(self, database: str) -> dict:
        """The config's `connection` block for `database`."""
        return {
            "dialect": self.dialect,
            "host": self.host,
            "port": self.port,
            "database": database,
            "username": self.admin_user,
            "password": PASSWORD,
        }

    def create_database(self, admin: sa.Engine, database: str) -> None:
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {database}")

    def drop_database(self, admin: sa.Engine, database: str) -> None:
        with admin.connect() as conn:
            if self.dialect == "postgres":  # a dropped ConnectorX stream may still hold a session
                conn.exec_driver_sql(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    f"WHERE datname = '{database}' AND pid <> pg_backend_pid()"
                )
            conn.exec_driver_sql(f"DROP DATABASE {database}")


@dataclass(frozen=True)
class OracleEngine(Engine):
    """Oracle Free, reached by service name. A test's "database" is a fresh user, which is its own schema.

    The user may read the dictionary views and cancel SQL, so the spike tests can watch
    and stop their own queries (v$session, ALTER SYSTEM CANCEL SQL).
    """

    service_name: str = "FREEPDB1"

    def unavailable(self) -> str | None:
        if not os.environ.get("ORACLE_CLIENT_LIB_DIR"):
            return (
                "Oracle needs thick-mode oracledb and Instant Client for ConnectorX (D10): "
                "run it inside the image with `mise run test:integration-image`"
            )
        return None

    def _url(self, user: str) -> sa.URL:
        return sa.URL.create(
            self.sqlalchemy_driver, username=user, password=PASSWORD, host=self.host, port=self.port,
            query={"service_name": self.service_name},
        )

    def url(self, database: str) -> sa.URL:
        return self._url(database)

    def admin(self) -> sa.Engine:
        return sa.create_engine(self._url(self.admin_user), isolation_level="AUTOCOMMIT")

    def connection(self, database: str) -> dict:
        return {
            "dialect": "oracle",
            "host": self.host,
            "port": self.port,
            "service_name": self.service_name,
            "username": database,
            "password": PASSWORD,
        }

    def create_database(self, admin: sa.Engine, database: str) -> None:
        with admin.connect() as conn:
            conn.exec_driver_sql(
                f'CREATE USER {database} IDENTIFIED BY "{PASSWORD}" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users'
            )
            conn.exec_driver_sql(
                "GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, "
                f"SELECT ANY DICTIONARY, ALTER SYSTEM TO {database}"
            )

    def drop_database(self, admin: sa.Engine, database: str) -> None:
        with admin.connect() as conn:  # a dropped ConnectorX stream may still hold a session
            sessions = conn.exec_driver_sql(
                f"SELECT sid, serial# FROM v$session WHERE username = '{database.upper()}'"
            ).all()
            for sid, serial in sessions:
                conn.exec_driver_sql(f"ALTER SYSTEM KILL SESSION '{sid},{serial}' IMMEDIATE")
            conn.exec_driver_sql(f"DROP USER {database} CASCADE")


ENGINES = {
    "postgres": Engine(
        "postgres", "postgres", int(os.environ.get("DB_BRONZE_IT_PG_PORT", "55432")), 5432, "postgres", "postgres",
        "postgresql+psycopg",
    ),
    "mysql": Engine(
        "mysql", "mysql", int(os.environ.get("DB_BRONZE_IT_MYSQL_PORT", "53306")), 3306, "root", "mysql",
        "mysql+pymysql",
    ),
    "oracle": OracleEngine(
        "oracle", "oracle", int(os.environ.get("DB_BRONZE_IT_ORACLE_PORT", "51521")), 1521, "system", "",
        "oracle+oracledb",
    ),
}


def skip_reason(name: str) -> str | None:
    """Why the engine's tests skip in this run, or None when they run."""
    selected = os.environ.get("DB_BRONZE_IT_ENGINES")
    if selected and name not in {s.strip() for s in selected.split(",")}:
        return f"{name} not in DB_BRONZE_IT_ENGINES={selected}"
    return ENGINES[name].unavailable()


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="session")
def compose() -> None:
    """The engines, up and healthy. Skips the integration suite when Docker is not available."""
    if IN_NETWORK:  # brought up by whoever started this container
        return
    if not _docker_available():
        pytest.skip("integration tests need Docker")
    services = [e.service for name, e in ENGINES.items() if skip_reason(name) is None]
    if not services:
        return
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait", *services],
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
def source(request, bucket: Path):
    """A fresh, empty database on each engine in turn; dropped when the test ends."""
    if reason := skip_reason(request.param):
        pytest.skip(reason)
    request.getfixturevalue("compose")
    engine = ENGINES[request.param]
    database = f"it_{uuid.uuid4().hex[:12]}"
    admin = engine.admin()
    engine.create_database(admin, database)
    db = SourceDB(engine, database, bucket)
    try:
        yield db
    finally:
        db.sa.dispose()
        engine.drop_database(admin, database)
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
