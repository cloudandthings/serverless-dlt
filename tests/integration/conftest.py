"""Integration harness: real source engines in docker-compose (DESIGN-db.md section 10).

Every test here is marked `integration`, which the default pytest run deselects
(pyproject `addopts`), so `mise run test` never needs Docker. `mise run
test:integration` (or `pytest -m integration`) selects them. The session fixture
brings the compose stack up itself (idempotent) and skips everything when Docker
is not available.

`DB_BRONZE_IT_ENGINES` (comma-separated, e.g. `mssql`) limits the run to some
engines, and `DB_BRONZE_IT_HOST` points at engines that are not on 127.0.0.1
(for a run inside the image, on the compose network).

Each test gets a fresh database on its engine, dropped afterwards, and runs the
handler against it into a `file://` bucket, the same seam as the SQLite suite.
Later engines join by adding a service to compose.yaml and an `Engine` below.
"""

from __future__ import annotations

import builtins
import json
import os
import subprocess
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

COMPOSE_FILE = Path(__file__).with_name("compose.yaml")
PASSWORD = "bronze-it"
HOST = os.environ.get("DB_BRONZE_IT_HOST", "127.0.0.1")


@dataclass(frozen=True)
class Engine:
    """One engine of the compose stack, and how to reach it from the host."""

    dialect: str
    service: str
    port: int
    admin_user: str
    admin_database: str
    sqlalchemy_driver: str
    password: str = PASSWORD
    driver_options: dict = field(default_factory=dict)  # the config's
    url_query: dict = field(default_factory=dict)  # the harness's own URL only (driver_options are added)
    host: str = HOST

    def url(self, database: str) -> sa.URL:
        return sa.URL.create(
            self.sqlalchemy_driver,
            username=self.admin_user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=database,
            query=self.url_query | self.driver_options,
        )

    def connection(self, database: str) -> dict:
        """The config's `connection` block for `database`."""
        return {
            "dialect": self.dialect,
            "host": self.host,
            "port": self.port,
            "database": database,
            "username": self.admin_user,
            "password": self.password,
            **({"driver_options": dict(self.driver_options)} if self.driver_options else {}),
        }


ENGINES = {
    "postgres": Engine(
        "postgres", "postgres", int(os.environ.get("DB_BRONZE_IT_PG_PORT", "55432")), "postgres", "postgres",
        "postgresql+psycopg",
    ),
    "mysql": Engine(
        "mysql", "mysql", int(os.environ.get("DB_BRONZE_IT_MYSQL_PORT", "53306")), "root", "mysql", "mysql+pymysql",
    ),
    "mssql": Engine(
        "mssql", "mssql", int(os.environ.get("DB_BRONZE_IT_MSSQL_PORT", "51433")), "sa", "master", "mssql+pyodbc",
        password="Bronze-it-1",
        # The container's certificate is self-signed; ODBC Driver 18 encrypts by default.
        driver_options={"TrustServerCertificate": "yes"},
        url_query={"driver": "ODBC Driver 18 for SQL Server"},
    ),
}
if os.environ.get("DB_BRONZE_IT_ENGINES"):
    ENGINES = {name: ENGINES[name] for name in os.environ["DB_BRONZE_IT_ENGINES"].split(",")}


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
        timeout=1800,  # SQL Server under x86_64 emulation starts slowly
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
            if engine.dialect == "postgres":  # a dropped ConnectorX stream may still hold a session
                conn.exec_driver_sql(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    f"WHERE datname = '{database}' AND pid <> pg_backend_pid()"
                )
            if engine.dialect == "mssql":  # the same, for SQL Server
                conn.exec_driver_sql(f"ALTER DATABASE {database} SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            conn.exec_driver_sql(f"DROP DATABASE {database}")
        admin.dispose()


@pytest.fixture(params=["pyarrow", "connectorx"])
def backend(request) -> str:
    """Every handler-level scenario runs on both backends."""
    return request.param


# Set by `mise run test:integration --image`: the suite runs inside the built image, and every
# handler run goes through the Lambda Runtime Interface Emulator at this URL, as Lambda would invoke it.
RIE_URL = os.environ.get("DB_BRONZE_IT_RIE")
in_image_only_in_process = pytest.mark.skipif(
    RIE_URL is not None, reason="patches the handler's process, which the emulator runs separately"
)


def invoke_rie(event: dict) -> dict:
    """POST `event` to the emulator; a function error is raised again as the db_bronze type it names."""
    from db_bronze import config, errors, preflight

    request = urllib.request.Request(
        f"{RIE_URL}/2015-03-31/functions/function/invocations", data=json.dumps(event).encode(), method="POST"
    )
    with urllib.request.urlopen(request, timeout=1800) as response:
        result = json.loads(response.read() or b"null")
    if isinstance(result, dict) and "errorType" in result:
        name = result["errorType"]
        kind = next((getattr(m, name) for m in (errors, config, preflight, builtins) if hasattr(m, name)), None)
        if not (isinstance(kind, type) and issubclass(kind, Exception)):
            raise AssertionError(f"function error: {result}")
        error = kind.__new__(kind)
        error.args = (result.get("errorMessage", ""),)
        raise error
    return result


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
        if RIE_URL is not None:  # the emulator's process has its own environment: the config goes inline
            return invoke_rie({k: v for k, v in event.items() if k != "source_id"} | {"config": cfg})
        (config_dir / f"{event['source_id']}.json").write_text(json.dumps(cfg))
        return handler(event, None)

    return invoke
