"""Integration harness: real source engines in docker-compose (DESIGN-db.md section 10).

Every test here is marked `integration`, which the default pytest run deselects
(pyproject `addopts`), so `mise run test` never needs Docker. `mise run
test:integration` (or `pytest -m integration`) selects them. The session fixture
brings the compose stack up itself (idempotent) and skips everything when Docker
is not available.

Each test gets a fresh database on its engine, dropped afterwards, and runs the
handler against it into a `file://` bucket, the same seam as the SQLite suite.
Later engines join by adding a service to compose.yaml and an `Engine` below.

Image mode: with `DB_BRONZE_IT_IMAGE=<tag>` (`mise run test:integration --image`)
the same tests invoke the handler inside that built image, through the Lambda
Runtime Interface Emulator, instead of in-process. The container joins the
compose network and reaches each engine by its service name; the buckets live
in a folder mounted at the same path inside the container. A failed invocation
raises an exception named after the Lambda `errorType`, so `fails()` checks
both modes alike. The session reports the peak memory of the run (spike S7).

Oracle (ticket 16) is reached by service name FREEPDB1, and a test's "database" is
a fresh user, i.e. a schema. The handler must run where oracledb can enter thick
mode and ConnectorX can load Instant Client (D10): in image mode, or in a process
with ORACLE_CLIENT_LIB_DIR set. Seeding from the host works in thin mode. The
Oracle service is in the compose profile `oracle`, so it starts only when used.

Inside mode: `mise run test:integration --inside` runs pytest itself inside the
image, on the compose network (`DB_BRONZE_IT_NETWORK=compose`), so the handler
and ConnectorX run in-process with Instant Client. Oracle's spike tests that call
ConnectorX directly need it.

    DB_BRONZE_IT_ENGINES   comma-separated engines to run (default: all); the others skip
"""

from __future__ import annotations

import json
import os
import platform
import re
import resource
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

# Before any Oracle connection: with ORACLE_CLIENT_LIB_DIR set (inside the image), the handler
# module puts oracledb into thick mode, process-wide, as a Lambda cold start does (D10).
import db_bronze.handler  # noqa: F401

COMPOSE_FILE = Path(__file__).with_name("compose.yaml")
PASSWORD = "bronze-it"
ENGINES_STARTED = False  # an integration test ran: report peak memory at the end
IMAGE = os.environ.get("DB_BRONZE_IT_IMAGE")  # image mode when set: run the handler inside this image
IN_NETWORK = os.environ.get("DB_BRONZE_IT_NETWORK") == "compose"  # inside mode: pytest runs on the compose network
# Each engine's port inside the compose network, where Engine.container_port is not set.
SERVER_PORTS = {"postgres": 5432, "mysql": 3306, "mssql": 1433, "oracle": 1521, "db2": 50000}


@dataclass(frozen=True)
class Engine:
    """One engine of the compose stack, and how to reach it from the host."""

    dialect: str
    service: str
    port: int
    admin_user: str
    admin_database: str
    sqlalchemy_driver: str
    container_port: int | None = None  # the port inside the compose network; None: SERVER_PORTS[dialect]

    def address(self, in_network: bool = IN_NETWORK) -> tuple[str, int]:
        """(host, port) from the host, or from inside the compose network."""
        if in_network:
            return self.service, self.container_port or SERVER_PORTS[self.dialect]
        return "127.0.0.1", self.port

    def unavailable(self) -> str | None:
        """Why this run cannot drive the engine, or None."""
        return None

    def url(self, database: str) -> sa.URL:
        host, port = self.address()
        return sa.URL.create(
            self.sqlalchemy_driver, username=self.admin_user, password=PASSWORD, host=host, port=port,
            database=database,
        )

    def admin(self) -> sa.Engine:
        return sa.create_engine(self.url(self.admin_database), isolation_level="AUTOCOMMIT")

    def connection(self, database: str, *, in_network: bool = IN_NETWORK) -> dict:
        """The config's `connection` block for `database`, from the host or from inside the compose network."""
        host, port = self.address(in_network)
        return {
            "dialect": self.dialect,
            "host": host,
            "port": port,
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


def connectorx_in_process(dialect: str) -> bool:
    """Whether this process can call ConnectorX on `dialect` directly (Oracle: only with Instant Client)."""
    return dialect != "oracle" or bool(os.environ.get("ORACLE_CLIENT_LIB_DIR"))


@dataclass(frozen=True)
class OracleEngine(Engine):
    """Oracle Free, reached by service name. A test's "database" is a fresh user, which is its own schema.

    The user may read the dictionary views and cancel SQL, so the spike tests can watch
    and stop their own queries (v$session, ALTER SYSTEM CANCEL SQL).
    """

    service_name: str = os.environ.get("DB_BRONZE_IT_ORACLE_SERVICE", "FREEPDB1")

    def unavailable(self) -> str | None:
        if IMAGE or connectorx_in_process("oracle"):
            return None
        return (
            "Oracle runs the handler only with thick-mode oracledb and Instant Client (D10): "
            "run `mise run test:integration --image` or `--inside`"
        )

    def url(self, database: str) -> sa.URL:
        host, port = self.address()
        return sa.URL.create(
            self.sqlalchemy_driver, username=database, password=PASSWORD, host=host, port=port,
            query={"service_name": self.service_name},
        )

    def admin(self) -> sa.Engine:
        return sa.create_engine(self.url(self.admin_user), isolation_level="AUTOCOMMIT")

    def connection(self, database: str, *, in_network: bool = IN_NETWORK) -> dict:
        host, port = self.address(in_network)
        return {
            "dialect": "oracle",
            "host": host,
            "port": port,
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
        with admin.connect() as conn:  # a ConnectorX read may still hold a session
            sessions = conn.exec_driver_sql(
                f"SELECT sid, serial# FROM v$session WHERE username = '{database.upper()}'"
            ).all()
            for sid, serial in sessions:
                conn.exec_driver_sql(f"ALTER SYSTEM KILL SESSION '{sid},{serial}' IMMEDIATE")
            conn.exec_driver_sql(f"DROP USER {database} CASCADE")


ENGINES = {
    "postgres": Engine(
        "postgres", "postgres", int(os.environ.get("DB_BRONZE_IT_PG_PORT", "55432")), "postgres", "postgres",
        "postgresql+psycopg",
    ),
    "mysql": Engine(
        "mysql", "mysql", int(os.environ.get("DB_BRONZE_IT_MYSQL_PORT", "53306")), "root", "mysql", "mysql+pymysql",
    ),
    "oracle": OracleEngine(
        "oracle", "oracle", int(os.environ.get("DB_BRONZE_IT_ORACLE_PORT", "51521")), "system", "", "oracle+oracledb",
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
    global ENGINES_STARTED
    if IN_NETWORK:  # brought up by whoever started this container
        ENGINES_STARTED = True
        return
    if not _docker_available():
        pytest.skip("integration tests need Docker")
    ENGINES_STARTED = True
    services = [e.service for name, e in ENGINES.items() if skip_reason(name) is None]
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

    def connectorx_url(self, **overrides) -> str:
        """The ConnectorX URL db_bronze builds for this database."""
        from db_bronze.config import ConnectionConfig
        from db_bronze.urls import connectorx_url

        return connectorx_url(ConnectionConfig(**self.connection(**overrides)))

    def create(self, define: Callable[[sa.MetaData], sa.Table]) -> sa.Table:
        table = define(self.metadata)
        table.create(self.sa)
        return table

    def insert(self, table: sa.Table, rows: list[dict]) -> None:
        with self.sa.begin() as conn:
            conn.execute(table.insert(), rows)

    def insert_into(self, name: str, rows: list[dict]) -> None:
        """Insert into a table by name, with bound values (portable), e.g. after DDL the metadata does not know."""
        table = sa.table(name, *(sa.column(c) for c in rows[0]))
        with self.sa.begin() as conn:
            conn.execute(table.insert(), rows)

    def execute(self, *statements: str) -> None:
        with self.sa.begin() as conn:
            for statement in statements:
                conn.exec_driver_sql(statement)

    def transactions(self, *rows: tuple, nullable_cursor: bool = False) -> sa.Table:
        """`Transactions` as in the SQLite suite (created on first use), plus `rows` of (id, UpdatedAt, branch, cents).

        Mixed-case and spaced names on purpose: they must land exactly as in the source.
        """
        if "Transactions" not in self.metadata.tables:
            self.create(
                lambda metadata: sa.Table(
                    "Transactions",
                    metadata,
                    sa.Column("TransactionId", sa.Integer, primary_key=True, autoincrement=False),
                    sa.Column("UpdatedAt", sa.DateTime, nullable=nullable_cursor),
                    sa.Column("Branch Code", sa.String(10)),
                    sa.Column("amountCents", sa.BigInteger),
                )
            )
        table = self.metadata.tables["Transactions"]
        if rows:
            self.insert(table, [
                {"TransactionId": i, "UpdatedAt": ts and datetime.fromisoformat(ts), "Branch Code": b, "amountCents": a}
                for i, ts, b, a in rows
            ])
        return table

    def quote(self, name: str) -> str:
        """`name` as an identifier in this engine's SQL, for `where` and `query` in configs."""
        return self.sa.dialect.identifier_preparer.quote(name)

    def files(self, table: str) -> list[Path]:
        root = self.bucket / "it" / table
        return sorted(root.rglob("*.parquet")) if root.exists() else []

    def landed(self, table: str, mode: str | None = None, load_ids: list[str] | None = None) -> list[dict]:
        """Rows landed for `table`, optionally only under `mode=` or only from the given loads."""
        files = [
            f for f in self.files(table)
            if (mode is None or f"/mode={mode}/" in f.as_posix())
            and (load_ids is None or f.name.rsplit(".", 2)[0] in load_ids)
        ]
        return [r for f in files for r in pq.read_table(f).to_pylist()]

    def landed_ids(self, table: str, column: str, **only) -> list:
        return sorted(r[column] for r in self.landed(table, **only))

    def partitions(self, table: str) -> set[str]:
        return {f.parent.relative_to(self.bucket / "it" / table).as_posix() for f in self.files(table)}

    def manifests(self) -> dict[str, dict]:
        root = self.bucket / "it" / "_manifests"
        return {f.relative_to(root).as_posix(): json.loads(f.read_text()) for f in sorted(root.rglob("*.json"))}

    def nothing_landed(self) -> bool:
        return not any(p.is_file() for p in self.bucket.rglob("*"))


def today_partition(mode: str) -> str:
    today = datetime.now(UTC)
    return f"mode={mode}/year={today:%Y}/month={today:%m}/day={today:%d}"


@contextmanager
def fails(error_type: str, match: str | None = None) -> Iterator[pytest.ExceptionInfo]:
    """The invocation fails with Lambda `errorType` `error_type` (the raised class name), its message matching."""
    with pytest.raises(BaseException) as failure:
        yield failure
    assert type(failure.value).__name__ == error_type, f"{type(failure.value).__name__}: {failure.value}"
    if match is not None:
        assert re.search(match, str(failure.value)), str(failure.value)


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


class LambdaImage:
    """The db_bronze image under the Runtime Interface Emulator, on the compose network, for the whole session."""

    def __init__(self, image: str) -> None:
        self.image = image
        # inside the project, not $TMPDIR: Docker Desktop does not share macOS /var/folders by default
        self.shared = Path(__file__).parents[2] / ".smoke" / f"it-{uuid.uuid4().hex[:8]}"
        self.shared.mkdir(parents=True)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        service = next(e.service for name, e in ENGINES.items() if skip_reason(name) is None)
        [container] = _compose("ps", "-q", service).split()
        network = subprocess.run(
            ["docker", "inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", container],
            check=True, capture_output=True, text=True,
        ).stdout.split()[0]
        self.ready = False  # the emulator has answered once
        self.container = subprocess.run(
            ["docker", "run", "-d", "--platform", "linux/amd64", "--network", network,
             "-p", f"127.0.0.1:{self.port}:8080", "-v", f"{self.shared}:{self.shared}",
             "-e", "AWS_LAMBDA_FUNCTION_TIMEOUT=900", *_native_emulator(self.shared.parent), image],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    def invoke(self, event: dict) -> dict:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/2015-03-31/functions/function/invocations",
            data=json.dumps(event).encode(),
            method="POST",
        )
        for attempt in range(240):  # the emulator takes a moment to listen, longer under emulation
            try:
                with urllib.request.urlopen(request, timeout=900) as response:
                    result = json.loads(response.read() or b"null")
                break
            except OSError as e:
                if not self.running():
                    logs = subprocess.run(["docker", "logs", self.container], capture_output=True, text=True)
                    lines = (logs.stdout + logs.stderr).splitlines()
                    fatal = [line for line in lines if line.startswith(("fatal error", "panic:", "SIG"))]
                    invokes = sum("INVOKE RTDONE" in line for line in lines)
                    raise RuntimeError(f"the Lambda container died after {invokes} invocations: {fatal}") from e
                if self.ready or attempt == 239:  # only retry while the emulator starts listening
                    raise
                time.sleep(0.5)
        self.ready = True
        if isinstance(result, dict) and "errorType" in result and "errorMessage" in result:
            raise type(result["errorType"], (Exception,), {})(result["errorMessage"])
        return result

    def running(self) -> bool:
        state = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", self.container],
                               capture_output=True, text=True)
        return state.stdout.strip() == "true"

    def peak_memory(self) -> int | None:
        """The container's peak memory (cgroup v2 memory.peak, or v1), in bytes."""
        for path in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
            out = subprocess.run(["docker", "exec", self.container, "cat", path], capture_output=True, text=True)
            if out.returncode == 0 and out.stdout.strip().isdigit():
                return int(out.stdout.strip())
        return None

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


def _native_emulator(cache: Path) -> list[str]:
    """`docker run` arguments that swap in the host's own build of the emulator on an arm64 host.

    The image is amd64 only, so on an arm64 host it runs under QEMU. The emulator is a Go
    binary, and under QEMU the Go runtime crashes every few dozen invocations ("found pointer
    to free object", "unexpected signal"), which a whole suite cannot survive. An arm64 binary
    runs natively even inside an amd64 container, so the emulator comes from the arm64 variant
    of the same AWS base image (docker/Dockerfile.db); only the handler runs under QEMU.
    """
    if platform.machine().lower() not in ("arm64", "aarch64"):
        return []
    rie = cache / "rie" / "aws-lambda-rie-arm64"
    if not rie.exists():
        rie.parent.mkdir(parents=True, exist_ok=True)
        base = "public.ecr.aws/lambda/python:3.12"
        container = subprocess.run(["docker", "create", "--platform", "linux/arm64", base],
                                   check=True, capture_output=True, text=True).stdout.strip()
        try:
            subprocess.run(["docker", "cp", f"{container}:/usr/local/bin/aws-lambda-rie", str(rie)],
                           check=True, capture_output=True)
        finally:
            subprocess.run(["docker", "rm", container], capture_output=True)
    return ["-v", f"{rie}:/usr/local/bin/aws-lambda-rie:ro"]


def _compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args], check=True, capture_output=True, text=True
    ).stdout


PEAK_MEMORY: dict[str, int] = {}  # reported at the end of the session (spike S7)


@pytest.fixture(scope="session")
def lambda_image(compose) -> Iterator[LambdaImage]:
    image = LambdaImage(IMAGE)
    try:
        yield image
    finally:
        if (peak := image.peak_memory()) is not None:
            PEAK_MEMORY[f"image {IMAGE} (container peak)"] = peak
        image.stop()


@pytest.fixture
def bucket(request, tmp_path: Path) -> Path:
    """In image mode the bucket lives in the folder shared with the container, at the same path."""
    root = request.getfixturevalue("lambda_image").shared / uuid.uuid4().hex[:12] if IMAGE else tmp_path
    path = root / "bronze"
    path.mkdir(parents=True)
    return path


def pytest_terminal_summary(terminalreporter) -> None:
    if not ENGINES_STARTED:
        return
    # macOS reports ru_maxrss in bytes, Linux in KiB
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    PEAK_MEMORY.setdefault("host pytest process (peak RSS)", rss)
    terminalreporter.section("db_bronze peak memory (spike S7)")
    for what, size in PEAK_MEMORY.items():
        terminalreporter.write_line(f"{what}: {size / 2**20:.0f} MiB")


@pytest.fixture
def run(request, source: SourceDB, bucket: Path, tmp_path: Path, pipelines_dir: str, monkeypatch):
    """Invoke the handler with a config of the given tables against `source`: in-process, or in the image."""
    if IMAGE:
        image = request.getfixturevalue("lambda_image")

        def invoke_image(event: dict, *tables: dict, connection: dict | None = None, context=None, **settings):
            # The emulator always passes a context with the function timeout's remaining time.
            cfg = {
                "pipeline_name": "it",
                "dataset_name": "it",
                "bucket_url": bucket.as_uri(),
                "connection": source.engine.connection(source.database, in_network=True) | (connection or {}),
                "tables": list(tables),
                **settings,
            }
            return image.invoke({**event, "config": cfg})

        return invoke_image

    from db_bronze.handler import handler

    monkeypatch.setenv("DLT_DATA_DIR", pipelines_dir)
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    monkeypatch.setenv("CONFIG_URI", config_dir.as_uri())

    def invoke(event: dict, *tables: dict, connection: dict | None = None, context=None, **settings) -> dict:
        """`context`: a Lambda context, for the time budget it derives; `settings`: top-level config keys."""
        cfg = {
            "pipeline_name": "it",
            "dataset_name": "it",
            "bucket_url": bucket.as_uri(),
            "connection": source.connection(**(connection or {})),
            "tables": list(tables),
            **settings,
        }
        (config_dir / f"{event['source_id']}.json").write_text(json.dumps(cfg))
        return handler(event, context)

    return invoke
