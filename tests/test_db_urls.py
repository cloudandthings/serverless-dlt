"""db_bronze: SQLAlchemy and ConnectorX URLs built from the discrete connection fields (DESIGN-db.md 5.1).

Offline: URLs are built and, for MSSQL, turned into the ODBC connection string, without any driver.
"""

from __future__ import annotations

import json
import sys
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc

from db_bronze import secrets as db_secrets
from db_bronze.config import ConfigError, ConnectionConfig, SourceConfig
from db_bronze.urls import connectorx_sql, connectorx_url, engine_isolation_level, sqlalchemy_url

USER = {"username": "ingest", "password": "s3cret"}


def conn(**fields) -> ConnectionConfig:
    return ConnectionConfig(**fields)


SQLALCHEMY = [
    (
        {"dialect": "mssql", "host": "sql01", "port": 1433, "database": "Core"} | USER,
        "mssql+pyodbc://ingest:s3cret@sql01:1433/Core?driver=ODBC+Driver+18+for+SQL+Server",
    ),
    (
        {"dialect": "postgres", "host": "pg01", "port": 5432, "database": "core"} | USER,
        "postgresql+psycopg://ingest:s3cret@pg01:5432/core",
    ),
    (
        {"dialect": "mysql", "host": "my01", "port": 3306, "database": "core"} | USER,
        "mysql+pymysql://ingest:s3cret@my01:3306/core",
    ),
    (
        {"dialect": "oracle", "host": "ora01", "port": 1521, "service_name": "CORE"} | USER,
        "oracle+oracledb://ingest:s3cret@ora01:1521?service_name=CORE",
    ),
    (
        {"dialect": "db2", "host": "db201", "port": 50000, "database": "CORE"} | USER,
        "db2+ibm_db://ingest:s3cret@db201:50000/CORE",
    ),
    ({"dialect": "sqlite", "path": "/data/core.sqlite"}, "sqlite:////data/core.sqlite"),
    ({"dialect": "snowflake", "host": "acct", "database": "core"} | USER, "snowflake://ingest:s3cret@acct/core"),
]


@pytest.mark.parametrize("fields,expected", SQLALCHEMY, ids=[f["dialect"] for f, _ in SQLALCHEMY])
def test_sqlalchemy_url(fields, expected):
    assert sqlalchemy_url(conn(**fields)).render_as_string(hide_password=False) == expected


CONNECTORX = [
    (
        {"dialect": "mssql", "host": "sql01", "port": 1433, "database": "Core"} | USER,
        "mssql://ingest:s3cret@sql01:1433/Core?encrypt=true",
    ),
    (
        {"dialect": "postgres", "host": "pg01", "port": 5432, "database": "core"} | USER,
        "postgresql://ingest:s3cret@pg01:5432/core",
    ),
    ({"dialect": "mysql", "host": "my01", "port": 3306, "database": "core"} | USER, "mysql://ingest:s3cret@my01:3306/core"),
    (
        {"dialect": "oracle", "host": "ora01", "port": 1521, "service_name": "CORE"} | USER,
        "oracle://ingest:s3cret@ora01:1521/CORE",
    ),
    ({"dialect": "oracle", "host": "ora01", "service_name": "CORE"} | USER, "oracle://ingest:s3cret@ora01/CORE"),
    ({"dialect": "sqlite", "path": "/data/core.sqlite"}, "sqlite:///data/core.sqlite"),
    ({"dialect": "sqlite", "path": "/data/core banking.sqlite"}, "sqlite:///data/core%20banking.sqlite"),
]


@pytest.mark.parametrize("fields,expected", CONNECTORX, ids=[f["dialect"] for f, _ in CONNECTORX])
def test_connectorx_url(fields, expected):
    assert connectorx_url(conn(**fields)) == expected


SPECIAL = "p@ss:w/rd%?#&= {;}"
SERVER = {
    "mssql": {"dialect": "mssql", "host": "sql01", "port": 1433, "database": "Core"},
    "postgres": {"dialect": "postgres", "host": "pg01", "port": 5432, "database": "core"},
    "mysql": {"dialect": "mysql", "host": "my01", "port": 3306, "database": "core"},
    "oracle": {"dialect": "oracle", "host": "ora01", "port": 1521, "service_name": "CORE"},
    "db2": {"dialect": "db2", "host": "db201", "port": 50000, "database": "CORE"},
}


@pytest.mark.parametrize("dialect", SERVER)
def test_special_characters_survive_the_sqlalchemy_url(dialect):
    url = sqlalchemy_url(conn(**SERVER[dialect], username="dom\\ingest@corp", password=SPECIAL))
    parsed = sa.make_url(url.render_as_string(hide_password=False))
    assert (parsed.username, parsed.password) == ("dom\\ingest@corp", SPECIAL)


@pytest.mark.parametrize("dialect", ["mssql", "postgres", "mysql", "oracle"])
def test_special_characters_survive_the_connectorx_url(dialect):
    url = urlsplit(connectorx_url(conn(**SERVER[dialect], username="dom\\ingest@corp", password=SPECIAL)))
    assert (unquote(url.username), unquote(url.password)) == ("dom\\ingest@corp", SPECIAL)
    assert url.hostname == SERVER[dialect]["host"]


def test_mssql_odbc_keywords_from_driver_options_reach_the_odbc_connection_string():
    options = {"Encrypt": "yes", "TrustServerCertificate": "no", "ApplicationIntent": "ReadOnly"}
    url = sqlalchemy_url(conn(**SERVER["mssql"], username="ingest", password="a;b}c", driver_options=options))
    [connection_string], _ = MSDialect_pyodbc().create_connect_args(url)
    assert connection_string == (
        "DRIVER={ODBC Driver 18 for SQL Server};Server=sql01,1433;Database=Core;UID=ingest;PWD={a;b}}c};"
        "Encrypt=yes;TrustServerCertificate=no;ApplicationIntent=ReadOnly"
    )


def test_mssql_odbc_driver_can_be_chosen_in_driver_options():
    url = sqlalchemy_url(conn(**SERVER["mssql"], driver_options={"driver": "ODBC Driver 17 for SQL Server"}))
    assert url.query == {"driver": "ODBC Driver 17 for SQL Server"}


@pytest.mark.parametrize(
    "dialect,options,query",
    [
        ("mssql", {"Encrypt": "no"}, "encrypt=false"),
        (
            "mssql",
            {"Encrypt": "Mandatory", "TrustServerCertificate": "yes"},
            "encrypt=true&trust_server_certificate=true",
        ),
        ("mssql", {"ApplicationIntent": "ReadOnly"}, "encrypt=true"),
        ("postgres", {"sslmode": "require", "application_name": "bronze"}, "sslmode=require"),
        ("mysql", {"charset": "utf8mb4"}, ""),
    ],
)
def test_driver_options_are_translated_for_connectorx_where_it_has_them(dialect, options, query):
    assert urlsplit(connectorx_url(conn(**SERVER[dialect], driver_options=options))).query == query


def test_postgres_isolation_level_reaches_connectorx_as_a_session_default():
    url = connectorx_url(conn(**SERVER["postgres"], isolation_level="REPEATABLE READ"))
    assert parse_qs(urlsplit(url).query) == {"options": ["-c default_transaction_isolation=repeatable\\ read"]}
    assert "+" not in url  # ConnectorX does not read "+" as a space: spaces must be %20


def test_postgres_isolation_level_joins_libpq_options_from_driver_options():
    options = {"options": "-c statement_timeout=60000", "sslmode": "require"}
    url = connectorx_url(conn(**SERVER["postgres"], isolation_level="SERIALIZABLE", driver_options=options))
    assert parse_qs(urlsplit(url).query) == {
        "sslmode": ["require"],
        "options": ["-c statement_timeout=60000 -c default_transaction_isolation=serializable"],
    }


def test_mssql_isolation_level_goes_ahead_of_the_connectorx_query_in_the_same_batch():
    # ConnectorX's SQL Server URL takes no session settings, but it runs a batch (S3).
    sql = connectorx_sql(conn(**SERVER["mssql"], isolation_level="SNAPSHOT"), "SELECT 1 AS x")
    assert sql == "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT 1 AS x"


@pytest.mark.parametrize("dialect", ["mssql", "postgres"])
def test_without_isolation_level_the_connectorx_query_is_unchanged(dialect):
    assert connectorx_sql(conn(**SERVER[dialect]), "SELECT 1 AS x") == "SELECT 1 AS x"


def test_postgres_isolation_level_is_not_put_in_the_query():  # it is in the URL
    assert connectorx_sql(conn(**SERVER["postgres"], isolation_level="SERIALIZABLE"), "SELECT 1") == "SELECT 1"


@pytest.mark.parametrize(
    "fields", [SERVER["db2"], {"dialect": "snowflake", "host": "acct", "database": "core"}], ids=["db2", "other"]
)
def test_there_is_no_connectorx_url_for_engines_it_does_not_read(fields):
    with pytest.raises(ConfigError, match=r'ConnectorX does not support .* set "backend": "pyarrow"'):
        connectorx_url(conn(**fields))


class FakeSecretsManager:
    """Stands in for boto3's Secrets Manager client: the AWS boundary."""

    def __init__(self, secrets: dict[str, str]) -> None:
        self.secrets = secrets

    def client(self, service: str):
        assert service == "secretsmanager"
        return self

    def get_secret_value(self, SecretId: str) -> dict:  # boto3 spelling
        return {"SecretString": self.secrets[SecretId]}


@pytest.fixture
def aws_secrets(monkeypatch):
    def install(secrets: dict[str, str]) -> None:
        monkeypatch.setitem(sys.modules, "boto3", FakeSecretsManager(secrets))

    db_secrets.clear_cache()
    yield install
    db_secrets.clear_cache()


REFS = {
    "username": {"env": "CORE_DB_USER"},
    "password": {"secret_id": "ingest/core", "key": "password"},
    "host": {"secret_id": "ingest/core_host"},
    "port": {"env": "CORE_DB_PORT"},
}


@pytest.fixture
def referenced(monkeypatch, aws_secrets):
    monkeypatch.setenv("CORE_DB_USER", "ingest")
    monkeypatch.setenv("CORE_DB_PORT", "5433")
    aws_secrets({"ingest/core": json.dumps({"password": SPECIAL}), "ingest/core_host": "pg01.corp"})


def test_secret_references_resolve_in_the_sqlalchemy_url(referenced):
    url = sqlalchemy_url(conn(dialect="postgres", database="core", **REFS))
    assert (url.username, url.password, url.host, url.port) == ("ingest", SPECIAL, "pg01.corp", 5433)


def test_secret_references_resolve_in_the_connectorx_url(referenced):
    url = urlsplit(connectorx_url(conn(dialect="postgres", database="core", **REFS)))
    assert (unquote(url.username), unquote(url.password), url.hostname, url.port) == (
        "ingest",
        SPECIAL,
        "pg01.corp",
        5433,
    )


def test_a_secret_reference_in_driver_options_resolves(monkeypatch):
    monkeypatch.setenv("SSL_MODE", "verify-full")
    url = sqlalchemy_url(conn(**SERVER["postgres"], driver_options={"sslmode": {"env": "SSL_MODE"}}))
    assert url.query == {"sslmode": "verify-full"}


def test_a_missing_environment_variable_names_it(monkeypatch):
    monkeypatch.delenv("CORE_DB_USER", raising=False)
    with pytest.raises(KeyError, match="CORE_DB_USER"):
        sqlalchemy_url(conn(**SERVER["postgres"], username={"env": "CORE_DB_USER"}))


@pytest.mark.parametrize("level", [None, "READ UNCOMMITTED"])
def test_the_traditional_engine_applies_the_isolation_level(tmp_path, level):
    from db_bronze.source import make_engine

    connection = {"dialect": "sqlite", "path": str(tmp_path / "x.sqlite"), "isolation_level": level}
    cfg = SourceConfig.from_dict(
        {
            "pipeline_name": "p",
            "dataset_name": "d",
            "bucket_url": "file:///tmp/unused",
            "connection": {k: v for k, v in connection.items() if v},
            "tables": [{"name": "t", "table": "T", "backend": "pyarrow"}],
        }
    )
    engine = make_engine(cfg)
    try:
        with engine.connect() as c:
            assert c.get_isolation_level() == (level or "SERIALIZABLE")  # SERIALIZABLE: SQLite's default
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("level", "db2"),
    [("READ UNCOMMITTED", "UR"), ("READ COMMITTED", "CS"), ("REPEATABLE READ", "RS"), ("SERIALIZABLE", "RR")],
)
def test_db2_gets_the_standard_isolation_level_by_its_own_name(level, db2):
    """Db2's Repeatable Read (RR) is the standard's SERIALIZABLE; the standard's REPEATABLE READ is its RS."""
    assert engine_isolation_level(conn(**SERVER["db2"], isolation_level=level)) == db2


@pytest.mark.parametrize("dialect", ["postgres", "mssql", "mysql"])
def test_other_engines_get_the_isolation_level_as_configured(dialect):
    assert engine_isolation_level(conn(**SERVER[dialect], isolation_level="REPEATABLE READ")) == "REPEATABLE READ"
    assert engine_isolation_level(conn(**SERVER[dialect])) is None


@pytest.mark.parametrize("name", ["mssql", "postgres", "mysql", "oracle", "db2", "sqlite"])
def test_a_config_dialect_and_its_sqlalchemy_dialect_are_one_entry(name):
    """The URL a config dialect builds loads a SQLAlchemy dialect that maps back to the same entry."""
    from db_bronze import dialects

    if name == "db2":
        pytest.importorskip("ibm_db_sa", reason="the Db2 SQLAlchemy dialect is not installed")
    entry = dialects.dialect(name)
    assert dialects.of(sa.URL.create(entry.driver).get_dialect()) is entry
