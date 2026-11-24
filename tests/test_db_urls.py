"""db_bronze: SQLAlchemy and ConnectorX URLs built from the discrete connection fields (DESIGN-db.md 5.1).

Offline: URLs are built and, for MSSQL, turned into the ODBC connection string, without any driver.
"""

from __future__ import annotations

from urllib.parse import unquote, urlsplit

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc

from db_bronze.config import ConfigError, ConnectionConfig
from db_bronze.urls import connectorx_url, sqlalchemy_url

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


@pytest.mark.parametrize(
    "fields", [SERVER["db2"], {"dialect": "snowflake", "host": "acct", "database": "core"}], ids=["db2", "other"]
)
def test_there_is_no_connectorx_url_for_engines_it_does_not_read(fields):
    with pytest.raises(ConfigError, match=r'ConnectorX does not support .* set "backend": "pyarrow"'):
        connectorx_url(conn(**fields))
