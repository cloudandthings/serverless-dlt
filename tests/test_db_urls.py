"""db_bronze: SQLAlchemy and ConnectorX URLs built from the discrete connection fields (DESIGN-db.md 5.1).

Offline: URLs are built and, for MSSQL, turned into the ODBC connection string, without any driver.
"""

from __future__ import annotations

import pytest

from db_bronze.config import ConnectionConfig
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
