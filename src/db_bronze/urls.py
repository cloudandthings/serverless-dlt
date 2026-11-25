"""Connection URLs built from the discrete connection fields (DESIGN-db.md section 5.1).

The SQLAlchemy URL (traditional backends, reflection) and the ConnectorX URL differ
in scheme (`+driver`) and in how options are spelled, so both are built here from
the same fields rather than one being derived from the other. Secret references
are resolved at this point, at run time, never when the config loads.

`driver_options` go to the traditional driver as URL query parameters (ODBC
keywords for MSSQL, libpq parameters for Postgres, connect arguments elsewhere).
ConnectorX has its own option names, so only the options with a ConnectorX
equivalent are translated; the rest are left out and logged.
"""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import quote, urlencode

import sqlalchemy as sa

from . import secrets
from .config import CONNECTORX_DIALECTS, ConfigError, ConnectionConfig

log = logging.getLogger(__name__)

SQLALCHEMY_DRIVERS = {
    "mssql": "mssql+pyodbc",
    "postgres": "postgresql+psycopg",
    "mysql": "mysql+pymysql",
    "oracle": "oracle+oracledb",
    "db2": "db2+ibm_db",
    "sqlite": "sqlite",
}
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
CONNECTORX_SCHEMES = {"mssql": "mssql", "postgres": "postgresql", "mysql": "mysql", "oracle": "oracle"}
_TRUE = ("yes", "true", "mandatory", "strict")


def _resolved(conn: ConnectionConfig, name: str) -> Any:
    return secrets.resolve(getattr(conn, name))


def _port(conn: ConnectionConfig) -> int | None:
    port = _resolved(conn, "port")
    return int(port) if port is not None else None


def _driver_options(conn: ConnectionConfig) -> dict[str, str]:
    return {k: str(secrets.resolve(v)) for k, v in conn.driver_options.items()}


def sqlalchemy_url(conn: ConnectionConfig) -> sa.URL:
    query = _driver_options(conn)
    if conn.dialect == "sqlite":
        return sa.URL.create("sqlite", database=_resolved(conn, "path"), query=query)
    if conn.dialect == "mssql" and "driver" not in {k.lower() for k in query}:
        query = {"driver": ODBC_DRIVER, **query}
    if conn.dialect == "oracle":
        query = {"service_name": _resolved(conn, "service_name"), **query}
    return sa.URL.create(
        SQLALCHEMY_DRIVERS.get(conn.dialect, conn.dialect),
        username=_resolved(conn, "username"),
        password=_resolved(conn, "password"),
        host=_resolved(conn, "host"),
        port=_port(conn),
        database=_resolved(conn, "database"),
        query=query,
    )


# ibm_db_sa knows only Db2's own level names. Db2's Repeatable Read (RR) is the standard's SERIALIZABLE,
# and the standard's REPEATABLE READ is Db2's Read Stability (RS); ibm_db_sa would take "REPEATABLE READ"
# as RR and reject the other standard names (integration, Db2).
DB2_ISOLATION_NAMES = {"READ UNCOMMITTED": "UR", "READ COMMITTED": "CS", "REPEATABLE READ": "RS", "SERIALIZABLE": "RR"}


def engine_isolation_level(conn: ConnectionConfig) -> str | None:
    """`isolation_level` as the SQLAlchemy dialect spells it, or None to leave the driver's default."""
    if conn.isolation_level is None or conn.dialect != "db2":
        return conn.isolation_level
    return DB2_ISOLATION_NAMES[conn.isolation_level]


def _connectorx_options(conn: ConnectionConfig) -> dict[str, str]:
    """`driver_options` spelled the ConnectorX way, where ConnectorX has the same option."""
    given = _driver_options(conn)
    options: dict[str, str] = {}
    used: set[str] = set()
    if conn.dialect == "mssql":
        lower = {k.lower(): (k, v) for k, v in given.items()}
        # ODBC Driver 18 encrypts unless told not to; ConnectorX only encrypts when told to.
        encrypt = lower.get("encrypt", (None, "yes"))
        options["encrypt"] = "true" if encrypt[1].lower() in _TRUE else "false"
        trust = lower.get("trustservercertificate")
        if trust is not None and trust[1].lower() in _TRUE:
            options["trust_server_certificate"] = "true"
        used = {pair[0] for pair in (lower.get("encrypt"), trust) if pair}
    elif conn.dialect == "postgres":
        # ConnectorX hands sslmode and libpq `options` to the server, as libpq does (spike S3).
        used = {k for k in ("sslmode", "options") if k in given}
        options = {k: given[k] for k in ("sslmode", "options") if k in given}
        if conn.isolation_level is not None:
            setting = "-c default_transaction_isolation=" + conn.isolation_level.lower().replace(" ", "\\ ")
            options["options"] = f"{options['options']} {setting}" if "options" in options else setting
    dropped = sorted(set(given) - used)
    if dropped:
        log.warning("driver_options %s have no ConnectorX equivalent and are not passed to it", dropped)
    return options


def connectorx_sql(conn: ConnectionConfig, sql: str) -> str:
    """The SQL ConnectorX runs: on SQL Server, `isolation_level` goes ahead of the query in the same batch.

    ConnectorX's SQL Server URL takes no session settings, but a batch's SET
    applies to the statement after it (spike S3). Elsewhere the level is in the
    URL (Postgres) or rejected by validation.
    """
    if conn.dialect == "mssql" and conn.isolation_level is not None:
        return f"SET TRANSACTION ISOLATION LEVEL {conn.isolation_level}; {sql}"
    return sql


def connectorx_url(conn: ConnectionConfig) -> str:
    if conn.dialect not in CONNECTORX_DIALECTS:
        raise ConfigError(f'ConnectorX does not support dialect {conn.dialect!r}; set "backend": "pyarrow"')
    if conn.dialect == "sqlite":
        return "sqlite://" + quote(os.path.abspath(_resolved(conn, "path")))
    user = quote(str(_resolved(conn, "username") or ""), safe="")
    password = _resolved(conn, "password")
    userinfo = f"{user}:{quote(str(password), safe='')}@" if password is not None else (f"{user}@" if user else "")
    port = _port(conn)
    host = _resolved(conn, "host") + (f":{port}" if port is not None else "")
    database = _resolved(conn, "service_name" if conn.dialect == "oracle" else "database")
    query = urlencode(_connectorx_options(conn), quote_via=quote)  # ConnectorX reads "+" literally
    return f"{CONNECTORX_SCHEMES[conn.dialect]}://{userinfo}{host}/{quote(database, safe='')}" + (
        f"?{query}" if query else ""
    )
