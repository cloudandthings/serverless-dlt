"""Connection URLs built from the discrete connection fields (docs/db/configuration.md).

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

from . import dialects, secrets
from .config import ConfigError, ConnectionConfig

log = logging.getLogger(__name__)

_TRUE = ("yes", "true", "mandatory", "strict")


def _resolved(conn: ConnectionConfig, name: str) -> Any:
    return secrets.resolve(getattr(conn, name))


def _port(conn: ConnectionConfig) -> int | None:
    port = _resolved(conn, "port")
    return int(port) if port is not None else None


def _driver_options(conn: ConnectionConfig) -> dict[str, str]:
    return {k: str(secrets.resolve(v)) for k, v in conn.driver_options.items()}


def sqlalchemy_url(conn: ConnectionConfig) -> sa.URL:
    d = conn.engine
    query = _driver_options(conn)
    if d.is_file:
        return sa.URL.create(d.driver, database=_resolved(conn, "path"), query=query)
    given = {k.lower() for k in query}
    query = {**{k: v for k, v in d.default_url_query.items() if k.lower() not in given}, **query}
    if d.database_key != "database":
        query = {d.database_key: _resolved(conn, d.database_key), **query}
    return sa.URL.create(
        d.driver,
        username=_resolved(conn, "username"),
        password=_resolved(conn, "password"),
        host=_resolved(conn, "host"),
        port=_port(conn),
        database=_resolved(conn, "database"),
        query=query,
    )


def engine_isolation_level(conn: ConnectionConfig) -> str | None:
    """`isolation_level` as the SQLAlchemy dialect spells it, or None to leave the driver's default."""
    level = conn.isolation_level
    return conn.engine.isolation_names.get(level, level) if level is not None else None


def _mssql_options(conn: ConnectionConfig, given: dict[str, str]) -> tuple[dict[str, str], set[str]]:
    lower = {k.lower(): (k, v) for k, v in given.items()}
    # ODBC Driver 18 encrypts unless told not to; ConnectorX only encrypts when told to.
    encrypt = lower.get("encrypt", (None, "yes"))
    options = {"encrypt": "true" if encrypt[1].lower() in _TRUE else "false"}
    trust = lower.get("trustservercertificate")
    if trust is not None and trust[1].lower() in _TRUE:
        options["trust_server_certificate"] = "true"
    return options, {pair[0] for pair in (lower.get("encrypt"), trust) if pair}


def _postgres_options(conn: ConnectionConfig, given: dict[str, str]) -> tuple[dict[str, str], set[str]]:
    # ConnectorX hands sslmode and libpq `options` to the server, as libpq does (spike S3).
    options = {k: given[k] for k in ("sslmode", "options") if k in given}
    used = set(options)
    if conn.isolation_level is not None:
        setting = "-c default_transaction_isolation=" + conn.isolation_level.lower().replace(" ", "\\ ")
        options["options"] = f"{options['options']} {setting}" if "options" in options else setting
    return options, used


# How each engine's `driver_options` translate to ConnectorX URL options: (options, the keys used).
_CONNECTORX_OPTIONS = {dialects.MSSQL.name: _mssql_options, dialects.POSTGRES.name: _postgres_options}


def _connectorx_options(conn: ConnectionConfig) -> dict[str, str]:
    """`driver_options` spelled the ConnectorX way, where ConnectorX has the same option."""
    given = _driver_options(conn)
    translate = _CONNECTORX_OPTIONS.get(conn.engine.name)
    options, used = translate(conn, given) if translate else ({}, set())
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
    if conn.engine.connectorx_isolation_in_batch and conn.isolation_level is not None:
        return f"SET TRANSACTION ISOLATION LEVEL {conn.isolation_level}; {sql}"
    return sql


def connectorx_url(conn: ConnectionConfig) -> str:
    d = conn.engine
    if d.connectorx is None:
        raise ConfigError(f'ConnectorX does not support dialect {conn.dialect!r}; set "backend": "pyarrow"')
    if d.is_file:
        return f"{d.connectorx}://" + quote(os.path.abspath(_resolved(conn, "path")))
    user = quote(str(_resolved(conn, "username") or ""), safe="")
    password = _resolved(conn, "password")
    userinfo = f"{user}:{quote(str(password), safe='')}@" if password is not None else (f"{user}@" if user else "")
    port = _port(conn)
    host = _resolved(conn, "host") + (f":{port}" if port is not None else "")
    database = _resolved(conn, d.database_key)
    query = urlencode(_connectorx_options(conn), quote_via=quote)  # ConnectorX reads "+" literally
    return f"{d.connectorx}://{userinfo}{host}/{quote(database, safe='')}" + (f"?{query}" if query else "")
