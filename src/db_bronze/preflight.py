"""Preflight: reflect every table about to run and fail before anything is extracted (DESIGN-db.md section 6.2).

The rules are pure functions over reflected column types, so they test against
fake metadata. `preflight()` applies them to a live engine.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

import sqlalchemy as sa
from sqlalchemy.dialects import mssql, oracle
from sqlalchemy.dialects import postgresql as pg

from .config import SourceConfig, TableConfig
from .errors import PreflightError
from .extract import DB2_DIALECT, EXTRACTED_AT
from .source import reflect

log = logging.getLogger(__name__)

# Minimum server version per dialect, as a tuple; None: not set yet. The integration
# matrix fills these in (spike S9); a server below the minimum fails the run, and the
# backend is never switched silently. Postgres 11, MySQL 8.0 and SQL Server 2017 (14) are
# the oldest series the whole integration suite has passed on (11.16, 8.0.46, 14.0.3570;
# DESIGN-db.md section 11). SQL Server on Linux starts at 2017, so nothing older was run.
# Oracle 23 (Free 23.26) is the only series it could run on so far: older servers are
# x86_64 only and do not start under emulation on an arm64 host (S9 stays open below 23).
# Db2 11.5: the whole suite passes on 11.5 and 12.1 (Community images; nothing older is published).
MIN_SERVER_VERSION: dict[str, tuple[int, ...] | None] = {
    "mssql": (14,),
    "postgres": (11,),
    "mysql": (8, 0),
    "oracle": (23,),
    "sqlite": None,
    "db2": (11, 5),
}

# ConnectorX produces Decimal128(38, 10) and holds about 28 significant digits (section 3.1).
MAX_DECIMAL_SCALE = 10
MAX_DECIMAL_PRECISION = 28
# Where a NUMERIC with no declared precision holds any scale, and ConnectorX silently
# drops digits past the 10th decimal place (integration type matrix, section 10.1).
UNCONSTRAINED_NUMERIC_DIALECTS = ("postgres", "oracle")


def _named(*names: str) -> Callable[[sa.types.TypeEngine], bool]:
    return lambda t: type(t).__name__.upper() in names


def _is_timetz(t: sa.types.TypeEngine) -> bool:
    return isinstance(t, sa.Time) and bool(t.timezone)


def _is_oracle_local_tz(t: sa.types.TypeEngine) -> bool:
    return isinstance(t, oracle.TIMESTAMP) and bool(t.local_timezone)


def _is_oracle_tz(t: sa.types.TypeEngine) -> bool:
    return isinstance(t, oracle.TIMESTAMP) and bool(t.timezone)


def _is_postgres_varbit(t: sa.types.TypeEngine) -> bool:
    return isinstance(t, pg.BIT) and bool(t.varying)


def _is_bare_string(t: sa.types.TypeEngine) -> bool:
    # SQLAlchemy reflects Postgres "char" (and name) as a plain String with no length; every other
    # text type reflects as a subclass (VARCHAR, CHAR, TEXT)
    return type(t) is sa.String and t.length is None


# Postgres array elements ConnectorX reads (integration type matrix). Arrays of any other
# element type panic in ConnectorX; NUMERIC elements are checked by the decimal rule.
_POSTGRES_ARRAY_ITEMS = (sa.Integer, sa.Boolean, sa.Float, sa.Numeric, sa.VARCHAR, sa.TEXT)


def _is_unreadable_postgres_array(t: sa.types.TypeEngine) -> bool:
    if not isinstance(t, sa.ARRAY):
        return False
    item = t.item_type
    return not isinstance(item, _POSTGRES_ARRAY_ITEMS) or isinstance(item, sa.CHAR)


# Types ConnectorX cannot read (section 3.1, settled by the integration type matrix,
# section 10.1), per dialect: (description, matcher). Types SQLAlchemy does not
# recognise at all (Postgres xml, geometric types, pg_lsn, composite types; MySQL
# geometry types) are caught on every server dialect by `_UNRECOGNISED`.
DENY_LIST: dict[str, list[tuple[str, Callable[[sa.types.TypeEngine], bool]]]] = {
    "postgres": [
        ("interval", lambda t: isinstance(t, (sa.Interval, pg.INTERVAL))),
        ("money", lambda t: isinstance(t, pg.MONEY)),
        ("timetz", _is_timetz),
        ("macaddr", lambda t: isinstance(t, (pg.MACADDR, pg.MACADDR8))),
        ("cidr", lambda t: isinstance(t, pg.CIDR)),
        ("varbit", _is_postgres_varbit),
        ("tsvector", lambda t: isinstance(t, pg.TSVECTOR)),
        ("tsquery", lambda t: isinstance(t, pg.TSQUERY)),
        ('"char" (or name: SQLAlchemy reflects both as String)', _is_bare_string),
        ("array of this element type", _is_unreadable_postgres_array),
    ],
    # money: read as a double, too narrow for its 19 digits. datetimeoffset: ConnectorX applies the offset
    # twice, so the instant is wrong (integration type matrix, section 10.1). sql_variant: UNREADABLE.
    "mssql": [
        ("xml", _named("XML")),
        ("hierarchyid", _named("HIERARCHYID")),
        ("geography", _named("GEOGRAPHY")),
        ("geometry", _named("GEOMETRY")),
        ("money", lambda t: isinstance(t, mssql.MONEY)),
        ("datetimeoffset", lambda t: isinstance(t, mssql.DATETIMEOFFSET)),
    ],
    "oracle": [
        ("raw", lambda t: isinstance(t, oracle.RAW)),
        ("rowid", lambda t: isinstance(t, oracle.ROWID)),
        ("interval", lambda t: isinstance(t, oracle.INTERVAL)),
        ("timestamp with local time zone", _is_oracle_local_tz),
        ("nclob", lambda t: isinstance(t, oracle.NCLOB)),
        ("bfile", lambda t: isinstance(t, oracle.BFILE)),
        ("xmltype", _named("XMLTYPE")),
        # integration (Oracle Free 23.26, ConnectorX 0.4.6): also not readable
        ("timestamp with time zone", _is_oracle_tz),
        ("boolean", lambda t: isinstance(t, sa.Boolean)),
        ("json", lambda t: isinstance(t, sa.JSON)),
    ],
}
# SQLite reflects a column declared without a type as NullType, and ConnectorX reads it from its values.
_UNRECOGNISED_EXEMPT = ("sqlite",)


# Types no backend lands losslessly: a finding on every backend. pyodbc reads a sql_variant
# only as the bytes of its value, without the value's type.
UNREADABLE: dict[str, list[tuple[str, Callable[[sa.types.TypeEngine], bool]]]] = {
    "mssql": [("sql_variant", _named("SQL_VARIANT"))],
}


def server_version(dialect: sa.Dialect) -> tuple[int, ...] | None:
    """The server version the dialect learned on connect, as numbers.

    ibm_db_sa reports the product and its level as text, ('DB2/LINUXX8664',
    '12.01.0500') (integration, Db2), which would not compare with a minimum.
    """
    version = dialect.server_version_info
    if dialect.name == DB2_DIALECT and version:
        return tuple(int(part) for part in version[-1].split("."))
    return version


def server_version_finding(
    dialect: str, version: tuple[int, ...] | None, minimums: dict[str, tuple[int, ...] | None] | None = None
) -> str | None:
    """A finding when the server is older than the dialect's minimum; None when fine, unset or unknown."""
    minimum = (MIN_SERVER_VERSION if minimums is None else minimums).get(dialect)
    if minimum is None or version is None or tuple(version) >= minimum:
        return None
    have, need = ".".join(map(str, version)), ".".join(map(str, minimum))
    return (
        f"{dialect} server {have} is below the minimum {need}; "
        "upgrade the server (the backend is never switched silently)"
    )


def _decimal_finding(dialect: str, col_type: sa.types.TypeEngine) -> str | None:
    if not isinstance(col_type, sa.Numeric) or isinstance(col_type, sa.Float):
        return None
    if isinstance(col_type, oracle.NUMBER) and col_type.scale == 0:
        # ConnectorX reads Oracle NUMBER(p, 0) as int64, whatever p: no decimal, so no silent loss. A
        # value wider than int64 fails the read loudly (integration type matrix, Oracle).
        return None
    p, s = col_type.precision, col_type.scale
    if p is None and dialect in UNCONSTRAINED_NUMERIC_DIALECTS:
        return (
            f"NUMERIC without precision would lose digits past the {MAX_DECIMAL_SCALE}th decimal place "
            "silently on ConnectorX; declare a precision or cast it"
        )
    if (s is not None and s > MAX_DECIMAL_SCALE) or (p is not None and p > MAX_DECIMAL_PRECISION):
        return (
            f"DECIMAL({p},{s}) would lose digits silently on ConnectorX "
            f"(limit: precision {MAX_DECIMAL_PRECISION}, scale {MAX_DECIMAL_SCALE})"
        )
    return None


def _connectorx_finding(dialect: str, col_type: sa.types.TypeEngine) -> str | None:
    """Why ConnectorX cannot read a column of this type exactly on `dialect`, or None when it can."""
    if isinstance(col_type, sa.types.NullType) and dialect not in _UNRECOGNISED_EXEMPT:
        return (
            f"type not recognised by SQLAlchemy; ConnectorX reads none of the unrecognised {dialect} types "
            "tested (e.g. xml, geometry)"
        )
    if isinstance(col_type, sa.ARRAY) and (item := _decimal_finding(dialect, col_type.item_type)):
        return f"array element: {item}"
    if decimal := _decimal_finding(dialect, col_type):
        return decimal
    for label, matches in DENY_LIST.get(dialect, []):
        if matches(col_type):
            return f"type {label} is not supported by ConnectorX on {dialect}"
    return None


def column_findings(dialect: str, backend: str, columns: list[tuple[str, sa.types.TypeEngine | None]]) -> list[str]:
    """Every offending column among `(name, reflected type)` pairs, one message each."""
    findings = []
    for name, col_type in columns:
        if name == EXTRACTED_AT:
            findings.append(f"column {name}: the name is reserved for the extraction timestamp; rename or exclude it")
        if col_type is None:
            continue
        unreadable = next((label for label, matches in UNREADABLE.get(dialect, []) if matches(col_type)), None)
        if unreadable is not None:
            findings.append(
                f"column {name}: type {unreadable} cannot be landed losslessly by any backend; "
                'exclude the column, or cast it in "query"'
            )
            continue
        if backend != "connectorx":
            continue
        if finding := _connectorx_finding(dialect, col_type):
            findings.append(f"column {name}: {finding}")
    return findings


def partition_findings(tc: TableConfig, table: sa.FromClause, dialect: sa.Dialect) -> list[str]:
    """What stops ConnectorX `partition_on` reading the table's partition column; empty when it can."""
    if tc.partition is None:
        return []
    column = tc.partition.column
    if column not in table.c:
        return [f"partition column {column!r} is not a column of the table"]
    if not isinstance(table.c[column].type, sa.Integer):
        return [
            f"partition column {column!r} is not an integer ({table.c[column].type}). "
            'Fix: partition on an integer column, or remove "partition"'
        ]
    if dialect.name == "oracle" and dialect.identifier_preparer.quote(column) != column:
        # ConnectorX upper-cases the column in its range query, quotes or not (integration, Oracle)
        return [
            f"ConnectorX cannot partition on Oracle by the case-sensitive column {column!r}. "
            'Fix: partition on a column with a case-insensitive name, or remove "partition"'
        ]
    return []


@dataclass
class TableReport:
    """What preflight learned about one table."""

    name: str
    backend: str
    findings: list[str] = field(default_factory=list)
    null_cursor_count: int | None = None  # None: not applicable (no cursor, or NULL cursors are included)
    table: sa.FromClause | None = field(default=None, repr=False)  # the reflection, reused by the run

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "backend": self.backend,
            "findings": self.findings,
            "null_cursor_count": self.null_cursor_count,
        }


def count_null_cursor(engine: sa.Engine, table: sa.FromClause, tc: TableConfig) -> int | None:
    """Rows the cursor filter will never see (NULL cursor), or None when they are included or there is no cursor."""
    if tc.cursor is None or tc.cursor.include_null_cursor:
        return None
    stmt = sa.select(sa.func.count()).select_from(table).where(table.c[tc.cursor.column].is_(None))
    if tc.where:
        stmt = stmt.where(sa.text(tc.where))
    with engine.connect() as conn:
        return int(conn.execute(stmt).scalar_one())


def preflight(
    cfg: SourceConfig, tables: tuple[TableConfig, ...], engine: sa.Engine
) -> tuple[list[str], list[TableReport]]:
    """Reflect each table and apply the rules. Returns (server findings, one report per table). Never extracts."""
    dialect = cfg.connection.dialect
    with engine.connect():  # the dialect learns the server version on first connect
        version = server_version(engine.dialect)
    server = server_version_finding(dialect, version)
    reports = []
    for tc in tables:
        backend = cfg.backend(tc)
        table = reflect(engine, tc)
        wanted = set(tc.columns) if tc.columns else None
        # A query's result columns are not reflected: their types are unknown (None), not unrecognised.
        columns = [(c.name, None if tc.query else c.type) for c in table.columns if wanted is None or c.name in wanted]
        findings = column_findings(dialect, backend, columns) + partition_findings(tc, table, engine.dialect)
        report = TableReport(tc.name, backend, findings, table=table)
        report.null_cursor_count = count_null_cursor(engine, table, tc)
        log.info("preflight %s", json.dumps(report.as_dict()))
        reports.append(report)
    return ([server] if server else []), reports


def raise_on_findings(server: list[str], reports: list[TableReport]) -> None:
    findings = {r.name: r.findings for r in reports if r.findings}
    if server:  # the server is the same for every table
        findings = {r.name: server + r.findings for r in reports}
    if findings:
        raise PreflightError(findings)
