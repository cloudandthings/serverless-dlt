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
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects import postgresql as pg

from .config import SourceConfig, TableConfig
from .extract import EXTRACTED_AT
from .source import reflect

log = logging.getLogger(__name__)

# Minimum server version per dialect, as a tuple; None: not set yet. The integration
# matrix fills these in (spike S9); a server below the minimum fails the run, and the
# backend is never switched silently.
MIN_SERVER_VERSION: dict[str, tuple[int, ...] | None] = {
    "mssql": None,
    "postgres": None,
    "mysql": None,
    "oracle": None,
    "sqlite": None,
    "db2": None,
}

# ConnectorX produces Decimal128(38, 10) and holds about 28 significant digits (section 3.1).
MAX_DECIMAL_SCALE = 10
MAX_DECIMAL_PRECISION = 28
# Where a NUMERIC with no declared precision holds any scale, and ConnectorX silently
# drops digits past the 10th decimal place (integration type matrix, section 10.1).
UNCONSTRAINED_NUMERIC_DIALECTS = ("postgres",)


def _named(*names: str) -> Callable[[sa.types.TypeEngine], bool]:
    return lambda t: type(t).__name__.upper() in names


def _is_timetz(t: sa.types.TypeEngine) -> bool:
    return isinstance(t, sa.Time) and bool(t.timezone)


def _is_oracle_local_tz(t: sa.types.TypeEngine) -> bool:
    return isinstance(t, oracle.TIMESTAMP) and bool(t.local_timezone)


# Types ConnectorX cannot read (section 3.1), per dialect: (description, matcher).
DENY_LIST: dict[str, list[tuple[str, Callable[[sa.types.TypeEngine], bool]]]] = {
    "postgres": [
        ("interval", lambda t: isinstance(t, (sa.Interval, pg.INTERVAL))),
        ("money", lambda t: isinstance(t, pg.MONEY)),
        ("timetz", _is_timetz),
        ("macaddr", lambda t: isinstance(t, pg.MACADDR)),
        ("array", lambda t: isinstance(t, sa.ARRAY)),
        ("xml", _named("XML")),
    ],
    "mssql": [
        ("sql_variant", _named("SQL_VARIANT")),
        ("xml", _named("XML")),
        ("hierarchyid", _named("HIERARCHYID")),
        ("geography", _named("GEOGRAPHY", "GEOMETRY")),
    ],
    "oracle": [
        ("raw", lambda t: isinstance(t, oracle.RAW)),
        ("rowid", lambda t: isinstance(t, oracle.ROWID)),
        ("interval", lambda t: isinstance(t, oracle.INTERVAL)),
        ("timestamp with local time zone", _is_oracle_local_tz),
        ("nclob", lambda t: isinstance(t, oracle.NCLOB)),
        ("bfile", lambda t: isinstance(t, oracle.BFILE)),
        ("xmltype", _named("XMLTYPE")),
    ],
}


class PreflightError(Exception):
    """One or more tables cannot run. `findings` maps table name to what is wrong with it."""

    def __init__(self, findings: dict[str, list[str]]):
        self.findings = findings
        lines = [f"preflight failed for {len(findings)} table(s); nothing was extracted:"]
        for name, items in findings.items():
            lines.append(f"  table {name}:")
            lines.extend(f"    - {item}" for item in items)
        lines.append(
            'Fix each table by setting "backend": "pyarrow" on it, or by excluding the offending column '
            'with "columns" (or casting it in "query").'
        )
        super().__init__("\n".join(lines))


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


def column_findings(dialect: str, backend: str, columns: list[tuple[str, sa.types.TypeEngine | None]]) -> list[str]:
    """Every offending column among `(name, reflected type)` pairs, one message each."""
    findings = []
    for name, col_type in columns:
        if name == EXTRACTED_AT:
            findings.append(f"column {name}: the name is reserved for the extraction timestamp; rename or exclude it")
        if backend != "connectorx" or col_type is None:
            continue
        if isinstance(col_type, sa.Numeric) and not isinstance(col_type, sa.Float):
            p, s = col_type.precision, col_type.scale
            if p is None and dialect in UNCONSTRAINED_NUMERIC_DIALECTS:
                findings.append(
                    f"column {name}: NUMERIC without precision would lose digits past the "
                    f"{MAX_DECIMAL_SCALE}th decimal place silently on ConnectorX; declare a precision or cast it"
                )
                continue
            if (s is not None and s > MAX_DECIMAL_SCALE) or (p is not None and p > MAX_DECIMAL_PRECISION):
                findings.append(
                    f"column {name}: DECIMAL({p},{s}) would lose digits silently on ConnectorX "
                    f"(limit: precision {MAX_DECIMAL_PRECISION}, scale {MAX_DECIMAL_SCALE})"
                )
                continue
        for label, matches in DENY_LIST.get(dialect, []):
            if matches(col_type):
                findings.append(f"column {name}: type {label} is not supported by ConnectorX on {dialect}")
                break
    return findings


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
        version = engine.dialect.server_version_info
    server = server_version_finding(dialect, version)
    reports = []
    for tc in tables:
        backend = cfg.backend(tc)
        table = reflect(engine, tc)
        wanted = set(tc.columns) if tc.columns else None
        columns = [(c.name, c.type) for c in table.columns if wanted is None or c.name in wanted]
        report = TableReport(tc.name, backend, column_findings(dialect, backend, columns), table=table)
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
