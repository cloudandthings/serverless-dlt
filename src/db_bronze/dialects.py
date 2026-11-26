"""Every source engine db_bronze knows, in one table (docs/db/engines.md,
configuration.md and preflight.md).

The config names an engine one way (`postgres`, `db2`), SQLAlchemy another
(`postgresql`, `ibm_db_sa`), and its URL a third (`postgresql+psycopg`,
`db2+ibm_db`). This table is the only place those vocabularies meet, and the
only place a per-engine difference is recorded: code asks the `Dialect` for a
property rather than comparing dialect names. Any other SQLAlchemy dialect name
in the config gets `generic(name)`: the traditional backend, no ConnectorX.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import sqlalchemy as sa

# What SQLAlchemy's `isolation_level` accepts across the supported dialects (SNAPSHOT: mssql).
ISOLATION_LEVELS = ("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE", "SNAPSHOT", "AUTOCOMMIT")
_STANDARD_LEVELS = ("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE")


@dataclass(frozen=True, eq=False)
class Dialect:
    name: str  # as the config spells it: connection.dialect
    title: str  # as messages spell it
    sqlalchemy: str  # what SQLAlchemy's dialect reports as its `name`
    driver: str  # the SQLAlchemy URL drivername
    # The connection keys that say where the database is: host + database, host + service_name, or path.
    address: tuple[str, ...] = ("host", "database")
    # ConnectorX URL scheme; None: ConnectorX does not read the engine (section 3.1).
    connectorx: str | None = None
    # The levels ConnectorX can apply (spike S3), and whether it does so with a SET ahead of the
    # query in the same batch (so not on a partitioned read, which ConnectorX parses as one SELECT).
    connectorx_isolation: tuple[str, ...] = ()
    connectorx_isolation_in_batch: bool = False
    connectorx_kerberos: bool = False  # ConnectorX supports Kerberos on this engine (docs/db/kerberos.md)
    # ConnectorX upper-cases a partition_on column in its range query, quoted or not (integration, Oracle).
    connectorx_partition_upper_cases: bool = False
    isolation_levels: tuple[str, ...] = ISOLATION_LEVELS  # what the traditional backend can apply
    isolation_names: Mapping[str, str] = field(default_factory=dict)  # level -> the SQLAlchemy dialect's spelling
    # Closing a traditional-backend stream early reads the rest of the result first (S1, section 7.4).
    stream_close_blocks: bool = False
    # Timestamps are text compared as text, so the cursor is compared through julianday() (section 7.3).
    timestamps_are_text: bool = False
    # An inlined timestamp literal is converted to the column's type, which may round it (spike S2).
    inlined_timestamps_need_cast: bool = False
    # OFFSET needs a LIMIT, which SQLAlchemy binds even under literal_binds.
    offset_needs_limit: bool = False
    # The driver reports the server version as text, e.g. ('DB2/LINUXX8664', '12.01.0500').
    version_is_text: bool = False
    default_url_query: Mapping[str, str] = field(default_factory=dict)  # added unless driver_options set the key

    @property
    def database_key(self) -> str:
        """The connection key that names the database: `database`, or Oracle's `service_name`."""
        return "service_name" if "service_name" in self.address else "database"

    @property
    def is_file(self) -> bool:
        return "path" in self.address


MSSQL = Dialect(
    "mssql",
    "SQL Server",
    "mssql",
    "mssql+pyodbc",
    connectorx="mssql",
    connectorx_isolation=("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SNAPSHOT", "SERIALIZABLE"),
    connectorx_isolation_in_batch=True,
    connectorx_kerberos=True,
    inlined_timestamps_need_cast=True,
    default_url_query={"driver": "ODBC Driver 18 for SQL Server"},
)
POSTGRES = Dialect(
    "postgres",
    "Postgres",
    "postgresql",
    "postgresql+psycopg",
    connectorx="postgresql",
    connectorx_isolation=_STANDARD_LEVELS,  # through libpq `options` in the URL
)
MYSQL = Dialect("mysql", "MySQL", "mysql", "mysql+pymysql", connectorx="mysql", stream_close_blocks=True)
ORACLE = Dialect(
    "oracle",
    "Oracle",
    "oracle",
    "oracle+oracledb",
    address=("host", "service_name"),
    connectorx="oracle",
    connectorx_partition_upper_cases=True,
)
# ibm_db_sa knows only Db2's own level names. Db2's Repeatable Read (RR) is the standard's SERIALIZABLE,
# and the standard's REPEATABLE READ is Db2's Read Stability (RS); ibm_db_sa would take "REPEATABLE READ"
# as RR and reject the other standard names (integration, Db2). No SNAPSHOT, and no AUTOCOMMIT.
DB2 = Dialect(
    "db2",
    "Db2",
    "ibm_db_sa",
    "db2+ibm_db",
    isolation_levels=_STANDARD_LEVELS,
    isolation_names={"READ UNCOMMITTED": "UR", "READ COMMITTED": "CS", "REPEATABLE READ": "RS", "SERIALIZABLE": "RR"},
    version_is_text=True,
)
SQLITE = Dialect(
    "sqlite",
    "SQLite",
    "sqlite",
    "sqlite",
    address=("path",),
    connectorx="sqlite",
    timestamps_are_text=True,
    offset_needs_limit=True,
)

DIALECTS: dict[str, Dialect] = {d.name: d for d in (MSSQL, POSTGRES, MYSQL, ORACLE, DB2, SQLITE)}
_BY_SQLALCHEMY = {d.sqlalchemy: d for d in DIALECTS.values()}
CONNECTORX_DIALECTS = tuple(name for name, d in DIALECTS.items() if d.connectorx)


def generic(name: str) -> Dialect:
    """A dialect db_bronze has no entry for: SQLAlchemy's own, on the traditional backend, address not checked."""
    return Dialect(name, name, name.split("+", 1)[0], name, address=())


def dialect(name: str) -> Dialect:
    """The entry for a config's `connection.dialect`."""
    return DIALECTS.get(name) or generic(name)


def of(sa_dialect: sa.Dialect) -> Dialect:
    """The entry for a live SQLAlchemy dialect (an engine's, or a compiler's)."""
    return _BY_SQLALCHEMY.get(sa_dialect.name) or generic(sa_dialect.name)
