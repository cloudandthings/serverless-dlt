"""Source config and run request: plain dataclasses, validated strictly (DESIGN-db.md sections 5 and 6.1).

One config document = one database connection = one dlt pipeline = one Lambda
function. Unknown keys are rejected, so a typo fails loudly instead of silently
falling back to a default. Every static rule fails with `ConfigError` and a
message that says how to fix it. Copied in style from `api_bronze.config` (D1).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from . import secrets
from .extract import EXTRACTED_AT

MODES = ("incremental", "full", "backfill", "plan")
PLANS = ("snapshot", "backfill")
BACKENDS = ("connectorx", "pyarrow", "sqlalchemy")
# Engines ConnectorX reads (section 3.1); these default to it (D3, section 5.3). Db2 is not supported.
CONNECTORX_DIALECTS = ("mssql", "postgres", "mysql", "oracle", "sqlite")
CURSOR_TYPES = ("timestamp", "integer")
CHUNKING_STRATEGIES = ("stream", "keyset")
AUTH_TYPES = ("password", "kerberos")
# What SQLAlchemy's `isolation_level` accepts across the supported dialects (SNAPSHOT: mssql).
ISOLATION_LEVELS = ("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE", "SNAPSHOT", "AUTOCOMMIT")
PARQUET_CODECS = ("zstd", "snappy", "gzip", "none")
SERVER_DIALECTS = ("mssql", "postgres", "mysql", "db2")  # host + database; oracle uses service_name
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_DIALECT = re.compile(r"^[a-z][a-z0-9_]*(\+[a-z0-9_]+)?$")


class ConfigError(ValueError):
    pass


def parse_iso(value: str, where: str) -> datetime:
    try:
        return datetime.fromisoformat(str(value))
    except ValueError as e:
        raise ConfigError(f"{where} is not ISO-8601: {value!r}") from e


def to_source_time(value: datetime, naive_timezone: str) -> datetime:
    """A timestamp as the source's naive columns represent it.

    A value with an offset is converted to wall-clock time in `naive_timezone`
    and its offset dropped. A naive value is already source time. Used only at
    the boundaries (config `initial_value`, event window bounds); data values are
    never converted (DESIGN-db.md 7.3).
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo(naive_timezone)).replace(tzinfo=None)


def default_backend(dialect: str) -> str:
    """The backend a table gets when neither it nor `defaults` names one: from the dialect alone (D3)."""
    return "connectorx" if dialect in CONNECTORX_DIALECTS else "pyarrow"


def _build(cls, data: Any, where: str, **overrides):
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: expected an object, got {type(data).__name__}")
    known = {f.name for f in fields(cls)}
    unknown = sorted(set(data) - known)
    if unknown:
        raise ConfigError(f"{where}: unknown keys {unknown}; allowed {sorted(known)}")
    try:
        return cls(**{**data, **overrides})
    except TypeError as e:
        raise ConfigError(f"{where}: {e}") from e


def _names(value: Any, where: str) -> tuple[str, ...]:
    """A list of source column names, as a tuple."""
    if not isinstance(value, list | tuple) or not all(isinstance(v, str) and v for v in value):
        raise ConfigError(f"{where}: expected a list of column names, got {value!r}")
    return tuple(value)


def _check_name(what: str, name: Any) -> None:
    if not isinstance(name, str) or not _NAME.match(name):
        raise ConfigError(
            f"{what} {name!r} must match {_NAME.pattern}: use lowercase letters, digits and underscores, "
            "starting with a letter"
        )


def _number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


@dataclass(frozen=True)
class AuthConfig:
    """Tagged: `password` is implemented; `kerberos` parses and fails at run start (section 8)."""

    type: str = "password"
    principal: str | None = None
    keytab: dict | None = None  # a secret reference
    krb5_conf: str | None = None

    def __post_init__(self) -> None:
        if self.type not in AUTH_TYPES:
            raise ConfigError(f"connection.auth.type {self.type!r} not in {AUTH_TYPES}")
        kerberos_keys = {"principal": self.principal, "keytab": self.keytab, "krb5_conf": self.krb5_conf}
        if self.type != "kerberos" and any(v is not None for v in kerberos_keys.values()):
            raise ConfigError(
                f"connection.auth: only kerberos takes {sorted(kerberos_keys)}; remove them for {self.type!r}"
            )
        if self.keytab is not None and not secrets.is_ref(self.keytab):
            raise ConfigError(
                'connection.auth.keytab must be a secret reference: {"secret_id": ...} or {"env": ...}'
            )


@dataclass(frozen=True)
class ConnectionConfig:
    dialect: str  # mssql | postgres | mysql | oracle | db2 | sqlite | another SQLAlchemy dialect
    host: str | dict | None = None
    port: int | dict | None = None
    database: str | dict | None = None
    service_name: str | dict | None = None  # oracle, instead of database
    path: str | dict | None = None  # sqlite database file
    username: str | dict | None = None
    password: str | dict | None = None
    auth: AuthConfig = field(default_factory=AuthConfig)
    driver_options: dict[str, Any] = field(default_factory=dict)  # passed to the traditional driver
    isolation_level: str | None = None  # applied to the traditional backend
    naive_timezone: str = "UTC"  # what naive source timestamps mean; converts cursor bounds only

    def __post_init__(self) -> None:
        if not isinstance(self.dialect, str) or not _DIALECT.match(self.dialect):
            raise ConfigError(
                f"connection.dialect {self.dialect!r} is not a SQLAlchemy dialect name; use one of "
                f"{('mssql', 'postgres', 'mysql', 'oracle', 'db2', 'sqlite')} or another dialect's name"
            )
        for name in ("host", "database", "service_name", "path", "username", "password"):
            value = getattr(self, name)
            if value is not None and not (isinstance(value, str) and value) and not secrets.is_ref(value):
                raise ConfigError(f"connection.{name} must be a string or a secret reference: {value!r}")
        port = self.port
        if port is not None and not secrets.is_ref(port) and not (_positive_int(port) and port <= 65535):
            raise ConfigError(f"connection.port must be an integer or a secret reference: {port!r}")
        if not isinstance(self.driver_options, dict):
            raise ConfigError(f"connection.driver_options must be an object: {self.driver_options!r}")
        for key, value in self.driver_options.items():
            if not (isinstance(value, str) or _number(value) or secrets.is_ref(value)):
                raise ConfigError(
                    f"connection.driver_options[{key!r}] must be a string, a number or a secret reference: {value!r}"
                )
        self._check_fields_for_dialect()
        if self.isolation_level is not None and self.isolation_level not in ISOLATION_LEVELS:
            raise ConfigError(f"connection.isolation_level {self.isolation_level!r} not in {ISOLATION_LEVELS}")
        try:
            ZoneInfo(self.naive_timezone)
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            raise ConfigError(
                f"connection.naive_timezone {self.naive_timezone!r} is not an IANA time zone; "
                'use a name such as "UTC" or "Africa/Johannesburg"'
            ) from None

    def _check_fields_for_dialect(self) -> None:
        d = self.dialect
        if d == "sqlite":
            if not self.path:
                raise ConfigError('connection: set "path" to the database file for sqlite')
            server_keys = ("host", "port", "database", "service_name", "username", "password")
            given = [k for k in server_keys if getattr(self, k) is not None]
            if given:
                raise ConfigError(f'connection: sqlite takes only "path"; remove "{given[0]}"')
            return
        if self.path is not None:
            raise ConfigError(f'connection: only sqlite takes "path"; for {d!r} set "host" and "database"')
        if d == "oracle":
            if self.database is not None:
                raise ConfigError('connection: oracle takes "service_name", not "database"; rename the key')
            required = ("host", "service_name")
        else:
            if self.service_name is not None:
                raise ConfigError(f'connection: only oracle takes "service_name"; for {d!r} use "database"')
            required = ("host", "database") if d in SERVER_DIALECTS else ()
        for key in required:
            if getattr(self, key) is None:
                raise ConfigError(f'connection: {d} needs a server address; set "{key}"')


@dataclass(frozen=True)
class CursorConfig:
    column: str  # source column name, exactly as in the database
    type: str  # timestamp | integer
    initial_value: str | int  # ISO-8601 for timestamp cursors, an integer for integer cursors
    # Seconds (timestamp) or an integer delta: re-read this trailing window on every run. dlt then
    # deduplicates nothing, so the whole window lands again each run (prototype S4, DESIGN-db.md 12).
    lag: int | float | None = None
    include_null_cursor: bool = False  # adds `OR cursor IS NULL`: those rows land on every run

    def __post_init__(self) -> None:
        if self.type not in CURSOR_TYPES:
            raise ConfigError(f"cursor.type {self.type!r} not in {CURSOR_TYPES}")
        if self.type == "integer" and (not isinstance(self.initial_value, int) or isinstance(self.initial_value, bool)):
            raise ConfigError(f"cursor.initial_value must be an integer for an integer cursor: {self.initial_value!r}")
        if self.type == "timestamp":
            parse_iso(self.initial_value, "cursor.initial_value")
        if self.lag is not None and (not _number(self.lag) or self.lag < 0):
            raise ConfigError(f"cursor.lag must be a number >= 0 (seconds, or an integer delta): {self.lag!r}")
        if self.type == "integer" and self.lag is not None and not isinstance(self.lag, int):
            raise ConfigError(f"cursor.lag {self.lag!r}: an integer cursor needs an integer lag")
        if not isinstance(self.include_null_cursor, bool):
            raise ConfigError(f"cursor.include_null_cursor must be true or false: {self.include_null_cursor!r}")

    def initial_cursor_value(self, naive_timezone: str) -> datetime | int:
        """The initial value as the source represents it: an integer, or a naive timestamp in source time."""
        if self.type == "integer":
            return self.initial_value
        return to_source_time(parse_iso(self.initial_value, "cursor.initial_value"), naive_timezone)

    def bound(self, value: str | int, what: str, naive_timezone: str) -> datetime | int:
        """An event window bound as the source represents it: an integer, or a naive timestamp in source time."""
        if self.type == "integer":
            if not _is_int(value):
                raise ConfigError(f"{what} must be an integer for the integer cursor {self.column!r}: {value!r}")
            return value
        if not isinstance(value, str):
            raise ConfigError(
                f"{what} must be an ISO-8601 timestamp for the timestamp cursor {self.column!r}: {value!r}"
            )
        return to_source_time(parse_iso(value, what), naive_timezone)


@dataclass(frozen=True)
class ChunkingConfig:
    strategy: str = "stream"  # stream | keyset
    order_by: tuple[str, ...] = ()
    page_size: int | None = None

    def __post_init__(self) -> None:
        if self.strategy not in CHUNKING_STRATEGIES:
            raise ConfigError(f"chunking.strategy {self.strategy!r} not in {CHUNKING_STRATEGIES}")
        if self.strategy == "stream" and (self.order_by or self.page_size is not None):
            raise ConfigError("chunking: order_by and page_size apply only to keyset; remove them or use keyset")


@dataclass(frozen=True)
class PartitionConfig:
    column: str  # ConnectorX partition_on; the preflight checks it is an integer column
    num: int

    def __post_init__(self) -> None:
        if not isinstance(self.column, str) or not self.column:
            raise ConfigError(f"partition.column must be a column name: {self.column!r}")
        if not _positive_int(self.num):
            raise ConfigError(f"partition.num must be a positive integer: {self.num!r}")


@dataclass(frozen=True)
class Defaults:
    backend: str | None = None
    batch_size: int = 50_000

    def __post_init__(self) -> None:
        if not _positive_int(self.batch_size):
            raise ConfigError(f"defaults.batch_size must be a positive integer: {self.batch_size!r}")


@dataclass(frozen=True)
class TableConfig:
    name: str  # bronze table name (path prefix)
    table: str | None = None  # source table name, exactly as in the database
    schema: str | None = None  # source schema; None is the connection's default
    query: str | None = None  # instead of schema + table: a view or join; needs a cursor to run incrementally
    primary_key: tuple[str, ...] = ()
    columns: tuple[str, ...] | None = None  # projection; None is every reflected column
    where: str | None = None  # static predicate, ANDed with the cursor filter
    cursor: CursorConfig | None = None  # None: a snapshot table
    backend: str | None = None  # overrides defaults.backend
    chunking: ChunkingConfig = field(default_factory=ChunkingConfig)
    batch_size: int | None = None  # rows per Arrow batch; overrides defaults.batch_size
    partition: PartitionConfig | None = None

    def __post_init__(self) -> None:
        _check_name("table name", self.name)
        if self.batch_size is not None and not _positive_int(self.batch_size):
            raise ConfigError(f"table {self.name}: batch_size must be a positive integer")
        self._check_source()
        named = {
            *self.primary_key,
            *(self.columns or ()),
            *self.chunking.order_by,
            *([self.cursor.column] if self.cursor else []),
            *([self.partition.column] if self.partition else []),
        }
        if EXTRACTED_AT in named:
            raise ConfigError(
                f'table {self.name}: a source column is named "{EXTRACTED_AT}", which db_bronze adds to every row; '
                f'leave it out of "columns", or rename it in a "query" (SELECT "{EXTRACTED_AT}" AS ...)'
            )
        if self.cursor and self.cursor.lag and not self.primary_key:
            raise ConfigError(
                f"table {self.name}: cursor.lag re-reads a window, and dlt needs the primary key for it; "
                'set "primary_key", or remove "lag"'
            )
        if self.chunking.strategy == "keyset":
            self._check_keyset()
        if self.partition and self.chunking.strategy != "stream":
            raise ConfigError(
                f'table {self.name}: partition reads cannot be chunked; use "chunking": {{"strategy": "stream"}}, '
                'or remove "partition"'
            )

    def _check_source(self) -> None:
        if self.query is None:
            if not self.table:
                raise ConfigError(f'table {self.name}: no source; set "table" (and optionally "schema"), or "query"')
            return
        clashing = [k for k in ("schema", "table", "columns", "where") if getattr(self, k) is not None]
        if clashing:
            remove = ", ".join(f'"{k}"' for k in clashing)
            raise ConfigError(
                f'table {self.name}: "query" replaces "schema"/"table", "columns" and "where"; '
                f"remove {remove} and put the projection and the filter inside the query"
            )

    def _check_keyset(self) -> None:
        where = f"table {self.name}: keyset chunking"
        cursor = self.cursor.column if self.cursor else "<cursor column>"
        if not self.chunking.order_by:
            raise ConfigError(f'{where} needs an order; set "order_by": ["{cursor}", <primary key columns>]')
        if self.cursor and self.chunking.order_by[0] != self.cursor.column:
            raise ConfigError(
                f'{where}: "order_by" {list(self.chunking.order_by)} does not start with the cursor; '
                f'start "order_by" with the cursor column "{cursor}", then the primary key columns'
            )
        if not self.primary_key:
            raise ConfigError(f'{where} breaks ties by the primary key; set "primary_key"')
        if not _positive_int(self.chunking.page_size):
            raise ConfigError(f'{where}: set "page_size" to a positive number of rows per page')


@dataclass(frozen=True)
class SourceConfig:
    pipeline_name: str  # STABLE: part of the state identity
    dataset_name: str  # STABLE: part of the state identity
    bucket_url: str  # STABLE: bronze root, e.g. "s3://bucket/bronze"
    connection: ConnectionConfig
    tables: tuple[TableConfig, ...]
    defaults: Defaults = field(default_factory=Defaults)
    parquet_compression: str = "zstd"
    max_time_s: float | None = None  # its rules belong to the time-budget work (section 7.4)

    def __post_init__(self) -> None:
        _check_name("pipeline_name", self.pipeline_name)
        _check_name("dataset_name", self.dataset_name)
        if not self.tables:
            raise ConfigError("tables: at least one is required")
        seen: set[str] = set()
        for t in self.tables:
            if t.name in seen:
                raise ConfigError(
                    f"duplicate table name {t.name!r}: each is a bronze path prefix; give each table its own name"
                )
            seen.add(t.name)
        if self.max_time_s is not None and not (_number(self.max_time_s) and self.max_time_s > 0):
            raise ConfigError(f"max_time_s must be a number of seconds > 0, or null: {self.max_time_s!r}")
        if self.parquet_compression not in PARQUET_CODECS:
            raise ConfigError(f"parquet_compression {self.parquet_compression!r} not in {PARQUET_CODECS}")
        if self.defaults.backend is not None and self.defaults.backend not in BACKENDS:
            raise ConfigError(f"defaults.backend {self.defaults.backend!r} not in {BACKENDS}")
        for t in self.tables:
            self._check_backend(t)

    def _check_backend(self, t: TableConfig) -> None:
        if t.backend is not None and t.backend not in BACKENDS:
            raise ConfigError(f"table {t.name}: backend {t.backend!r} not in {BACKENDS}")
        if t.partition and self.backend(t) != "connectorx":
            raise ConfigError(
                f"table {t.name}: partition is a ConnectorX read (partition_on), but the table resolves to "
                f'{self.backend(t)!r}; set "backend": "connectorx", or remove "partition"'
            )
        if self.backend(t) != "connectorx":
            return
        dialect = self.connection.dialect
        if dialect not in CONNECTORX_DIALECTS:
            raise ConfigError(
                f"table {t.name}: ConnectorX does not support dialect {dialect!r} (it reads {CONNECTORX_DIALECTS}); "
                'set "backend": "pyarrow" on the table or in defaults'
            )
        if self.connection.isolation_level is not None:
            # Until spike S3 records which engines let ConnectorX apply session settings.
            raise ConfigError(
                f"table {t.name}: ConnectorX cannot apply connection.isolation_level "
                f"{self.connection.isolation_level!r} (spike S3 is open for every engine); "
                'set "backend": "pyarrow" on the table or in defaults, or remove "isolation_level"'
            )
        if self.connection.auth.type == "kerberos" and dialect != "mssql":
            raise ConfigError(
                f"table {t.name}: ConnectorX supports Kerberos only on mssql, not {dialect!r} (docs/KERBEROS.md); "
                'set "backend": "pyarrow" on the table or in defaults'
            )

    def backend(self, table: TableConfig) -> str:
        """The table's backend, resolved from the config alone (D3, section 5.3)."""
        return table.backend or self.defaults.backend or default_backend(self.connection.dialect)

    def tables_for(self, req: RunRequest) -> tuple[TableConfig, ...]:
        """The tables one invocation runs: all of them, or the event's `tables` subset."""
        if req.tables is None:
            return self.tables
        known = [t.name for t in self.tables]
        unknown = [n for n in req.tables if n not in known]
        if unknown:
            raise ConfigError(f'event "tables": unknown tables {unknown}; this config has {known}')
        return tuple(t for t in self.tables if t.name in req.tables)

    def batch_size(self, table: TableConfig) -> int:
        return table.batch_size or self.defaults.batch_size

    @classmethod
    def from_dict(cls, data: dict) -> SourceConfig:
        if not isinstance(data, dict):
            raise ConfigError("config: expected an object")
        raw_conn = data.get("connection")
        auth = raw_conn.get("auth", {}) if isinstance(raw_conn, dict) else {}
        connection = _build(
            ConnectionConfig, raw_conn, "connection", auth=_build(AuthConfig, auth, "connection.auth")
        )
        defaults = _build(Defaults, data.get("defaults", {}), "defaults")
        raw_tables = data.get("tables")
        if not isinstance(raw_tables, list):
            raise ConfigError("tables: expected a list")
        tables = [_table(t, f"tables[{i}]") for i, t in enumerate(raw_tables)]
        return _build(cls, data, "config", connection=connection, defaults=defaults, tables=tuple(tables))

    @classmethod
    def from_json(cls, text: str | bytes) -> SourceConfig:
        try:
            data = json.loads(text)
        except ValueError as e:
            raise ConfigError(f"config is not valid JSON: {e}") from e
        return cls.from_dict(data)


def _table(t: Any, where: str) -> TableConfig:
    if not isinstance(t, dict):
        raise ConfigError(f"{where}: expected an object, got {type(t).__name__}")
    nested: dict[str, Any] = {}
    if t.get("cursor") is not None:
        nested["cursor"] = _build(CursorConfig, t["cursor"], f"{where}.cursor")
    if t.get("chunking") is not None:
        chunking = t["chunking"]
        order_by = chunking.get("order_by") if isinstance(chunking, dict) else None
        order_by = _names(order_by, f"{where}.chunking.order_by") if order_by is not None else ()
        nested["chunking"] = _build(ChunkingConfig, chunking, f"{where}.chunking", order_by=order_by)
    if t.get("partition") is not None:
        nested["partition"] = _build(PartitionConfig, t["partition"], f"{where}.partition")
    if t.get("primary_key") is not None:
        nested["primary_key"] = _names(t["primary_key"], f"{where}.primary_key")
    if t.get("columns") is not None:
        nested["columns"] = _names(t["columns"], f"{where}.columns")
    return _build(TableConfig, t, where, **nested)


@dataclass(frozen=True)
class RunRequest:
    """What one invocation should do. Built from the Lambda event."""

    mode: str = "incremental"
    tables: tuple[str, ...] | None = None  # a subset of the config's table names; None runs them all
    window_start: str | int | None = None  # backfill only, inclusive; ISO-8601 or integer, as the cursor
    window_end: str | int | None = None  # backfill only, exclusive
    plan: str | None = None  # mode=plan: "snapshot" (pages) or "backfill" (windows)
    page_size: int | None = None  # plan=snapshot: rows per page

    @property
    def partition_datetime(self) -> datetime | None:
        """The date bronze paths carry instead of the load date: a timestamp backfill's window start (UTC)."""
        if self.mode != "backfill" or not isinstance(self.window_start, str):
            return None
        dt = parse_iso(self.window_start, "window_start")
        return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)

    @classmethod
    def from_event(cls, event: dict) -> RunRequest:
        mode = event.get("mode", "incremental")
        if mode not in MODES:
            raise ConfigError(f"mode {mode!r} not in {MODES}")
        start, end = event.get("window_start"), event.get("window_end")
        if mode == "backfill":
            _check_window(start, end)
        elif start is not None or end is not None:
            raise ConfigError(f"window_start/window_end are only valid with mode=backfill, not {mode}")
        tables = event.get("tables")
        if tables is not None:
            if not isinstance(tables, list) or not tables or not all(isinstance(n, str) for n in tables):
                raise ConfigError(f'event "tables": expected a non-empty list of table names, got {tables!r}')
            tables = tuple(tables)
        plan, page_size = event.get("plan"), event.get("page_size")
        if mode == "plan":
            if plan not in PLANS:
                raise ConfigError(f'mode=plan needs "plan" in {PLANS}, got {plan!r}')
            if plan == "snapshot" and not _positive_int(page_size):
                raise ConfigError(f'plan=snapshot needs "page_size", a positive number of rows: {page_size!r}')
        _only_with(event, ("plan", "page_size"), mode == "plan", "mode=plan")
        return cls(mode, tables=tables, window_start=start, window_end=end, plan=plan, page_size=page_size)


def _only_with(event: dict, keys: tuple[str, ...], allowed: bool, what: str) -> None:
    """Reject event keys that belong to another kind of event, so a mistyped event fails instead of being ignored."""
    given = [k for k in keys if event.get(k) is not None]
    if given and not allowed:
        raise ConfigError(f"event keys {given} are only valid with {what}; remove them")


def _check_window(start: Any, end: Any) -> None:
    """A backfill window: both bounds, both integers or both ISO-8601, and end after start."""
    if start is None or end is None:
        raise ConfigError("mode=backfill needs window_start and window_end")
    if _is_int(start) and _is_int(end):
        ordered = end > start
    elif isinstance(start, str) and isinstance(end, str):
        s, e = parse_iso(start, "window_start"), parse_iso(end, "window_end")
        if (s.tzinfo is None) != (e.tzinfo is None):
            raise ConfigError("window_start and window_end must both carry an offset, or neither")
        ordered = e > s
    else:
        raise ConfigError(f"window_start and window_end must both be integers or both ISO-8601: {start!r}, {end!r}")
    if not ordered:
        raise ConfigError(f"window_end must be after window_start: [{start!r}, {end!r})")
