"""Source config and run request: plain dataclasses, validated strictly (DESIGN-db.md section 5).

One config document = one database connection = one dlt pipeline = one Lambda
function. Unknown keys are rejected, so a typo fails loudly instead of silently
falling back to a default. Copied in style from `api_bronze.config` (D1).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MODES = ("incremental",)
BACKENDS = ("pyarrow",)
CURSOR_TYPES = ("timestamp", "integer")
PARQUET_CODECS = ("zstd", "snappy", "gzip", "none")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


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


@dataclass(frozen=True)
class ConnectionConfig:
    dialect: str  # only sqlite so far
    path: str | None = None  # SQLite database file
    naive_timezone: str = "UTC"  # what naive source timestamps mean; converts cursor bounds only

    def __post_init__(self) -> None:
        try:
            ZoneInfo(self.naive_timezone)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ConfigError(
                f"connection.naive_timezone {self.naive_timezone!r} is not an IANA time zone, "
                "e.g. 'Africa/Johannesburg'"
            ) from e
        if self.dialect != "sqlite":
            raise ConfigError(f"connection.dialect {self.dialect!r}: only 'sqlite' is supported so far")
        if not self.path:
            raise ConfigError("connection.path is required for sqlite")


@dataclass(frozen=True)
class CursorConfig:
    column: str  # source column name, exactly as in the database
    type: str  # timestamp | integer
    initial_value: str | int  # ISO-8601 for timestamp cursors, an integer for integer cursors
    # Seconds (timestamp) or an integer delta: re-read this trailing window on every run. dlt then
    # deduplicates nothing, so the whole window lands again each run (prototype S4, DESIGN-db.md 12).
    lag: float | int | None = None
    include_null_cursor: bool = False  # adds `OR cursor IS NULL`: those rows land on every run

    def __post_init__(self) -> None:
        if self.type not in CURSOR_TYPES:
            raise ConfigError(f"cursor.type {self.type!r} not in {CURSOR_TYPES}")
        if self.type == "integer" and (not isinstance(self.initial_value, int) or isinstance(self.initial_value, bool)):
            raise ConfigError(f"cursor.initial_value must be an integer for an integer cursor: {self.initial_value!r}")
        if self.type == "timestamp":
            parse_iso(self.initial_value, "cursor.initial_value")

    def initial_cursor_value(self, naive_timezone: str) -> datetime | int:
        """The initial value as the source represents it: an integer, or a naive timestamp in source time."""
        if self.type == "integer":
            return self.initial_value
        return to_source_time(parse_iso(self.initial_value, "cursor.initial_value"), naive_timezone)


@dataclass(frozen=True)
class Defaults:
    backend: str | None = None
    batch_size: int = 50_000


@dataclass(frozen=True)
class TableConfig:
    name: str  # bronze table name (path prefix)
    table: str | None = None  # source table name, exactly as in the database
    schema: str | None = None  # source schema; None is the connection's default
    primary_key: tuple[str, ...] = ()
    cursor: CursorConfig | None = None
    backend: str | None = None  # overrides defaults.backend
    batch_size: int | None = None  # rows per Arrow batch; overrides defaults.batch_size
    columns: tuple[str, ...] | None = None  # projection; None is every reflected column
    where: str | None = None  # static predicate, ANDed with the cursor filter
    query: str | None = None  # instead of schema + table: a view or join; needs a cursor

    def __post_init__(self) -> None:
        if not _NAME.match(self.name):
            raise ConfigError(f"table name {self.name!r} must match {_NAME.pattern}")
        if self.cursor is None:
            raise ConfigError(f"table {self.name}: a cursor is required (snapshot tables are not supported yet)")
        if (self.table is None) == (self.query is None):
            raise ConfigError(f"table {self.name}: set either `table` (with optional `schema`) or `query`")
        if self.batch_size is not None and (not isinstance(self.batch_size, int) or self.batch_size < 1):
            raise ConfigError(f"table {self.name}: batch_size must be a positive integer")


@dataclass(frozen=True)
class SourceConfig:
    pipeline_name: str  # STABLE: part of the state identity
    dataset_name: str  # STABLE: part of the state identity
    bucket_url: str  # STABLE: bronze root, e.g. "s3://bucket/bronze"
    connection: ConnectionConfig
    tables: tuple[TableConfig, ...]
    defaults: Defaults = field(default_factory=Defaults)
    parquet_compression: str = "zstd"

    def __post_init__(self) -> None:
        for n in (self.pipeline_name, self.dataset_name):
            if not _NAME.match(n):
                raise ConfigError(f"name {n!r} must match {_NAME.pattern}")
        if not self.tables:
            raise ConfigError("tables: at least one is required")
        names = [t.name for t in self.tables]
        if len(set(names)) != len(names):
            raise ConfigError(f"table names must be unique: {names}")
        if self.parquet_compression not in PARQUET_CODECS:
            raise ConfigError(f"parquet_compression {self.parquet_compression!r} not in {PARQUET_CODECS}")
        for t in self.tables:
            backend = self.backend(t)
            if backend not in BACKENDS:
                raise ConfigError(f"table {t.name}: backend {backend!r} not in {BACKENDS}")

    def backend(self, table: TableConfig) -> str | None:
        return table.backend or self.defaults.backend

    def batch_size(self, table: TableConfig) -> int:
        return table.batch_size or self.defaults.batch_size

    @classmethod
    def from_dict(cls, data: dict) -> SourceConfig:
        if not isinstance(data, dict):
            raise ConfigError("config: expected an object")
        connection = _build(ConnectionConfig, data.get("connection"), "connection")
        defaults = _build(Defaults, data.get("defaults", {}), "defaults")
        raw_tables = data.get("tables")
        if not isinstance(raw_tables, list):
            raise ConfigError("tables: expected a list")
        tables = []
        for i, t in enumerate(raw_tables):
            where = f"tables[{i}]"
            cur = t.get("cursor") if isinstance(t, dict) else None
            cursor = _build(CursorConfig, cur, f"{where}.cursor") if cur is not None else None
            pk = tuple(t.get("primary_key") or ()) if isinstance(t, dict) else ()
            columns = t.get("columns") if isinstance(t, dict) else None
            columns = tuple(columns) if columns is not None else None
            tables.append(_build(TableConfig, t, where, cursor=cursor, primary_key=pk, columns=columns))
        return _build(
            cls, data, "config", connection=connection, defaults=defaults, tables=tuple(tables)
        )

    @classmethod
    def from_json(cls, text: str | bytes) -> SourceConfig:
        return cls.from_dict(json.loads(text))


@dataclass(frozen=True)
class RunRequest:
    """What one invocation should do. Built from the Lambda event."""

    mode: str = "incremental"

    @classmethod
    def from_event(cls, event: dict) -> RunRequest:
        mode = event.get("mode", "incremental")
        if mode not in MODES:
            raise ConfigError(f"mode {mode!r} not in {MODES}")
        return cls(mode)
