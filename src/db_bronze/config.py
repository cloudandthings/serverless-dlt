"""Source config and run request: plain dataclasses, validated strictly (DESIGN-db.md section 5).

One config document = one database connection = one dlt pipeline = one Lambda
function. Unknown keys are rejected, so a typo fails loudly instead of silently
falling back to a default. Copied in style from `api_bronze.config` (D1).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from typing import Any

MODES = ("incremental", "full")
BACKENDS = ("connectorx", "pyarrow")
# Engines ConnectorX reads (section 3.1). Every other dialect defaults to pyarrow (D3, section 5.3).
CONNECTORX_DIALECTS = ("mssql", "postgres", "mysql", "oracle", "sqlite")
CURSOR_TYPES = ("timestamp", "integer")
PARQUET_CODECS = ("zstd", "snappy", "gzip", "none")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(ValueError):
    pass


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


@dataclass(frozen=True)
class ConnectionConfig:
    dialect: str  # only sqlite so far
    path: str | None = None  # SQLite database file

    def __post_init__(self) -> None:
        if self.dialect != "sqlite":
            raise ConfigError(f"connection.dialect {self.dialect!r}: only 'sqlite' is supported so far")
        if not self.path:
            raise ConfigError("connection.path is required for sqlite")


@dataclass(frozen=True)
class CursorConfig:
    column: str  # source column name, exactly as in the database
    type: str  # timestamp | integer
    initial_value: str | int  # ISO-8601 for timestamp cursors, an integer for integer cursors

    def __post_init__(self) -> None:
        if self.type not in CURSOR_TYPES:
            raise ConfigError(f"cursor.type {self.type!r} not in {CURSOR_TYPES}")
        if self.type == "integer" and (not isinstance(self.initial_value, int) or isinstance(self.initial_value, bool)):
            raise ConfigError(f"cursor.initial_value must be an integer for an integer cursor: {self.initial_value!r}")
        if self.type == "timestamp":
            self.initial_timestamp  # noqa: B018 (validates)

    @property
    def initial_timestamp(self) -> datetime:
        """The ISO initial value as a naive UTC timestamp (an offset, if any, is converted to UTC)."""
        try:
            dt = datetime.fromisoformat(str(self.initial_value))
        except ValueError as e:
            raise ConfigError(f"cursor.initial_value is not ISO-8601: {self.initial_value!r}") from e
        return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


@dataclass(frozen=True)
class Defaults:
    backend: str | None = None
    batch_size: int = 50_000


@dataclass(frozen=True)
class TableConfig:
    name: str  # bronze table name (path prefix)
    table: str  # source table name, exactly as in the database
    schema: str | None = None  # source schema; None is the connection's default
    primary_key: tuple[str, ...] = ()
    cursor: CursorConfig | None = None
    backend: str | None = None  # overrides defaults.backend
    batch_size: int | None = None  # rows per Arrow batch; overrides defaults.batch_size

    def __post_init__(self) -> None:
        if not _NAME.match(self.name):
            raise ConfigError(f"table name {self.name!r} must match {_NAME.pattern}")
        if self.cursor is None:
            raise ConfigError(f"table {self.name}: a cursor is required (snapshot tables are not supported yet)")
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

    def backend(self, table: TableConfig) -> str:
        """The table's backend, resolved from the config alone (D3): table, then defaults, then the dialect."""
        return table.backend or self.defaults.backend or default_backend(self.connection.dialect)

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
            tables.append(_build(TableConfig, t, where, cursor=cursor, primary_key=pk))
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
