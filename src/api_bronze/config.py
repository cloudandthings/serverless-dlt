"""Source config and run request: plain dataclasses, validated strictly.

One config document = one dlt pipeline = one Lambda function. Unknown keys are
rejected, so a typo fails loudly instead of silently falling back to a default.
See README.md "Config reference" for every field.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from bronze_common.config import strict_builder

from .envelope import CURSOR_TYPES, parse_cursor
from .split import path_keys

MODES = ("incremental", "full", "backfill")
PARAM_FORMATS = ("iso8601", "iso8601_z", "epoch_s", "epoch_ms")
PARQUET_CODECS = ("zstd", "snappy", "gzip", "none")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(ValueError):
    pass


_build = strict_builder(ConfigError)


def parse_utc(value: str | datetime, where: str) -> datetime:
    try:
        return value.astimezone(UTC) if isinstance(value, datetime) else parse_cursor(value, "iso8601")
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{where}: not an ISO-8601 timestamp: {value!r}") from e


def format_param(dt: datetime, fmt: str) -> str:
    """Render a cursor bound for the API query string."""
    dt = dt.astimezone(UTC)
    if fmt == "iso8601":
        return dt.isoformat()
    if fmt == "iso8601_z":
        return dt.isoformat().replace("+00:00", "Z")
    if fmt == "epoch_s":
        return str(int(dt.timestamp()))
    return str(int(dt.timestamp() * 1000))


@dataclass(frozen=True)
class IncrementalConfig:
    cursor_path: str  # dotted path to the cursor inside each record, e.g. "updatedAt"
    initial_value: str  # ISO-8601; where the first run (and mode=full) starts
    start_param: str  # query param that receives the lower bound
    end_param: str | None = None  # query param for the upper bound, if the API has one
    cursor_type: str = "iso8601"  # how the record's cursor is encoded: iso8601 | epoch_s | epoch_ms
    param_format: str = "iso8601"  # how bounds are sent: iso8601 | iso8601_z | epoch_s | epoch_ms
    sorted_ascending: bool = False  # API returns records in ascending cursor order (enables split loading)

    def __post_init__(self) -> None:
        if self.cursor_type not in CURSOR_TYPES:
            raise ConfigError(f"cursor_type {self.cursor_type!r} not in {CURSOR_TYPES}")
        if self.param_format not in PARAM_FORMATS:
            raise ConfigError(f"param_format {self.param_format!r} not in {PARAM_FORMATS}")
        parse_utc(self.initial_value, "initial_value")

    @property
    def initial(self) -> datetime:
        return parse_utc(self.initial_value, "initial_value")


@dataclass(frozen=True)
class ResourceConfig:
    name: str  # bronze table name, e.g. "orders_raw"
    path: str  # request path relative to client.base_url
    records_path: str  # dotted path to the records array, "$" if the body is the array
    id_path: str | None = None  # dotted path to the record id (null record_id if omitted)
    params: dict[str, Any] = field(default_factory=dict)  # static query params
    paginator: dict[str, Any] | None = None  # overrides client.paginator for this endpoint
    incremental: IncrementalConfig | None = None

    def __post_init__(self) -> None:
        if not _NAME.match(self.name):
            raise ConfigError(f"resource name {self.name!r} must match {_NAME.pattern}")
        try:
            path_keys(self.records_path)
        except ValueError as e:
            raise ConfigError(str(e)) from e


@dataclass(frozen=True)
class ClientConfig:
    base_url: str
    paginator: dict[str, Any]  # dlt rest_api paginator config, e.g. {"type": "json_link", ...}
    auth: dict[str, Any] | None = None  # dlt rest_api auth config; values may be secret refs
    headers: dict[str, Any] = field(default_factory=dict)  # values may be secret refs

    def __post_init__(self) -> None:
        if not isinstance(self.paginator, dict) or self.paginator.get("type", "auto") == "auto":
            raise ConfigError("client.paginator must name a type; auto-detection is not allowed")


@dataclass(frozen=True)
class SourceConfig:
    pipeline_name: str  # STABLE: part of the state identity
    dataset_name: str  # STABLE: part of the state identity
    bucket_url: str  # STABLE: bronze root, e.g. "s3://bucket/bronze"
    client: ClientConfig
    resources: tuple[ResourceConfig, ...]
    quarantine_url: str | None = None  # defaults to bucket_url
    parquet_compression: str = "zstd"
    max_time_s: float | None = None  # cap per run for split loading (sorted_ascending only)

    def __post_init__(self) -> None:
        for n in (self.pipeline_name, self.dataset_name):
            if not _NAME.match(n):
                raise ConfigError(f"name {n!r} must match {_NAME.pattern}")
        if not self.resources:
            raise ConfigError("resources: at least one is required")
        names = [r.name for r in self.resources]
        if len(set(names)) != len(names):
            raise ConfigError(f"resource names must be unique: {names}")
        if self.parquet_compression not in PARQUET_CODECS:
            raise ConfigError(f"parquet_compression {self.parquet_compression!r} not in {PARQUET_CODECS}")

    @classmethod
    def from_dict(cls, data: dict) -> SourceConfig:
        if not isinstance(data, dict):
            raise ConfigError("config: expected an object")
        client = _build(ClientConfig, data.get("client"), "client")
        raw_resources = data.get("resources")
        if not isinstance(raw_resources, list):
            raise ConfigError("resources: expected a list")
        resources = []
        for i, r in enumerate(raw_resources):
            where = f"resources[{i}]"
            inc = r.get("incremental") if isinstance(r, dict) else None
            inc_cfg = _build(IncrementalConfig, inc, f"{where}.incremental") if inc is not None else None
            resources.append(_build(ResourceConfig, r, where, incremental=inc_cfg))
        return _build(cls, data, "config", client=client, resources=tuple(resources))

    @classmethod
    def from_json(cls, text: str | bytes) -> SourceConfig:
        return cls.from_dict(json.loads(text))


@dataclass(frozen=True)
class RunRequest:
    """What one invocation should do. Built from the Lambda event."""

    mode: str = "incremental"
    window_start: datetime | None = None  # backfill only, inclusive
    window_end: datetime | None = None  # backfill only, exclusive

    @classmethod
    def from_event(cls, event: dict) -> RunRequest:
        mode = event.get("mode", "incremental")
        if mode not in MODES:
            raise ConfigError(f"mode {mode!r} not in {MODES}")
        if mode == "backfill":
            if not event.get("window_start") or not event.get("window_end"):
                raise ConfigError("mode=backfill needs window_start and window_end")
            start = parse_utc(event["window_start"], "window_start")
            end = parse_utc(event["window_end"], "window_end")
            if end <= start:
                raise ConfigError("window_end must be after window_start")
            return cls(mode, start, end)
        if event.get("window_start") or event.get("window_end"):
            raise ConfigError(f"window_start/window_end are only valid with mode=backfill, not {mode}")
        return cls(mode)
