"""REST API -> raw, record-grain Parquet bronze with dlt. See docs/api/ (configuration.md, runs.md, design.md)."""

from .config import ConfigError, IncrementalConfig, ResourceConfig, RunRequest, SourceConfig
from .envelope import CURSOR_COLUMN, RECORD_SCHEMA, BrokenPageError, envelope_records
from .pipeline import LAYOUT, run_pipeline
from .split import SplitError, split_records, split_records_parsed

__all__ = [
    "CURSOR_COLUMN",
    "LAYOUT",
    "RECORD_SCHEMA",
    "BrokenPageError",
    "ConfigError",
    "IncrementalConfig",
    "ResourceConfig",
    "RunRequest",
    "SourceConfig",
    "SplitError",
    "envelope_records",
    "run_pipeline",
    "split_records",
    "split_records_parsed",
]
