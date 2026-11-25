"""README-db.md documents everything the code accepts: every config key, event key, mode, error type and metric.

A key added to the config or the event without a README entry fails here, so the operator spec cannot
fall behind the code. (The reverse, nothing documented that is unimplemented, is checked by review.)
"""

from __future__ import annotations

import re
from dataclasses import fields
from pathlib import Path

import pytest

from db_bronze import config, errors, preflight
from db_bronze.config import EVENT_KEYS, MODES

README = (Path(__file__).resolve().parent.parent / "README-db.md").read_text()

CONFIG_CLASSES = (
    config.SourceConfig,
    config.ConnectionConfig,
    config.AuthConfig,
    config.Defaults,
    config.TableConfig,
    config.CursorConfig,
    config.ChunkingConfig,
    config.PartitionConfig,
    config.Page,
)


def documented(name: str) -> bool:
    return f"`{name}`" in README


@pytest.mark.parametrize("cls", CONFIG_CLASSES, ids=lambda c: c.__name__)
def test_every_config_key_is_documented(cls):
    missing = [f.name for f in fields(cls) if not documented(f.name)]
    assert not missing, f"{cls.__name__} keys missing from README-db.md: {missing}"


def test_every_event_key_is_documented():
    assert not [k for k in sorted(EVENT_KEYS) if not documented(k)]


def test_every_mode_and_plan_is_documented():
    assert not [m for m in (*MODES, *config.PLANS) if not documented(m)]


def test_every_enumerated_value_is_documented():
    values = (*config.BACKENDS, *config.CURSOR_TYPES, *config.CHUNKING_STRATEGIES, *config.AUTH_TYPES)
    values += (*config.ISOLATION_LEVELS, *config.PARQUET_CODECS)
    values += ("mssql", "postgres", "mysql", "oracle", "db2", "sqlite")
    assert not [v for v in values if v not in README]


def test_every_error_type_is_documented_with_a_fix():
    ours = [c for c in vars(errors).values() if isinstance(c, type) and issubclass(c, errors.DbBronzeError)]
    names = {c.__name__ for c in ours if c is not errors.DbBronzeError}
    names |= {config.ConfigError.__name__, preflight.PreflightError.__name__}
    names |= {"NotImplementedError", "PipelineStepFailed"}
    section = README.split("## Errors", 1)[1]
    assert not [n for n in sorted(names) if f"`{n}`" not in section]


def test_every_metric_is_documented():
    metrics = {c.metric for c in vars(errors).values() if isinstance(c, type) and getattr(c, "metric", None)}
    assert not [m for m in sorted(metrics | {"RowsExtracted", "NullCursorRows"}) if not documented(m)]


def test_server_minimums_match_the_code():
    for dialect, minimum in preflight.MIN_SERVER_VERSION.items():
        if minimum:
            row = re.search(rf"\| {dialect} \|[^\n]*", README)
            assert row and ".".join(map(str, minimum)) in row.group(0), dialect
