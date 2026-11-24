"""db_bronze: backend resolution from the config alone (DESIGN-db.md D3, section 5.3)."""

from __future__ import annotations

import pytest

from db_bronze.config import ConfigError, SourceConfig, default_backend


@pytest.mark.parametrize(
    ("dialect", "backend"),
    [
        ("mssql", "connectorx"),
        ("postgres", "connectorx"),
        ("mysql", "connectorx"),
        ("oracle", "connectorx"),
        ("sqlite", "connectorx"),
        ("db2", "pyarrow"),
        ("snowflake", "pyarrow"),
    ],
)
def test_default_backend_comes_from_the_dialect(dialect, backend):
    assert default_backend(dialect) == backend


def config(defaults_backend: str | None, table_backend: str | None) -> SourceConfig:
    table = {"name": "t", "table": "T", "cursor": {"column": "Id", "type": "integer", "initial_value": 0}}
    return SourceConfig.from_dict(
        {
            "pipeline_name": "p",
            "dataset_name": "p",
            "bucket_url": "file:///bronze",
            "connection": {"dialect": "sqlite", "path": "/db.sqlite"},
            "defaults": {"backend": defaults_backend},
            "tables": [table | {"backend": table_backend}],
        }
    )


@pytest.mark.parametrize(
    ("defaults_backend", "table_backend", "resolved"),
    [
        (None, None, "connectorx"),  # sqlite's default
        ("pyarrow", None, "pyarrow"),
        (None, "pyarrow", "pyarrow"),
        ("pyarrow", "connectorx", "connectorx"),  # the table wins
        ("connectorx", "pyarrow", "pyarrow"),
    ],
)
def test_table_backend_wins_over_defaults_which_win_over_the_dialect(defaults_backend, table_backend, resolved):
    cfg = config(defaults_backend, table_backend)
    assert cfg.backend(cfg.tables[0]) == resolved


def test_unknown_backend_is_rejected():
    with pytest.raises(ConfigError, match="backend 'duckdb'"):
        config(None, "duckdb")
