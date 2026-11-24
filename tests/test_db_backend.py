"""db_bronze: backend resolution from the config alone (DESIGN-db.md D3, section 5.3)."""

from __future__ import annotations

import pytest

from db_bronze.config import default_backend


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
