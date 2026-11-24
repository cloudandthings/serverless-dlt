"""db_bronze: preflight rules against fake reflected metadata (DESIGN-db.md section 6.2)."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, oracle
from sqlalchemy.dialects import postgresql as pg

from db_bronze.preflight import MIN_SERVER_VERSION, PreflightError, column_findings, server_version_finding

OK = [("Id", sa.Integer()), ("Name", sa.String(20)), ("Amount", sa.Numeric(18, 2)), ("At", sa.DateTime())]


@pytest.mark.parametrize("dialect", ["mssql", "postgres", "mysql", "oracle", "sqlite"])
def test_ordinary_columns_pass_on_every_dialect(dialect):
    assert column_findings(dialect, "connectorx", OK) == []


@pytest.mark.parametrize("dialect", ["mssql", "postgres", "mysql", "oracle", "sqlite"])
@pytest.mark.parametrize(("precision", "scale"), [(38, 12), (20, 11), (30, 2), (29, 0)])
def test_decimal_beyond_connectorx_precision_is_a_finding(dialect, precision, scale):
    findings = column_findings(dialect, "connectorx", [("Big", sa.Numeric(precision, scale))])
    assert len(findings) == 1
    assert "Big" in findings[0]


@pytest.mark.parametrize(("precision", "scale"), [(28, 10), (18, 4), (10, 0)])
def test_decimal_within_the_limits_passes(precision, scale):
    assert column_findings("postgres", "connectorx", [("Ok", sa.Numeric(precision, scale))]) == []


def test_decimal_with_no_declared_precision_passes():
    assert column_findings("postgres", "connectorx", [("Ok", sa.Numeric())]) == []


def test_decimal_rule_does_not_apply_to_pyarrow():
    assert column_findings("postgres", "pyarrow", [("Big", sa.Numeric(38, 12))]) == []


DENIED = {
    "postgres": [pg.INTERVAL(), pg.MONEY(), sa.Time(timezone=True), pg.MACADDR(), sa.ARRAY(sa.Integer())],
    "mssql": [mssql.SQL_VARIANT(), mssql.XML()],
    "oracle": [
        oracle.RAW(16),
        oracle.ROWID(),
        oracle.INTERVAL(),
        oracle.TIMESTAMP(local_timezone=True),
        oracle.NCLOB(),
        oracle.BFILE(),
    ],
}


@pytest.mark.parametrize(
    ("dialect", "col_type"), [(d, t) for d, types in DENIED.items() for t in types], ids=lambda v: str(type(v).__name__)
)
def test_denied_types_are_findings_on_connectorx_only(dialect, col_type):
    assert len(column_findings(dialect, "connectorx", [("C", col_type)])) == 1
    assert column_findings(dialect, "pyarrow", [("C", col_type)]) == []


def test_a_type_denied_on_one_dialect_is_fine_on_another():
    assert column_findings("mysql", "connectorx", [("C", sa.Time(timezone=True))]) == []
    assert column_findings("oracle", "connectorx", [("C", pg.MONEY())]) == []


def test_a_column_named_extracted_at_is_a_finding_on_every_backend():
    for backend in ("connectorx", "pyarrow"):
        findings = column_findings("sqlite", backend, [("_extracted_at", sa.DateTime())])
        assert len(findings) == 1
        assert "_extracted_at" in findings[0]


def test_every_offending_column_is_listed_not_just_the_first():
    cols = [("A", sa.Numeric(38, 12)), ("B", pg.MONEY()), ("C", sa.Integer()), ("_extracted_at", sa.DateTime())]
    findings = column_findings("postgres", "connectorx", cols)
    assert len(findings) == 3
    assert [n in " ".join(findings) for n in "AB"] == [True, True]


def test_the_error_message_names_both_fixes_and_every_finding():
    err = PreflightError({"t1": ["column A: bad"], "t2": ["column B: worse"]})
    msg = str(err)
    assert "column A: bad" in msg and "column B: worse" in msg and "t1" in msg and "t2" in msg
    assert '"backend": "pyarrow"' in msg
    assert "exclud" in msg and "query" in msg


def test_the_minimum_version_table_has_an_entry_per_dialect():
    assert set(MIN_SERVER_VERSION) == {"mssql", "postgres", "mysql", "oracle", "sqlite", "db2"}


def test_a_server_below_the_minimum_is_a_finding():
    mins = {"postgres": (13, 0)}
    assert server_version_finding("postgres", (12, 9), mins) is not None
    assert "13" in server_version_finding("postgres", (12, 9), mins)
    assert server_version_finding("postgres", (13, 0), mins) is None
    assert server_version_finding("postgres", (16, 2), mins) is None


def test_no_minimum_means_no_check():
    assert server_version_finding("postgres", (1,), {"postgres": None}) is None
    assert server_version_finding("postgres", None, {"postgres": (13,)}) is None
