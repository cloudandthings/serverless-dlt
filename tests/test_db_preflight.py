"""db_bronze: preflight rules against fake reflected metadata (DESIGN-db.md section 6.2)."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql, oracle
from sqlalchemy.dialects import postgresql as pg

from db_bronze.extract import register_mssql_types
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


def test_unconstrained_postgres_numeric_is_a_finding():
    # ConnectorX reads it as decimal(38,10) and drops digits past the 10th decimal place (integration type matrix).
    [finding] = column_findings("postgres", "connectorx", [("Any", sa.Numeric())])
    assert "Any" in finding and "NUMERIC without precision" in finding


def test_oracle_number_without_precision_is_a_finding_on_connectorx():
    """Integration: ConnectorX reads it as decimal128(38, 10) and drops digits past the 10th place silently."""
    [finding] = column_findings("oracle", "connectorx", [("Any", oracle.NUMBER())])
    assert "Any" in finding and "NUMERIC without precision" in finding


def test_oracle_integers_pass_whatever_their_precision():
    """ConnectorX reads NUMBER(p, 0) as int64; a wider value fails the read loudly (ConnectorXFailed)."""
    columns = [("A", oracle.NUMBER(19, 0)), ("B", oracle.NUMBER(38, 0)), ("C", sa.INTEGER())]
    assert column_findings("oracle", "connectorx", columns) == []


@pytest.mark.parametrize("dialect", ["sqlite", "mysql"])
def test_decimal_with_no_declared_precision_passes_elsewhere(dialect):
    assert column_findings(dialect, "connectorx", [("Ok", sa.Numeric())]) == []


def test_decimal_rule_does_not_apply_to_pyarrow():
    assert column_findings("postgres", "pyarrow", [("Big", sa.Numeric(38, 12))]) == []


DENIED = {
    "postgres": [
        pg.INTERVAL(),
        pg.MONEY(),
        sa.Time(timezone=True),
        pg.MACADDR(),
        pg.MACADDR8(),
        pg.CIDR(),
        pg.BIT(varying=True),
        pg.TSVECTOR(),
        pg.TSQUERY(),
        sa.String(),  # how SQLAlchemy reflects "char" (and name)
        sa.ARRAY(sa.Uuid()),
        sa.ARRAY(sa.DateTime()),
        sa.ARRAY(sa.CHAR(2)),
        sa.ARRAY(pg.JSONB()),
        sa.ARRAY(sa.Numeric()),  # each element would lose digits, as for a NUMERIC column
        sa.ARRAY(sa.Numeric(38, 12)),
    ],
    # money: ConnectorX reads it as a double, which drops digits of its 19. datetimeoffset: ConnectorX
    # applies the offset twice, so the instant is wrong.
    "mssql": [mssql.XML(), mssql.MONEY(), mssql.DATETIMEOFFSET()],
    "oracle": [
        oracle.RAW(16),
        oracle.ROWID(),
        oracle.INTERVAL(),
        oracle.TIMESTAMP(local_timezone=True),
        oracle.NCLOB(),
        oracle.BFILE(),
        oracle.TIMESTAMP(timezone=True),  # integration: ConnectorX 0.4.6 fails on it
        sa.Boolean(),  # 23ai BOOLEAN
        oracle.JSON(),  # 23ai JSON
    ],
}


@pytest.mark.parametrize(
    ("dialect", "col_type"), [(d, t) for d, types in DENIED.items() for t in types], ids=lambda v: str(type(v).__name__)
)
def test_denied_types_are_findings_on_connectorx_only(dialect, col_type):
    assert len(column_findings(dialect, "connectorx", [("C", col_type)])) == 1
    assert column_findings(dialect, "pyarrow", [("C", col_type)]) == []


def reflected_mssql(name: str) -> sa.types.TypeEngine:
    """The type SQL Server reflection gives a column of `name`, with db_bronze's additions registered."""
    dialect = mssql.dialect()
    register_mssql_types(dialect)
    return dialect.ischema_names[name]()


@pytest.mark.parametrize("name", ["hierarchyid", "geography", "geometry"])
def test_sql_server_clr_types_are_recognised_and_denied_on_connectorx(name):
    # Unregistered, SQLAlchemy reflects them as NullType, which no deny-list entry can match.
    col_type = reflected_mssql(name)
    assert not isinstance(col_type, sa.types.NullType)
    [finding] = column_findings("mssql", "connectorx", [("C", col_type)])
    assert f"type {name} is not supported by ConnectorX" in finding
    assert column_findings("mssql", "pyarrow", [("C", col_type)]) == []  # pyarrow lands their bytes


def test_sql_variant_is_a_finding_on_every_backend():
    # pyodbc reads a sql_variant only as the value's bytes, without its type: nothing lands it losslessly.
    for backend in ("connectorx", "pyarrow"):
        [finding] = column_findings("mssql", backend, [("C", mssql.SQL_VARIANT())])
        assert "sql_variant" in finding and "cast it" in finding


def test_smallmoney_fits_a_double_and_passes():
    assert column_findings("mssql", "connectorx", [("C", mssql.SMALLMONEY())]) == []


@pytest.mark.parametrize(
    "item",
    [sa.SmallInteger(), sa.Integer(), sa.BigInteger(), sa.Boolean(), pg.REAL(), pg.DOUBLE_PRECISION(),
     sa.Numeric(10, 2), sa.VARCHAR(5), sa.TEXT()],
    ids=lambda v: type(v).__name__,
)
def test_postgres_arrays_of_elements_connectorx_reads_pass(item):
    assert column_findings("postgres", "connectorx", [("C", sa.ARRAY(item))]) == []


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "mssql", "oracle"])
def test_a_type_sqlalchemy_does_not_recognise_is_a_finding_on_connectorx_only(dialect):
    # Postgres xml, geometric types, pg_lsn and composite types; MySQL geometry types (integration type matrix)
    [finding] = column_findings(dialect, "connectorx", [("C", sa.types.NullType())])
    assert "C" in finding and "not recognised" in finding
    assert column_findings(dialect, "pyarrow", [("C", sa.types.NullType())]) == []


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


def test_a_query_tables_columns_are_not_checked_their_types_are_not_reflected(tmp_path):
    # A server dialect's rules over a SQLite engine: a query's result columns carry no reflected type,
    # which must not read as "a type SQLAlchemy does not recognise".
    from db_bronze.config import SourceConfig
    from db_bronze.preflight import preflight

    engine = sa.create_engine(f"sqlite:///{tmp_path / 's.sqlite'}")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, at DATETIME, label TEXT)")
    cfg = SourceConfig.from_dict({
        "pipeline_name": "p", "dataset_name": "p", "bucket_url": "file:///b",
        "connection": {"dialect": "postgres", "host": "h", "database": "d"},
        "tables": [{"name": "q", "query": "SELECT id, at, label FROM t", "backend": "connectorx",
                    "cursor": {"column": "at", "type": "timestamp", "initial_value": "2026-01-01"}}],
    })
    _, [report] = preflight(cfg, cfg.tables, engine)
    assert report.findings == []
