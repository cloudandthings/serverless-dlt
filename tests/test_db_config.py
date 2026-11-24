"""db_bronze: config schema and static validation (DESIGN-db.md sections 5 and 6.1). Offline: no database."""

from __future__ import annotations

import copy

import pytest

from db_bronze import ConfigError, SourceConfig

# The section 5 example. One deviation: the example sets `isolation_level` on an mssql connection whose
# `transactions` table resolves to connectorx, which validation rejects until spike S3 says which engines
# can honour it. Here the transactions table is pinned to pyarrow.
EXAMPLE = {
    "pipeline_name": "core_banking",
    "dataset_name": "core_banking",
    "bucket_url": "s3://bronze-bucket/bronze",
    "parquet_compression": "zstd",
    "max_time_s": None,
    "connection": {
        "dialect": "mssql",
        "host": "sql01.corp.example",
        "port": 1433,
        "database": "CoreBanking",
        "username": {"secret_id": "ingest/core_banking", "key": "username"},
        "password": {"secret_id": "ingest/core_banking", "key": "password"},
        "auth": {"type": "password"},
        "driver_options": {"Encrypt": "yes"},
        "isolation_level": "READ COMMITTED",
        "naive_timezone": "Africa/Johannesburg",
    },
    "defaults": {"backend": None, "batch_size": 50000},
    "tables": [
        {
            "name": "transactions",
            "schema": "dbo",
            "table": "Transactions",
            "primary_key": ["TransactionId"],
            "columns": None,
            "where": "BranchCode <> 'TEST'",
            "cursor": {
                "column": "UpdatedAt",
                "type": "timestamp",
                "initial_value": "2020-01-01T00:00:00Z",
                "lag": 300,
                "include_null_cursor": False,
            },
            "chunking": {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 200000},
            "backend": "pyarrow",
        },
        {
            "name": "branches",
            "schema": "dbo",
            "table": "Branches",
            "primary_key": ["BranchCode"],
            "backend": "pyarrow",
        },
    ],
}


def example(**changes) -> dict:
    return copy.deepcopy(EXAMPLE) | changes


def test_the_section_5_example_loads():
    cfg = SourceConfig.from_dict(example())
    conn = cfg.connection
    assert (conn.dialect, conn.host, conn.port, conn.database) == ("mssql", "sql01.corp.example", 1433, "CoreBanking")
    assert conn.password == {"secret_id": "ingest/core_banking", "key": "password"}  # resolved at run time
    assert conn.auth.type == "password"
    assert conn.driver_options == {"Encrypt": "yes"}
    assert conn.isolation_level == "READ COMMITTED"
    assert conn.naive_timezone == "Africa/Johannesburg"
    assert cfg.max_time_s is None
    tx, branches = cfg.tables
    assert tx.where == "BranchCode <> 'TEST'"
    assert tx.cursor.lag == 300 and tx.cursor.include_null_cursor is False
    assert tx.chunking.strategy == "keyset"
    assert tx.chunking.order_by == ("UpdatedAt", "TransactionId")
    assert tx.chunking.page_size == 200000
    assert branches.cursor is None
    assert branches.chunking.strategy == "stream"


CONNECTIONS = {
    "mssql": {"dialect": "mssql", "host": "db.example", "database": "core"},
    "postgres": {"dialect": "postgres", "host": "db.example", "database": "core"},
    "mysql": {"dialect": "mysql", "host": "db.example", "database": "core"},
    "oracle": {"dialect": "oracle", "host": "db.example", "service_name": "CORE"},
    "db2": {"dialect": "db2", "host": "db.example", "database": "core"},
    "sqlite": {"dialect": "sqlite", "path": "/data/core.sqlite"},
    "snowflake": {"dialect": "snowflake", "host": "acct.example", "database": "core"},
}
CONNECTORX_DIALECTS = ["mssql", "postgres", "mysql", "oracle", "sqlite"]


def config(dialect: str = "sqlite", connection: dict | None = None, defaults: dict | None = None, **table) -> dict:
    """A one-table config: an incremental `transactions` table, with `table` overriding its keys."""
    tx = {
        "name": "transactions",
        "table": "Transactions",
        "primary_key": ["TransactionId"],
        "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"},
    }
    return {
        "pipeline_name": "core_banking",
        "dataset_name": "core_banking",
        "bucket_url": "s3://bronze/bronze",
        "connection": CONNECTIONS[dialect] | (connection or {}),
        "defaults": defaults or {},
        "tables": [{k: v for k, v in (tx | table).items() if v is not None}],
    }


def rejected(cfg: dict) -> str:
    with pytest.raises(ConfigError) as e:
        SourceConfig.from_dict(cfg)
    return str(e.value)


@pytest.mark.parametrize("dialect", ["db2", "snowflake"])
@pytest.mark.parametrize("where", ["table", "defaults"])
def test_connectorx_on_an_engine_it_does_not_support_is_rejected(dialect, where):
    if where == "table":
        cfg = config(dialect, backend="connectorx")
    else:
        cfg = config(dialect, defaults={"backend": "connectorx"})
    message = rejected(cfg)
    assert "ConnectorX does not support" in message
    assert 'set "backend": "pyarrow"' in message


KERBEROS = {"type": "kerberos", "principal": "svc_ingest@CORP.EXAMPLE", "keytab": {"secret_id": "ingest/keytab"}}


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "oracle", "sqlite"])
def test_connectorx_with_kerberos_is_rejected_except_on_mssql(dialect):
    message = rejected(config(dialect, connection={"auth": KERBEROS}))  # resolves to connectorx
    assert "ConnectorX supports Kerberos only on mssql" in message
    assert 'set "backend": "pyarrow"' in message


def test_kerberos_parses_on_mssql_connectorx_and_on_pyarrow():
    cfg = SourceConfig.from_dict(config("mssql", connection={"auth": KERBEROS}))
    assert cfg.connection.auth.type == "kerberos"
    assert cfg.connection.auth.principal == "svc_ingest@CORP.EXAMPLE"
    SourceConfig.from_dict(config("postgres", connection={"auth": KERBEROS}, backend="pyarrow"))


@pytest.mark.parametrize(
    "auth,match",
    [
        ({"type": "ntlm"}, "auth.type 'ntlm' not in"),
        ({"type": "password", "principal": "svc@CORP"}, "only kerberos takes"),
        ({"type": "kerberos", "keytab": "/etc/krb5.keytab"}, "keytab must be a secret reference"),
    ],
)
def test_auth_is_tagged_by_type(auth, match):
    assert match in rejected(config("mssql", connection={"auth": auth}))


def keyset(**chunking) -> dict:
    return {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 1000} | chunking


@pytest.mark.parametrize(
    "table,fix",
    [
        ({"chunking": keyset(order_by=None)}, 'set "order_by": ["UpdatedAt", <primary key columns>]'),
        ({"chunking": keyset(order_by=["TransactionId", "UpdatedAt"])}, 'start "order_by" with the cursor column'),
        ({"chunking": keyset(), "primary_key": []}, 'set "primary_key"'),
        ({"chunking": keyset(page_size=None)}, 'set "page_size"'),
        ({"chunking": keyset(page_size=0)}, 'set "page_size"'),
        ({"chunking": keyset(), "columns": ["UpdatedAt", "Amount"]}, 'add ["TransactionId"] to "columns"'),
        ({"chunking": {"strategy": "pages"}}, "chunking.strategy 'pages' not in"),
        ({"chunking": {"strategy": "stream", "page_size": 10}}, "remove them or use keyset"),
    ],
)
def test_keyset_chunking_rules(table, fix):
    assert fix in rejected(config(backend="pyarrow", **table))


def test_keyset_led_by_the_cursor_with_a_primary_key_loads():
    cfg = SourceConfig.from_dict(config(chunking=keyset()))
    assert cfg.tables[0].chunking.order_by == ("UpdatedAt", "TransactionId")


def cursor(**changes) -> dict:
    return {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2026-01-01T00:00:00Z"} | changes


def test_lag_without_a_primary_key_is_rejected():
    message = rejected(config(cursor=cursor(lag=300), primary_key=[]))
    assert "lag" in message and 'set "primary_key"' in message


def test_zero_lag_needs_no_primary_key():
    SourceConfig.from_dict(config(cursor=cursor(lag=0), primary_key=[], backend="pyarrow"))


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"lag": -1}, "cursor.lag must be a number >= 0"),
        ({"lag": "5m"}, "cursor.lag must be a number >= 0"),
        ({"type": "integer", "initial_value": 0, "lag": 1.5}, "an integer cursor needs an integer lag"),
        ({"include_null_cursor": "yes"}, "include_null_cursor must be true or false"),
    ],
)
def test_cursor_value_rules(changes, match):
    assert match in rejected(config(cursor=cursor(**changes)))


PARTITION = {"column": "TransactionId", "num": 4}


@pytest.mark.parametrize("backend", ["pyarrow", "sqlalchemy"])
def test_partition_needs_connectorx(backend):
    message = rejected(config("postgres", backend=backend, partition=PARTITION))
    assert "partition is a ConnectorX read" in message and 'set "backend": "connectorx"' in message


def test_partition_needs_stream_chunking():
    message = rejected(config("postgres", partition=PARTITION, chunking=keyset()))
    assert "partition" in message and 'use "chunking": {"strategy": "stream"}' in message


@pytest.mark.parametrize("num", [0, -2, "4", 2.5])
def test_partition_num_is_a_positive_integer(num):
    message = rejected(config("postgres", partition=PARTITION | {"num": num}))
    assert "partition.num must be a positive integer" in message


def test_partition_on_connectorx_stream_loads():
    cfg = SourceConfig.from_dict(config("postgres", partition=PARTITION))
    assert cfg.tables[0].partition.num == 4


QUERY = 'SELECT t.*, b.Region FROM dbo.Transactions t JOIN dbo.Branches b ON b.BranchCode = t.BranchCode'


@pytest.mark.parametrize(
    "extra",
    [{"table": "Transactions"}, {"schema": "dbo"}, {"columns": ["TransactionId"]}, {"where": "Region = 'ZA'"}],
)
def test_query_excludes_schema_table_columns_and_where(extra):
    message = rejected(config(**{"table": None, "query": QUERY} | extra))
    assert f'remove "{next(iter(extra))}"' in message
    assert "put the projection and the filter inside the query" in message


def test_a_table_needs_a_source():
    message = rejected(config(table=None))
    assert 'set "table" (and optionally "schema"), or "query"' in message


def test_query_table_loads():
    cfg = SourceConfig.from_dict(config(table=None, query=QUERY))
    assert cfg.tables[0].query == QUERY and cfg.tables[0].table is None


@pytest.mark.parametrize(
    "table",
    [
        {"columns": ["TransactionId", "_extracted_at"]},
        {"cursor": cursor(column="_extracted_at")},
        {"primary_key": ["_extracted_at"]},
    ],
)
def test_a_source_column_named_extracted_at_is_rejected(table):
    message = rejected(config(**table))
    assert "_extracted_at" in message
    assert 'rename it in a "query" (SELECT "_extracted_at" AS ...)' in message


NAME_FIX = "use lowercase letters, digits and underscores, starting with a letter"


@pytest.mark.parametrize("key", ["pipeline_name", "dataset_name"])
@pytest.mark.parametrize("name", ["CoreBanking", "core-banking", "1core", ""])
def test_pipeline_and_dataset_names_are_lowercase_identifiers(key, name):
    message = rejected(config() | {key: name})
    assert key in message and NAME_FIX in message


def test_table_names_are_lowercase_identifiers():
    assert NAME_FIX in rejected(config(name="Transactions"))


def test_duplicate_table_names_are_rejected():
    cfg = config()
    cfg["tables"].append(dict(cfg["tables"][0]))
    message = rejected(cfg)
    assert "duplicate table name 'transactions'" in message and "give each table its own name" in message


@pytest.mark.parametrize("dialect", CONNECTORX_DIALECTS)
def test_isolation_level_with_connectorx_is_rejected_on_every_engine(dialect):
    message = rejected(config(dialect, connection={"isolation_level": "READ COMMITTED"}))  # resolves to connectorx
    assert "isolation_level" in message
    assert 'set "backend": "pyarrow" on the table or in defaults, or remove "isolation_level"' in message


@pytest.mark.parametrize("dialect", CONNECTORX_DIALECTS)
def test_isolation_level_on_the_traditional_backend_loads(dialect):
    cfg = SourceConfig.from_dict(config(dialect, connection={"isolation_level": "SERIALIZABLE"}, backend="pyarrow"))
    assert cfg.connection.isolation_level == "SERIALIZABLE"


def test_isolation_level_must_be_one_sqlalchemy_knows():
    message = rejected(config(connection={"isolation_level": "read committed"}, backend="pyarrow"))
    assert "isolation_level 'read committed' not in" in message


@pytest.mark.parametrize(
    "dialect,connection,fix",
    [
        ("sqlite", {"path": None}, 'set "path" to the database file'),
        ("sqlite", {"host": "db.example"}, 'sqlite takes only "path"; remove "host"'),
        ("oracle", {"database": "CORE"}, 'oracle takes "service_name", not "database"'),
        ("oracle", {"service_name": None}, 'set "service_name"'),
        ("mssql", {"host": None}, 'set "host"'),
        ("postgres", {"database": None}, 'set "database"'),
        ("mysql", {"service_name": "x"}, 'only oracle takes "service_name"'),
        ("db2", {"path": "/x"}, 'only sqlite takes "path"'),
        ("postgres", {"port": "5432"}, "connection.port must be an integer or a secret reference"),
        ("postgres", {"port": 70000}, "connection.port must be an integer or a secret reference"),
        ("postgres", {"password": {"secret": "x"}}, "connection.password must be a string or a secret reference"),
        ("postgres", {"username": 7}, "connection.username must be a string or a secret reference"),
        ("postgres", {"naive_timezone": "Mars/Olympus_Mons"}, "not an IANA time zone"),
        ("postgres", {"dialect": "Postgres SQL"}, "connection.dialect 'Postgres SQL' is not a SQLAlchemy dialect"),
        ("mssql", {"driver_options": {"Encrypt": ["yes"]}}, "driver_options['Encrypt'] must be a string"),
        ("mssql", {"driver_options": ["Encrypt=yes"]}, "connection.driver_options must be an object"),
    ],
)
def test_connection_fields(dialect, connection, fix):
    cfg = config(dialect, backend="pyarrow")
    cfg["connection"] = {k: v for k, v in (cfg["connection"] | connection).items() if v is not None}
    assert fix in rejected(cfg)


def test_every_connection_field_may_be_a_secret_reference():
    refs = {
        "host": {"env": "DB_HOST"},
        "port": {"env": "DB_PORT"},
        "database": {"secret_id": "ingest/core", "key": "database"},
        "username": {"secret_id": "ingest/core", "key": "username"},
        "password": {"secret_id": "ingest/core"},
        "driver_options": {"Encrypt": "yes", "PWD": {"env": "X"}},
    }
    cfg = SourceConfig.from_dict(config("mssql", connection=refs, backend="pyarrow"))
    assert cfg.connection.port == {"env": "DB_PORT"}  # resolved at run time, never at load


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"max_time_s": "600"}, "max_time_s must be a number of seconds > 0"),
        ({"max_time_s": 0}, "max_time_s must be a number of seconds > 0"),
        ({"defaults": {"batch_size": 0}}, "defaults.batch_size must be a positive integer"),
        ({"parquet_compression": "lz4"}, "parquet_compression 'lz4' not in"),
        ({"pipline_name": "typo"}, "unknown keys ['pipline_name']"),
        ({"tables": []}, "at least one"),
    ],
)
def test_top_level_values(changes, match):
    assert match in rejected(config(backend="pyarrow") | changes)


@pytest.mark.parametrize("where", ["table", "defaults", "dialect"])
def test_max_time_s_with_a_connectorx_stream_table_is_rejected_with_the_fix(where):
    backend = {"table": {"backend": "connectorx"}, "defaults": {}, "dialect": {}}[where]
    defaults = {"backend": "connectorx"} if where == "defaults" else None
    cfg = config("postgres", defaults=defaults, **backend) | {"max_time_s": 600}
    message = rejected(cfg)
    assert "max_time_s" in message and "transactions" in message
    assert '"strategy": "keyset"' in message


@pytest.mark.parametrize("table", [{"backend": "pyarrow"}, {"chunking": keyset()}])
def test_max_time_s_with_a_keyset_or_traditional_stream_table_loads(table):
    cfg = SourceConfig.from_dict(config("postgres", **table) | {"max_time_s": 600})
    assert cfg.max_time_s == 600
