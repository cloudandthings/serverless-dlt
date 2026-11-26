# Database config reference (`db_bronze`)

One config document = one database connection = one dlt pipeline = one Lambda function.
Unknown keys are rejected, and every static rule fails with `ConfigError` and a message
that says how to fix it. Configs live at `${CONFIG_URI}/${source_id}.json` (any fsspec
URL) or inline in the event as `config`. Example: `examples/core_banking.json`.
JSON Schema: `schemas/db_bronze.config.schema.json` (see [../testing.md](../testing.md#config-schemas)).

```json
{
  "pipeline_name": "core_banking",
  "dataset_name": "core_banking",
  "bucket_url": "s3://bronze-bucket/bronze",
  "parquet_compression": "zstd",
  "max_time_s": null,
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
    "naive_timezone": "Africa/Johannesburg"
  },
  "defaults": {"backend": null, "batch_size": 50000},
  "tables": [
    {
      "name": "transactions",
      "schema": "dbo",
      "table": "Transactions",
      "primary_key": ["TransactionId"],
      "where": "BranchCode <> 'TEST'",
      "cursor": {"column": "UpdatedAt", "type": "timestamp", "initial_value": "2020-01-01T00:00:00Z", "lag": 300},
      "chunking": {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 200000}
    },
    {"name": "branches", "schema": "dbo", "table": "Branches", "primary_key": ["BranchCode"], "backend": "pyarrow"}
  ]
}
```

## Top level

| Key | Meaning |
|---|---|
| `pipeline_name` | Required. `^[a-z][a-z0-9_]*$`. **Stable**: part of the state identity. |
| `dataset_name` | Required. Same pattern. **Stable**: part of the state identity. |
| `bucket_url` | Required. Bronze root, e.g. `s3://bucket/bronze`. **Stable**: part of the state identity. |
| `connection` | Required. See below. |
| `tables` | Required, at least one. See below. |
| `defaults` | `backend` and `batch_size` for every table. |
| `parquet_compression` | `zstd` (default), `snappy`, `gzip` or `none`. |
| `max_time_s` | `null` (default) or seconds > 0: caps an incremental run, for split loading. Rejected while any table's stream cannot stop early (see [modes.md](modes.md#time-budget-and-re-invocation)). |

## `connection`

| Key | Meaning |
|---|---|
| `dialect` | Required. `mssql`, `postgres`, `mysql`, `oracle`, `db2`, `sqlite`, or another SQLAlchemy dialect name (traditional backend only). |
| `host`, `port`, `database` | Required with `host` + `database` for `mssql`, `postgres`, `mysql`, `db2`. |
| `service_name` | Oracle only, instead of `database` (host + `service_name` required). Oracle reads the connecting user's schema unless the table sets `schema`. |
| `path` | SQLite only: the database file. SQLite takes no other address key. |
| `username`, `password` | A literal, or a secret reference. Any connection field may be a reference: `{"env": "NAME"}`, `{"secret_id": "id"}` (whole SecretString) or `{"secret_id": "id", "key": "k"}` (one key of a JSON SecretString). Secrets Manager values are cached per container. |
| `auth` | `{"type": "password"}` (default). `{"type": "kerberos", "principal", "keytab", "krb5_conf"}` parses but fails at run start with `NotImplementedError`; see [kerberos.md](kerberos.md). `principal`, `keytab` (a secret reference) and `krb5_conf` are only valid with `kerberos`. |
| `driver_options` | Object of strings, numbers or secret references, passed to the traditional driver (e.g. ODBC keywords for SQL Server). Db2 passes on only `CurrentSchema`, `Security` and `SSL...` keywords and drops others silently; set a short schema per table with `schema` instead of `CurrentSchema`. |
| `isolation_level` | `READ UNCOMMITTED`, `READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE`, `SNAPSHOT` or `AUTOCOMMIT`. Applied to the traditional backend. ConnectorX applies it only on Postgres (the four standard levels) and SQL Server (five levels, not on a `partition` read); elsewhere the combination fails validation. Db2 takes the four standard levels only. |
| `naive_timezone` | IANA zone name, default `UTC`. See [../bronze-contract.md](../bronze-contract.md#database-rows-db_bronze). |

## `defaults`

| Key | Meaning |
|---|---|
| `backend` | `connectorx` or `pyarrow`; `null` resolves per table (below). `sqlalchemy` is not built yet and fails with `ConfigError`. |
| `batch_size` | Rows per Arrow batch, default 50000. A table's `batch_size` overrides it. |

## Per table (`tables[]`)

| Key | Meaning |
|---|---|
| `name` | Required. Bronze table name and path prefix, `^[a-z][a-z0-9_]*$`, unique. |
| `schema`, `table` | The source table, exactly as in the database. `schema` null is the connection's default schema. |
| `query` | Instead of `schema`/`table`: a view or join. Cannot be combined with `schema`, `table`, `columns` or `where` (put the projection and filter inside it). Needs a `cursor` to run incrementally. Its column types are not reflected, so the preflight cannot check them. |
| `primary_key` | List of source column names. Required for `lag`, keyset chunking and offset paging; also used for deduplication at the cursor boundary. |
| `columns` | Projection (null: every column). This is how a column ConnectorX cannot read is excluded. |
| `where` | Static SQL predicate, ANDed with the cursor filter. |
| `cursor` | Omit for a **snapshot table**, which runs in `full` mode only. See below. |
| `backend` | `connectorx` or `pyarrow`. Overrides `defaults.backend`. |
| `chunking` | `{"strategy": "stream"}` (default) or keyset. See below. |
| `batch_size` | Rows per Arrow batch (ConnectorX `batch_size`, or the fetch size on the traditional backend). |
| `partition` | `{"column": "...", "num": N}`: ConnectorX parallel read. See [modes.md](modes.md#partitioned-reads). |

`cursor`:

| Key | Meaning |
|---|---|
| `column` | Required. The source column, exactly as in the database. |
| `type` | Required. `timestamp` or `integer`. |
| `initial_value` | Required. ISO-8601 for `timestamp` (an offset is converted through `naive_timezone`), an integer for `integer`. Applies to the first incremental run, and is the lower bound of `full`. |
| `lag` | Seconds (`timestamp`) or an integer delta (`integer`); re-reads a trailing window to catch late commits. Needs `primary_key`. Re-lands the whole window every run (see [modes.md](modes.md#duplicates-from-lag)). |
| `include_null_cursor` | Default `false`; see [modes.md](modes.md#null-cursors). |

`chunking`:

| Key | Meaning |
|---|---|
| `strategy` | `stream` (default) or `keyset`. |
| `order_by` | Keyset only. Must start with the cursor column, then the primary key columns, and those columns must be in `columns` if set. |
| `page_size` | Keyset only. Positive number of rows per page. |

Keyset also requires `primary_key`.

`partition`: `column` (required; an integer column) and `num` (required; positive
number of parallel connections).

`page` (an event key, see [modes.md](modes.md)): `offset` (required;
rows skipped, integer >= 0) and `limit` (rows in the page, positive; `null` for the
open-ended last page).

**Default backend** is resolved from the config alone, so a config always gives the same
backend: `connectorx` for `mssql`, `postgres`, `mysql`, `oracle`, `sqlite`; `pyarrow`
for `db2` and any other dialect. There is no runtime fallback. (`sqlalchemy` is a
designed backend that is not built yet: the config rejects it with `ConfigError`; use
`pyarrow`.)

## Static validation rules

All fail with `ConfigError` before anything runs.

- `backend: connectorx` on an engine ConnectorX does not read (Db2, other dialects): set `"backend": "pyarrow"`.
- `backend: sqlalchemy` (not built yet): set `"backend": "pyarrow"`.
- `backend: connectorx` with `auth.type: kerberos` on any engine except SQL Server.
- `isolation_level` that ConnectorX cannot apply on that engine (anything on MySQL and Oracle): use `pyarrow`, or remove it. On SQL Server not on a table with `partition`.
- `isolation_level` `SNAPSHOT` or `AUTOCOMMIT` on `db2`.
- `partition` on anything but `connectorx` with `chunking: stream`.
- `max_time_s` with a table whose stream cannot stop early (below).
- keyset without `order_by`, with an `order_by` not starting with the cursor, without `primary_key`, without `page_size`, or with an `order_by` column outside `columns`; `order_by` or `page_size` with `stream`.
- `cursor.lag` without `primary_key`; a non-integer `lag` on an integer cursor.
- a source column named `_extracted_at` in `columns`, `primary_key`, `order_by`, `cursor` or `partition`.
- unknown keys; `name` not a lowercase identifier; duplicate `name`; `tables` empty.
- `service_name` on a non-Oracle dialect, `database` on Oracle, `path` on a non-SQLite dialect, server keys on SQLite; a missing `host`/`database`/`service_name`/`path`.
