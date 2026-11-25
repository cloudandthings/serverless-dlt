# db-bronze

Land relational database tables in the S3 bronze layer as **typed Parquet**, with dlt
doing the plumbing, on AWS Lambda. Source column names survive exactly; types are
the ones the backend (ConnectorX or a traditional driver) returns; one column,
`_extracted_at`, is added. A silver job types and merges it later.

This is the operator-facing spec. It documents only what the code accepts: a config
or event key not listed here is rejected. Why it is built this way, and the evidence
behind each decision, is in [DESIGN-db.md](DESIGN-db.md); read it before changing
behaviour. The API counterpart is [README.md](README.md) (`api_bronze`).

Supported engines: SQL Server, PostgreSQL, MySQL, Oracle, Db2 for LUW, and SQLite
(tests). Any other SQLAlchemy dialect works on the traditional backend only.

## Quick start

```sh
mise install
mise run setup                      # library, db drivers, pytest, ruff
mise run test:unit                  # SQLite end to end, no Docker
mise run test:integration           # docker-compose engines (see Testing)
mise run docker:smoke --db       # build the db image, invoke it through the emulator
```

Run a source locally:

```python
from db_bronze.config import RunRequest, SourceConfig
from db_bronze.pipeline import run_pipeline

cfg = SourceConfig.from_json(open("source.json").read())
print(run_pipeline(cfg, RunRequest("incremental")))
```

## The bronze contract

### Paths

```
<bucket_url>/<dataset_name>/<name>/mode=<mode>/year=YYYY/month=MM/day=DD/<load_id>.<file_id>.parquet
<bucket_url>/<dataset_name>/_dlt_loads/...                       load status (completeness)
<bucket_url>/<dataset_name>/_manifests/<name>/<snapshot_id>.json  full-snapshot manifests
```

`<name>` is the table's `name`. The date is the load date, except for a timestamp
`backfill`, where it is the window start, and for a page of a planned snapshot, where
it is the plan's `partition_date`. Bronze is **append-only**, zstd by default
(`parquet_compression`).

### Row schema

The source columns, with their source names (case, spaces and dashes kept) and the
backend's types, plus `_extracted_at` (`timestamp[us, UTC]`, the run start). A source
column named `_extracted_at` is rejected. The Parquet type each source type lands as,
per backend and engine, is DESIGN-db.md section 10.1.

- **Naive timestamps stay naive.** A source timestamp without a time zone lands as a
  naive Parquet timestamp: no shift, no UTC label. Interpret it with the config's
  `connection.naive_timezone` (default `UTC`). `naive_timezone` converts only cursor
  bounds (`cursor.initial_value`, event window bounds, `plan` output) between UTC and
  the source's wall clock; data values are never touched. Set it to what the source
  database's naive columns mean, or incremental windows will be off by the offset.
- **Types never change within one `name`.** A new source column lands. A type change
  on an existing column (including a decimal precision or scale change) fails the run
  and lands nothing. Fix the source, or land the table under a new `name`.
- **The backend is part of the type.** The two backends produce different Arrow types
  (integer widths, `binary` vs `large_binary`, every decimal). A backend change since
  the last successful run fails with `BackendChanged`, so **switching backend means a
  new table `name`** (a new bronze prefix; silver unions old and new).

### Completeness (what silver may read)

| Mode | A data file counts when |
|---|---|
| `incremental`, `backfill` | its `load_id` has `status = 0` in `_dlt_loads` |
| `full` | its load id is listed in a manifest `_manifests/<name>/<snapshot_id>.json` (`{"table", "snapshot_id", "load_ids", "written_at"}`). A single-invocation `full` run writes the manifest itself on success; a paged snapshot gets it from `finalize`. A `full` load without a manifest is an incomplete snapshot and must be ignored. |

### Duplicates from `lag`

With `cursor.lag`, dlt deduplicates nothing, so **every incremental run re-lands every
row in its lag window** (`[last cursor - lag, ...)`). Identical row versions repeat
across loads. Silver removes them: deduplicate on (`primary_key`, cursor column) and
keep the latest `_extracted_at`. Without `lag`, dlt deduplicates at the cursor
boundary by `primary_key`.

### NULL cursors

A row whose cursor is NULL never matches the cursor filter. The run counts them
(`NullCursorRows`, and the `null_cursor_count` of `validate`), so the gap is visible.
`cursor.include_null_cursor: true` adds `OR cursor IS NULL`, which re-reads those rows
on every run.

## Config reference

One config document = one database connection = one dlt pipeline = one Lambda
function. Unknown keys are rejected, and every static rule fails with `ConfigError` and
a message that says how to fix it. Configs live at `${CONFIG_URI}/${source_id}.json`
(any fsspec URL) or inline in the event as `config`.

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

### Top level

| Key | Meaning |
|---|---|
| `pipeline_name` | Required. `^[a-z][a-z0-9_]*$`. **Stable**: part of the state identity. |
| `dataset_name` | Required. Same pattern. **Stable**: part of the state identity. |
| `bucket_url` | Required. Bronze root, e.g. `s3://bucket/bronze`. **Stable**: part of the state identity. |
| `connection` | Required. See below. |
| `tables` | Required, at least one. See below. |
| `defaults` | `backend` and `batch_size` for every table. |
| `parquet_compression` | `zstd` (default), `snappy`, `gzip` or `none`. |
| `max_time_s` | `null` (default) or seconds > 0: caps an incremental run, for split loading. Rejected while any table's stream cannot stop early (see Chunking). |

Changing `pipeline_name`, `dataset_name` or `bucket_url` silently restarts every
cursor from its `initial_value`.

### `connection`

| Key | Meaning |
|---|---|
| `dialect` | Required. `mssql`, `postgres`, `mysql`, `oracle`, `db2`, `sqlite`, or another SQLAlchemy dialect name (traditional backend only). |
| `host`, `port`, `database` | Required with `host` + `database` for `mssql`, `postgres`, `mysql`, `db2`. |
| `service_name` | Oracle only, instead of `database` (host + `service_name` required). Oracle reads the connecting user's schema unless the table sets `schema`. |
| `path` | SQLite only: the database file. SQLite takes no other address key. |
| `username`, `password` | A literal, or a secret reference. Any connection field may be a reference: `{"env": "NAME"}`, `{"secret_id": "id"}` (whole SecretString) or `{"secret_id": "id", "key": "k"}` (one key of a JSON SecretString). Secrets Manager values are cached per container. |
| `auth` | `{"type": "password"}` (default). `{"type": "kerberos", "principal", "keytab", "krb5_conf"}` parses but fails at run start with `NotImplementedError`; see [docs/KERBEROS.md](docs/KERBEROS.md). `principal`, `keytab` (a secret reference) and `krb5_conf` are only valid with `kerberos`. |
| `driver_options` | Object of strings, numbers or secret references, passed to the traditional driver (e.g. ODBC keywords for SQL Server). Db2 passes on only `CurrentSchema`, `Security` and `SSL...` keywords and drops others silently; set a short schema per table with `schema` instead of `CurrentSchema`. |
| `isolation_level` | `READ UNCOMMITTED`, `READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE`, `SNAPSHOT` or `AUTOCOMMIT`. Applied to the traditional backend. ConnectorX applies it only on Postgres (the four standard levels) and SQL Server (five levels, not on a `partition` read); elsewhere the combination fails validation. Db2 takes the four standard levels only. |
| `naive_timezone` | IANA zone name, default `UTC`. See the bronze contract. |

### `defaults`

| Key | Meaning |
|---|---|
| `backend` | `connectorx`, `pyarrow` or `sqlalchemy`; `null` resolves per table (below). |
| `batch_size` | Rows per Arrow batch, default 50000. A table's `batch_size` overrides it. |

### Per table (`tables[]`)

| Key | Meaning |
|---|---|
| `name` | Required. Bronze table name and path prefix, `^[a-z][a-z0-9_]*$`, unique. |
| `schema`, `table` | The source table, exactly as in the database. `schema` null is the connection's default schema. |
| `query` | Instead of `schema`/`table`: a view or join. Cannot be combined with `schema`, `table`, `columns` or `where` (put the projection and filter inside it). Needs a `cursor` to run incrementally. Its column types are not reflected, so the preflight cannot check them. |
| `primary_key` | List of source column names. Required for `lag`, keyset chunking and offset paging; also used for deduplication at the cursor boundary. |
| `columns` | Projection (null: every column). This is how a column ConnectorX cannot read is excluded. |
| `where` | Static SQL predicate, ANDed with the cursor filter. |
| `cursor` | Omit for a **snapshot table**, which runs in `full` mode only. See below. |
| `backend` | `connectorx`, `pyarrow` or `sqlalchemy`. Overrides `defaults.backend`. |
| `chunking` | `{"strategy": "stream"}` (default) or keyset. See below. |
| `batch_size` | Rows per Arrow batch (ConnectorX `batch_size`, or the fetch size on the traditional backend). |
| `partition` | `{"column": "...", "num": N}`: ConnectorX parallel read. See Partitioned reads. |

`cursor`:

| Key | Meaning |
|---|---|
| `column` | Required. The source column, exactly as in the database. |
| `type` | Required. `timestamp` or `integer`. |
| `initial_value` | Required. ISO-8601 for `timestamp` (an offset is converted through `naive_timezone`), an integer for `integer`. Applies to the first incremental run, and is the lower bound of `full`. |
| `lag` | Seconds (`timestamp`) or an integer delta (`integer`); re-reads a trailing window to catch late commits. Needs `primary_key`. Re-lands the whole window every run (see Duplicates). |
| `include_null_cursor` | Default `false`; see NULL cursors. |

`chunking`:

| Key | Meaning |
|---|---|
| `strategy` | `stream` (default) or `keyset`. |
| `order_by` | Keyset only. Must start with the cursor column, then the primary key columns, and those columns must be in `columns` if set. |
| `page_size` | Keyset only. Positive number of rows per page. |

Keyset also requires `primary_key`.

`partition`: `column` (required; an integer column) and `num` (required; positive
number of parallel connections).

`page` (an event key, see Events): `offset` (required; rows skipped, integer >= 0) and
`limit` (rows in the page, positive; `null` for the open-ended last page).

**Default backend** is resolved from the config alone, so a config always gives the
same backend: `connectorx` for `mssql`, `postgres`, `mysql`, `oracle`, `sqlite`;
`pyarrow` for `db2` and any other dialect. There is no runtime fallback. (`sqlalchemy`
is accepted by validation but not implemented: a run fails with `NotImplementedError`;
use `pyarrow`.)

### Static validation rules

All fail with `ConfigError` before anything runs.

- `backend: connectorx` on an engine ConnectorX does not read (Db2, other dialects): set `"backend": "pyarrow"`.
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

## Events and modes

The Lambda event selects the config and the mode. Unknown event keys fail with
`ConfigError`, and a key that belongs to another kind of event is rejected too, so a
mistyped event fails instead of being ignored.

| Key | Used by |
|---|---|
| `source_id` | Every event (unless `config` is given): config file `${CONFIG_URI}/<source_id>.json`; a plain name, no `/`, not starting with `.`. |
| `config` | Every event: an inline config instead of `source_id`. |
| `mode` | `incremental` (default), `full`, `backfill`, `validate`, `plan`, `finalize`. |
| `tables` | Every event: a non-empty list of table names to run; unknown names fail. |
| `window_start`, `window_end` | `backfill` (both required, end after start; both integers or both ISO-8601, both with an offset or neither). |
| `plan` | `plan`: `snapshot` or `backfill`. |
| `page_size` | `plan: snapshot`: positive rows per page. |
| `windows`, `window_size` | `plan: backfill`: exactly one. `windows` is how many; `window_size` is how wide (seconds, or an integer delta). |
| `page`, `snapshot_id`, `partition_date` | One page of a planned snapshot (`mode: full`): all three together, with exactly one table in `tables`. `page` is `{"offset": N, "limit": N or null}`; `partition_date` is `YYYY-MM-DD`. |
| `table`, `load_ids` | `finalize`: the table, and every page's load ids. `snapshot_id` too; `tables` is not allowed. |

| Event | Behaviour | Path / state |
|---|---|---|
| `{"source_id": "x"}` | **incremental**: `[last cursor - lag, ...)`. First run starts at `initial_value`. Snapshot tables must be left out with `tables`. | `mode=incremental/<load date>`; cursor advances |
| `{"mode": "full"}` | **full**, one invocation: `[initial_value, run start)` for cursor tables, everything for snapshot tables. Writes the manifest on success. | `mode=full/<load date>`; cursor untouched |
| `{"mode": "backfill", "window_start": ..., "window_end": ...}` | **backfill**: `[start, end)`, ISO-8601 or integer to match the cursor. Stateless. May use `partition`. | `mode=backfill/<window start date>`; cursor untouched |
| `{"mode": "validate"}` | Static checks plus preflight; nothing extracted or written. Returns `ok`, `server_findings` and per-table `findings`, `backend`, `null_cursor_count`. Use it when onboarding a source. | none |
| `{"mode": "plan", "plan": "snapshot", "page_size": N}` | `COUNT(*)` per table, then `pages: [{table, offset, limit}]` (the last open-ended), a fresh `snapshot_id` and `partition_date`. Needs `primary_key` or keyset `order_by`. | none |
| `{"mode": "plan", "plan": "backfill", "windows": N}` | The cursor's `MIN`/`MAX` split into half-open, contiguous `windows: [{table, window_start, window_end}]`, timestamps in UTC. The last window ends just past MAX (`+1`, or `+1 minute`). Cursor tables only. | none |
| `{"mode": "full", "tables": ["t"], "page": {...}, "snapshot_id": ..., "partition_date": ...}` | One page, ordered by the primary key (or keyset `order_by`). Writes no manifest. | `mode=full/<partition_date>` |
| `{"mode": "finalize", "table": "t", "snapshot_id": ..., "load_ids": [...]}` | Writes `_manifests/<t>/<snapshot_id>.json`, which makes the pages count for silver. | manifest |

Run-start checks (`ConfigError`): a snapshot table cannot run in `incremental`,
`backfill` or `plan: backfill` (leave it out with `tables`); `partition` is rejected in
`incremental` mode, because partition batches interleave and rows do not arrive in
cursor order; paging needs a `primary_key` or keyset `order_by`.

The result of a run reports `rows` per table, `load_ids`, `backends`,
`null_cursor_rows`, `snapshot_id`, `is_empty` and `time_limited`. A paged snapshot is
orchestrated by a Step Function: [examples/db_snapshot.asl.json](examples/db_snapshot.asl.json)
runs `plan`, a `Map` over the pages, then `finalize` with the collected `load_ids`.

### Time budget and re-invocation

An incremental run gets a budget of Lambda's remaining time minus `SAFETY_MARGIN_S`
(default 120, never under 60 s); `max_time_s` caps it further. The result says
`time_limited`; the orchestrator re-invokes while `is_empty` is false.

| Table | What happens at the budget |
|---|---|
| keyset | stops **between pages**; no query is left running |
| traditional-backend `stream` (Postgres, SQL Server, Oracle, Db2) | dlt cuts the stream; closing the cursor ends the query |
| ConnectorX `stream`, or MySQL `stream` on the traditional backend | **never cut**: a dropped ConnectorX stream leaves its query running on the server with the connection open, and closing PyMySQL's cursor blocks until the query ends. The Lambda budget skips these tables (logged), and `max_time_s` is rejected with them. A table that does not fit one invocation must use keyset |

### Environment

| Variable | Meaning |
|---|---|
| `CONFIG_URI` | Where `source_id` configs live (fsspec URL). Required with `source_id`. |
| `SAFETY_MARGIN_S` | Seconds kept free before the Lambda timeout. Default 120. |
| `LOG_LEVEL` | Default `INFO`. |
| `ORACLE_CLIENT_LIB_DIR` | Instant Client directory, set by the image. When set, oracledb enters thick mode at import and a broken client fails the cold start. |
| `DATA_WRITER__COMPRESSION` | Set from `parquet_compression` on every run. |

## Chunking: stream, keyset, or offset paging

- **`stream`** (default): one query, ordered by the cursor, batches streamed. It holds
  a snapshot or locks for the query's whole duration, and cannot be split across
  invocations on ConnectorX or MySQL.
- **`keyset`**: repeated short queries, each an index range scan seeded by the last row
  of the previous page (`order_by` = cursor, then primary key). Use it for big
  tables, for time-budgeted split loading, and for Oracle tables larger than memory.
  It cannot check that the `order_by` columns are indexed: **ask the DBA to confirm an
  index on exactly those columns, in that order**, or every page is a scan.
- **Offset paging** (`plan: snapshot` + page events) is for big **snapshots** that need
  a Step Function. Caveats: OFFSET is **not consistent across invocations**, so rows
  that change while a snapshot runs can be duplicated or missed; and deep offsets get
  slower. Prefer keyset wherever a unique sortable key exists; use paging only when the
  table has no usable key, or its key is not indexed. Ask the DBA to confirm the
  `primary_key` (or `order_by`) is indexed, since every page sorts by it.

### Separate pipeline name for paged and parallel runs

Paged snapshots and parallel backfill windows run under their **own function and
`pipeline_name`** (e.g. `core_banking_backfill`, `core_banking_snapshot`). They still
write dlt state rows, so they must never share a pipeline name with a concurrently
running incremental. One concurrent run per pipeline name.

### Partitioned reads

`partition: {"column": ..., "num": N}` reads with N parallel ConnectorX connections.
Opt-in, never automatic. The column must be an integer column; the config must resolve
to `connectorx` and `chunking: stream`; it is rejected in `incremental` mode. On Oracle
the column must be case-insensitive (stored upper case, written lower case in the
config), because ConnectorX upper-cases it. On SQL Server it cannot be combined with
`isolation_level`.

## Preflight and `validate`

Every run (and `mode: validate`, which stops there) reflects each table about to run
and fails **before anything is extracted** with `PreflightError`, listing every
offending column. The two fixes are the same everywhere: set `"backend": "pyarrow"` on
the table, or exclude the column with `columns` (or cast it in `query`).

| Check | Applies to | Fix |
|---|---|---|
| Server version below the minimum (table below) | every table | upgrade the server; the backend is never switched silently |
| DECIMAL/NUMERIC with scale > 10 or precision > 28, or Postgres/Oracle NUMERIC without precision (ConnectorX loses digits silently) | ConnectorX | `pyarrow`, or declare a precision or cast. Oracle `NUMBER(p, 0)` is exempt |
| Per-dialect deny list: types ConnectorX cannot read, or reads wrongly (Postgres interval, money, timetz, macaddr, cidr, varbit, tsvector, `"char"`, most arrays; SQL Server xml, hierarchyid, geography, geometry, money, datetimeoffset; Oracle raw, rowid, interval, TIMESTAMP WITH (LOCAL) TIME ZONE, nclob, bfile, xmltype, boolean, json). A type SQLAlchemy does not recognise is a finding on every server dialect | ConnectorX | as above |
| `sql_variant` (SQL Server) | every backend | exclude or cast in `query` |
| A column named `_extracted_at` | every backend | rename or exclude it |
| `partition` column missing, not an integer, or case-sensitive on Oracle | partitioned tables | partition on an integer (case-insensitive on Oracle) column, or remove `partition` |

Minimum server versions, set from what the integration suite passed on:

| Dialect | Minimum | Evidence |
|---|---|---|
| mssql | 14 (SQL Server 2017) | 2017, 2019, 2022 |
| postgres | 11 | 11, 12, 17 |
| mysql | 8.0 | 8.0, 8.4 |
| oracle | 19 | Oracle Free 23.26 only. **19c and 21c are accepted but unverified**: the integration suite has not run on them (their images do not start on an arm64 host). Run it on an x86_64 host before relying on them |
| db2 | 11.5 | 11.5.5.1 and 12.1.5. 11.1 and 10.5 were not run |
| sqlite | none | |

## Errors

The handler unwraps these from dlt's `PipelineStepFailed`, so the Lambda `errorType` is
the class name. Nothing lands and cursor state holds for every one of them (state only
persists with a successful load).

| `errorType` | When | Fix |
|---|---|---|
| `ConfigError` | Static validation, or a bad event (unknown key, bad mode, a snapshot table in a cursor mode, partition in `incremental`, paging without a key, unknown table in `tables`). The message says how to fix it | Fix the config or event |
| `PreflightError` | Reflection checks failed (table above); also a bad `partition` column. Metric `PreflightFailed` | `"backend": "pyarrow"` on the table, exclude the column with `columns`, cast it in `query`, or upgrade the server |
| `ConnectorXFailed` | ConnectorX raised at any point, including mid-stream and for a `query` table's unreadable types. Metric `ConnectorXFailed` | `"backend": "pyarrow"` on that table. This changes bronze types, so the next run fails with `BackendChanged` unless the table lands under a new `name` |
| `BackendChanged` | The table's resolved backend differs from the last successful run's. Fails before extraction. Metric `BackendChanged` | Land the table under a new `name` (silver unions old and new), or set `backend` back to the previous one |
| `NotImplementedError` | `auth.type: kerberos`, or `backend: sqlalchemy` | Use `password` (see docs/KERBEROS.md) / use `pyarrow` |
| `PipelineStepFailed` | dlt wrapped an error that is not one of ours. A source column type change on an existing bronze column surfaces as this, with `DataValidationError` as its cause | Restore the column's type, or land the table under a new `name` |
| `ValueError` | The event has neither an inline `config` nor a plain `source_id` | Fix the event |
| `KeyError` | A secret `{"env": ...}` reference names an unset environment variable, or `CONFIG_URI` is unset | Set the variable |

EMF metrics (namespace `db_bronze`, dimensions `pipeline` and `table`): `RowsExtracted`
and `NullCursorRows` after each successful run, and one of `PreflightFailed`,
`ConnectorXFailed`, `BackendChanged` (value 1) when that error ends a run. Logs are
JSON: one `run start`, one `run done` carrying rows and the backend per table.

## Memory sizing

ConnectorX's stream channel is **unbounded**: if Parquet writing is slower than the
database read, batches pile up in memory, and `batch_size` and the Lambda memory are the
only controls (no backpressure is built). Oracle's ConnectorX source reads the whole
result before the first batch, so peak memory is the whole result: an Oracle table
larger than memory must use keyset (or `pyarrow`). Size each function from a measured
`validate`-plus-run on its largest table (peak RSS). Reference points: the fixed cost of
a run is about 590 MiB for the image container under emulation (an overestimate) and
about 254 MiB RSS in-process, so 1 GB leaves room; the per-table figure on the largest
real table has **not** been measured yet.

## Deploying and the image

One **x86_64** image holds every driver (`docker/Dockerfile.db`, about 1.2-1.5 GB):
`msodbcsql18` and unixODBC, Oracle Instant Client Basic (thick mode, always), ibm_db's
clidriver (x86_64 wheels only, hence no arm64 build), PyMySQL, psycopg. Build arguments
(`MSSQL_REPO_URL`, `ORACLE_IC_URL`, `ORACLE_IC_SHA256`) point at internal mirrors.
`CMD` is `db_bronze.handler.handler`. Least privilege is enforced per function through
IAM and a read-only database user, not by the image.

```sh
mise run docker:lock --db        # docker/requirements-db.lock from uv.lock
mise run docker:build --db       # lock + image (linux/amd64 only)
mise run docker:smoke --db       # drivers, Instant Client, SQLite on both backends
```

Per function: memory sized as above; a VPC route to the database; `CONFIG_URI`;
`SAFETY_MARGIN_S`; `bucket_url` access; Secrets Manager read for any `secret_id`
references; a timeout longer than the margin plus the slowest page.

## Testing

| Layer | Command |
|---|---|
| Unit, SQLite end to end, both backends | `mise run test:unit` |
| Integration in docker-compose: Postgres, MySQL and SQL Server by default (Oracle with `--image`/`--inside`, Db2 with `--inside` or on x86_64 Linux; `--engines` picks) | `mise run test:integration [--engines a,b] [--keep]` |
| The same suite through the Lambda emulator in the image | `mise run test:integration --image` |
| pytest inside the image (Oracle ConnectorX spikes, Db2 off x86_64) | `mise run test:integration --inside` |

Kerberos is untested (it is a placeholder).

## Open items

Not validated: real S3, real Lambda in a VPC against production databases, Glue 6.0
reading typed bronze, peak memory on the largest real table. Oracle 19c/21c and Db2
11.1/10.5 minimums (above). SQL Server and Oracle integration were not rerun after the
last shared harness edits. There is no database silver transform: only this contract.
