# Bronze ingestion on AWS Lambda

Two dlt-based Lambda functions that land source data in an S3 bronze layer as Parquet.
dlt does the plumbing (pagination or querying, incremental cursor state restored from
S3 on every run, the `mode=/year=/month=/day=` layout, completeness markers); the two
packages differ in what a row is.

| Package | Source | What lands |
|---|---|---|
| `api_bronze` | REST APIs | One row per API record. `payload` is the record's JSON text, cut **verbatim** from the response body: same bytes, never parsed and re-serialised. A Spark job (Glue 6.0) parses it into typed silver columns. |
| `db_bronze` | SQL Server, PostgreSQL, MySQL, Oracle, Db2 for LUW (SQLite in tests; any other SQLAlchemy dialect on the traditional backend) | **Typed Parquet**: source column names kept exactly, types as the backend (ConnectorX or a traditional driver) returns them, plus one `_extracted_at` column. |

Both share `bronze_common` (strict config building, secret references, handler helpers),
the bronze path layout, and the deployment model: one config document per source, one
function per source, one container image per package.

This README is the operator-facing spec. It documents only what the code accepts: a
config or event key not listed here is rejected. Why it is built this way, what else was
considered, and the evidence behind each decision are in [DESIGN-api.md](DESIGN-api.md)
and [DESIGN-db.md](DESIGN-db.md); read the relevant one before changing behaviour.
[dlt-lambda-ingestion-assessment.md](dlt-lambda-ingestion-assessment.md) is the earlier
feasibility study (background only).

**Contents:** [Status](#status) · [Quick start](#quick-start) ·
[Shared bronze layout](#the-shared-bronze-layout) ·
[API sources](#api-sources-api_bronze) · [Database sources](#database-sources-db_bronze) ·
[Deploying on Lambda](#deploying-on-lambda) · [Silver](#silver-spark-41--glue-60) ·
[Testing](#testing) · [Open items](#open-items)

## Status

| Part | State |
|---|---|
| `src/api_bronze` (library + Lambda handler) | Tested against an in-process fake API, local filesystem as the bucket |
| `src/db_bronze` (library + Lambda handler) | Tested end to end on SQLite on both backends; integration suite passed on Postgres, MySQL, SQL Server, Oracle and Db2 in docker-compose, in-process and through the image (exceptions in [Open items](#open-items)) |
| `docker/Dockerfile.api`, `docker/Dockerfile.db` | Built and smoke-tested through the Lambda Runtime Interface Emulator (`scripts/smoke_image.py`, `scripts/smoke_db_image.py`) |
| `silver/silver_transform.py`: read, completeness filter, parse, quarantine split, rescue, dedup | Tested locally with pyspark 4.1.1 on bronze written by `api_bronze` |
| `silver/silver_transform.py`: `merge_into_iceberg`, `main` | **Untested sketch**: no Iceberg catalog was available |
| `terraform/` | Planned against a mocked AWS provider only; never applied to a real account |
| Kerberos database auth | **Placeholder**: parses, fails at run start ([docs/KERBEROS.md](docs/KERBEROS.md)) |
| Real S3, real Lambda, Glue 6.0, Iceberg write, memory on real data | **Not yet validated**: see [Open items](#open-items) |

## Directory layout

```
src/api_bronze/
  split.py        verbatim record splitter (the core trick)
  envelope.py     record rows + broken-response policy + quarantine
  config.py       config / run-request dataclasses, strict validation
  secrets.py      secret references in config -> env vars / Secrets Manager
  source.py       dlt resources per endpoint and run mode
  pipeline.py     run_pipeline(config, request): the portable core
  handler.py      Lambda entry point (thin wrapper)
src/db_bronze/
  config.py       config / event dataclasses, strict validation
  dialects.py     the per-engine table (drivers, ConnectorX support, quirks)
  urls.py, query.py   connection URLs; SQLAlchemy Core statements (window, keyset, offset, count)
  extract.py      the two backends: ConnectorX and pyarrow
  preflight.py    reflected-type checks, server minimums (every run and `validate`)
  source.py       dlt resources per table, schema contract, backend-change guard
  plan.py, manifest.py   `plan` and `finalize` modes for Step Function snapshots
  errors.py, metrics.py  handler-visible errors and their EMF metrics
  pipeline.py, handler.py, secrets.py
src/bronze_common/  shared by both: strict config building, secrets, handler helpers (no dlt)
silver/silver_transform.py   API bronze -> silver reference (Spark 4.1 / Glue 6.0)
tests/            pytest suite: fake API in conftest.py, SQLite for db, integration/ (Docker)
docker/           one Lambda image per package + pinned, hashed requirements-*.lock
scripts/          image smoke tests (smoke_image.py, smoke_db_image.py), bench_split.py
examples/         sample source configs, invocation events, the db snapshot state machine
terraform/        deployment modules + examples (platform, api and db sources)
docs/KERBEROS.md  what Kerberos support would take
.mise/tasks/      mise tasks (bash): setup, lint, check, bench, test/*, docker/*, tf/*, dev/*
```

## Quick start

Tools (Python 3.12, [uv](https://docs.astral.sh/uv/), Java 21 for Spark) and tasks are
managed with [mise](https://mise.jdx.dev/). `mise tasks` lists them; each is a bash
script under `.mise/tasks/`.

```sh
mise install                    # python, uv, java
mise run setup                  # both packages, db drivers, pytest, ruff (--silver adds pyspark)
mise run check                  # lint + every unit test (Spark tests skip without pyspark)
mise run test:spark             # installs pyspark, runs the Spark tests
mise run test:integration       # db_bronze against docker-compose engines (see Testing)

mise run docker:smoke           # api image, invoked through the Lambda emulator (-p linux/amd64 for x86_64)
mise run docker:smoke --db      # db image (x86_64 only): drivers, Instant Client, SQLite on both backends
mise run tf                     # Terraform fmt/validate/test/lint (needs network)
```

Run a source locally:

```python
from api_bronze import RunRequest, SourceConfig, run_pipeline

cfg = SourceConfig.from_json(open("examples/vendor_orders.json").read())
print(run_pipeline(cfg, RunRequest("incremental")))
```

```python
from db_bronze.config import RunRequest, SourceConfig
from db_bronze.pipeline import run_pipeline

cfg = SourceConfig.from_json(open("examples/core_banking.json").read())
print(run_pipeline(cfg, RunRequest("incremental")))
```

## The shared bronze layout

This is what silver (and anyone else) can rely on, for both packages.

### Paths

```
<bucket_url>/<dataset_name>/<name>/mode=<mode>/year=YYYY/month=MM/day=DD/<load_id>.<file_id>.parquet
<bucket_url>/<dataset_name>/_dlt_loads/<pipeline_name>__<load_id>.jsonl          completed loads
<bucket_url>/<dataset_name>/_dlt_pipeline_state/...                              cursor state (dlt-owned)
<bucket_url>/<dataset_name>/_manifests/<name>/<snapshot_id>.json                 db: full-snapshot manifests
<quarantine_url>/_quarantine/<pipeline_name>/<endpoint>/year=/month=/day=/<ts>_page<N>.body + .meta.json   api: broken pages
```

- `<name>` is the configured resource (api) or table (db) `name`.
- The date is the **load date**, with two exceptions: a backfill (api, or db with a
  timestamp cursor) uses the **window start**, so a backfill window is self-describing
  and can be re-run on its own; a page of a planned db snapshot uses the plan's
  `partition_date`.
- A day with no data has no prefix. Treat a missing path as "nothing to do".
- Parquet is zstd-compressed by default (`parquet_compression`).

### Completeness

A data file belongs to a finished load only if its `<load_id>` (the part of the file
name before the second-to-last dot) has an entry in `_dlt_loads` with `status = 0`.
Silver must skip any other file: it belongs to an in-flight or failed load and will be
picked up later. A db `full` load additionally needs a manifest (see
[Completeness of full snapshots](#completeness-of-full-snapshots)).

### Append-only, at-least-once

Bronze is **append-only**. Every resource uses `write_disposition="append"`, and there
is deliberately no way to configure `replace`: a `replace` load deletes the whole table
folder, including other `mode=` partitions.

Delivery is **at-least-once**: updated records, overlapping backfill windows, inclusive
API `since` filters and db `lag` windows all re-land rows. Silver deduplicates (api: on
`record_id` + `updated_at_utc`; db: on `primary_key` + cursor, latest `_extracted_at`).

### State identity

`pipeline_name`, `dataset_name` and `bucket_url` together identify a pipeline's cursor
state. Changing any of them silently restarts every cursor from its `initial_value`.
One concurrent run per pipeline name: two concurrent runs both read state, and the last
writer wins (see [Orchestration](#orchestration)).

## API sources (`api_bronze`)

### Row schema (one row per API record)

| Column | Type | Meaning |
|---|---|---|
| `endpoint` | string | Request path as configured |
| `url` | string | Full URL fetched, query included |
| `status` | int32 | HTTP status |
| `content_type` | string | Response `Content-Type` |
| `fetched_at` | timestamp (UTC) | When the page was received |
| `page_seq` | int32 | 1-based page number within this run and endpoint |
| `record_index` | int32 | 0-based position in the page's records array |
| `record_id` | string | Value at `id_path` (null if not configured or bad) |
| `payload` | string | **The record's JSON text, verbatim** |
| `payload_raw` | binary | Whole response body, only when it is not valid UTF-8 (then `payload` is null) |
| `error` | string | Null for good rows; otherwise why the row is suspect |
| `updated_at_utc` | timestamp (UTC) | Parsed cursor; what dlt's incremental tracks. Null on error rows |

`(file, page_seq, record_index)` identifies a row exactly, which is how silver points
back to bronze. The same record lands again when it is updated, when backfill windows
overlap, and when the API's `since` filter is inclusive.

### Broken-response policy

| What the API returned | What happens |
|---|---|
| 2xx, but not JSON, empty, truncated, records key missing / `null` / not an array, or the records key duplicated | Raw body + metadata written to `_quarantine/`. The run **fails** with `BrokenPageError`, **nothing** from the run lands (not even earlier good pages), and the cursor does not move, so the next run retries. A page that stays broken fails every run: alarm on it. |
| A record that isn't an object, or whose id or cursor is missing / unparseable | Lands verbatim with `error` set and a null cursor. The run continues. Good records advance the cursor. |
| `NaN` / `Infinity` inside a record | Lands verbatim like any record (`error` null). Spark cannot parse it, so silver quarantines it. |
| Body is valid JSON but not valid UTF-8 | One row for the whole page: bytes in `payload_raw`, `error` set. The run continues. |
| `"data": []` | Normal: no rows. A quiet run writes no files and no load marker. |
| HTTP 4xx / 5xx | dlt retries per its runtime config, then the run fails. Nothing is quarantined. |

### API config reference

One JSON document per source = one dlt pipeline = one Lambda function. Unknown keys are
rejected, so typos fail loudly. Examples: `examples/vendor_orders.json`,
`examples/vendor_countries.json`.

```jsonc
{
  "pipeline_name": "vendor_orders",   // STABLE: part of the state identity
  "dataset_name": "vendor",           // STABLE: part of the state identity; folder under bucket_url
  "bucket_url": "s3://bronze/bronze", // STABLE: changing any of these three silently starts from scratch
  "quarantine_url": null,             // default: bucket_url
  "parquet_compression": "zstd",      // zstd | snappy | gzip | none
  "max_time_s": null,                 // optional cap per run for split loading (see Run modes)
  "client": {
    "base_url": "https://api.vendor.example/v2/",
    "paginator": {"type": "json_link", "next_url_path": "pagination.next"},
    "auth": {"type": "bearer", "token": {"secret_id": "ingest/vendor", "key": "api_token"}},
    "headers": {"Accept": "application/json"}
  },
  "resources": [{
    "name": "orders_raw",             // bronze table name: [a-z][a-z0-9_]*
    "path": "orders",                 // relative to base_url
    "params": {"limit": 500},         // static query params
    "records_path": "data",           // dotted path to the records array; "$" if the body is the array
    "id_path": "orderId",             // dotted path to the record id (optional)
    "paginator": null,                // optional per-endpoint override of client.paginator
    "incremental": {                  // optional; without it the endpoint is a snapshot (mode=full only)
      "cursor_path": "updatedAt",     // dotted path to the cursor in each record
      "cursor_type": "iso8601",       // iso8601 | epoch_s | epoch_ms  (ISO without offset = UTC)
      "initial_value": "2020-01-01T00:00:00Z",
      "start_param": "updated_since", // query param that gets the lower bound
      "end_param": "updated_before",  // query param for the upper bound, if the API has one
      "param_format": "iso8601",      // iso8601 (+00:00) | iso8601_z (Z) | epoch_s | epoch_ms
      "sorted_ascending": false       // true only if the API returns records in ascending cursor order
    }
  }]
}
```

- **Paginators and auth** use dlt's `rest_api` config vocabulary unchanged
  (`json_link`, `header_link`, `offset`, `page_number`, `cursor`, `header_cursor`,
  `single_page`; auth `bearer`, `api_key`, `http_basic`, `oauth2_client_credentials`).
  See the dlt 1.32 REST API source docs for each type's options. `auto` detection is
  rejected: it can guess wrong silently.
- **Secret references.** Any value under `client.auth` or `client.headers` may be
  `{"env": "VAR"}`, `{"secret_id": "id"}`, `{"secret_id": "id", "key": "json_key"}`,
  optionally with `"format": "Bearer {}"`. Secrets Manager values are cached per
  container; rotation applies from the next cold start.
- **Several resources in one config** share one client and one run. A broken page in
  any of them fails the whole run, so nothing lands for any of them. Give endpoints that
  should fail independently their own config.

### Run modes (the Lambda event)

| Event | Mode | Window | Cursor state | Path |
|---|---|---|---|---|
| `{"source_id": "x"}` | incremental | `[last cursor, ...)`; `initial_value` on the very first run | Read and advanced | `mode=incremental/<load date>` |
| `{"source_id": "x", "mode": "full"}` | full | `[initial_value, run start)` | Untouched | `mode=full/<load date>` |
| `{"source_id": "x", "mode": "backfill", "window_start": "...", "window_end": "..."}` | backfill | `[window_start, window_end)` | Untouched | `mode=backfill/<window start date>` |
| `{"config": {...}, ...}` | any | | | Inline config instead of `source_id`, for tests and ad hoc runs |

Sample events: `examples/events.json`.

- Endpoints without `incremental` can only run as `mode=full`.
- **Upper bounds.** When the API has an `end_param`, the window is sent to it. Without
  one, the API returns everything after the start and records at or past the end are
  dropped client-side. That is correct (filtering is per record) but fetches more.
- **Split loading.** If every resource is `sorted_ascending`, an incremental run stops
  extracting when the time budget runs out (the Lambda's remaining time minus
  `SAFETY_MARGIN_S`, or `max_time_s` if smaller). It stops between pages, and the cursor
  advances to what landed. The orchestrator re-invokes while `is_empty` is false. For
  unsorted APIs there is no limit: a run must finish inside the Lambda timeout, because
  stopping early would skip older records on later pages.
- **Cursor ties.** If the API's `since` is **exclusive**, a record with exactly the last
  landed timestamp that arrives later (or sits on the next page when a split-loading run
  is cut) is missed. Prefer inclusive `since`; the re-landed duplicate is removed by
  silver.

The handler returns:

```json
{"pipeline_name": "vendor_orders", "mode": "incremental", "window_start": null, "window_end": null,
 "load_ids": ["1795437864.2080688"], "rows": {"orders_raw": 4}, "is_empty": false,
 "time_limited": false, "started_at": "2026-11-23T12:44:24.174+00:00"}
```

Failures raise, so the invocation is marked failed and the Lambda `Errors` metric fires.
A broken page surfaces as `errorType: "BrokenPageError"` with the quarantine path in the
message.

### API operations

- **`BrokenPageError` alarm:** open the quarantined `.body` and `.meta.json`. Common
  causes are an HTML maintenance page, a 200 carrying an error object, or a truncated
  body. The function fails every run until the API recovers; nothing is lost, because
  the cursor has not moved. If the vendor changed the response shape, fix `records_path`
  in the config.
- **Rows with `error`** are expected occasionally. Silver routes them to its quarantine
  table. A sudden rise usually means the vendor changed a field name (`missing id`,
  `bad <cursor>`).
- **Adding a source** needs no code change, unless the API needs a paginator or auth
  that dlt does not have.

## Database sources (`db_bronze`)

### Row schema and types

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

### Completeness of full snapshots

| Mode | A data file counts when |
|---|---|
| `incremental`, `backfill` | its `load_id` has `status = 0` in `_dlt_loads` |
| `full` | its load id is listed in a manifest `_manifests/<name>/<snapshot_id>.json` (`{"table", "snapshot_id", "load_ids", "written_at"}`). A single-invocation `full` run writes the manifest itself on success; a paged snapshot gets it from `finalize`. A `full` load without a manifest is an incomplete snapshot and must be ignored. |

### Duplicates from `lag`

With `cursor.lag`, dlt deduplicates nothing, so **every incremental run re-lands every
row in its lag window** (`[last cursor - lag, ...)`). Identical row versions repeat
across loads. Silver removes them: deduplicate on (`primary_key`, cursor column) and
keep the latest `_extracted_at`. Without `lag`, dlt deduplicates at the cursor boundary
by `primary_key`.

### NULL cursors

A row whose cursor is NULL never matches the cursor filter. The preflight counts them
(within `where`), and every run reports that count as `null_cursor_rows` and the
`NullCursorRows` metric, as `validate` does as `null_cursor_count`, so the gap is
visible although those rows never land. `cursor.include_null_cursor: true` adds
`OR cursor IS NULL`, which re-reads those rows on every run; `NullCursorRows` then
counts the NULL-cursor rows the run landed.

### Database config reference

One config document = one database connection = one dlt pipeline = one Lambda function.
Unknown keys are rejected, and every static rule fails with `ConfigError` and a message
that says how to fix it. Configs live at `${CONFIG_URI}/${source_id}.json` (any fsspec
URL) or inline in the event as `config`. Example: `examples/core_banking.json`.

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

#### Top level

| Key | Meaning |
|---|---|
| `pipeline_name` | Required. `^[a-z][a-z0-9_]*$`. **Stable**: part of the state identity. |
| `dataset_name` | Required. Same pattern. **Stable**: part of the state identity. |
| `bucket_url` | Required. Bronze root, e.g. `s3://bucket/bronze`. **Stable**: part of the state identity. |
| `connection` | Required. See below. |
| `tables` | Required, at least one. See below. |
| `defaults` | `backend` and `batch_size` for every table. |
| `parquet_compression` | `zstd` (default), `snappy`, `gzip` or `none`. |
| `max_time_s` | `null` (default) or seconds > 0: caps an incremental run, for split loading. Rejected while any table's stream cannot stop early (see [Chunking](#chunking-stream-keyset-or-offset-paging)). |

#### `connection`

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
| `naive_timezone` | IANA zone name, default `UTC`. See [Row schema and types](#row-schema-and-types). |

#### `defaults`

| Key | Meaning |
|---|---|
| `backend` | `connectorx` or `pyarrow`; `null` resolves per table (below). `sqlalchemy` is not built yet and fails with `ConfigError`. |
| `batch_size` | Rows per Arrow batch, default 50000. A table's `batch_size` overrides it. |

#### Per table (`tables[]`)

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
| `partition` | `{"column": "...", "num": N}`: ConnectorX parallel read. See [Partitioned reads](#partitioned-reads). |

`cursor`:

| Key | Meaning |
|---|---|
| `column` | Required. The source column, exactly as in the database. |
| `type` | Required. `timestamp` or `integer`. |
| `initial_value` | Required. ISO-8601 for `timestamp` (an offset is converted through `naive_timezone`), an integer for `integer`. Applies to the first incremental run, and is the lower bound of `full`. |
| `lag` | Seconds (`timestamp`) or an integer delta (`integer`); re-reads a trailing window to catch late commits. Needs `primary_key`. Re-lands the whole window every run (see [Duplicates](#duplicates-from-lag)). |
| `include_null_cursor` | Default `false`; see [NULL cursors](#null-cursors). |

`chunking`:

| Key | Meaning |
|---|---|
| `strategy` | `stream` (default) or `keyset`. |
| `order_by` | Keyset only. Must start with the cursor column, then the primary key columns, and those columns must be in `columns` if set. |
| `page_size` | Keyset only. Positive number of rows per page. |

Keyset also requires `primary_key`.

`partition`: `column` (required; an integer column) and `num` (required; positive
number of parallel connections).

`page` (an event key, see [Events and modes](#events-and-modes)): `offset` (required;
rows skipped, integer >= 0) and `limit` (rows in the page, positive; `null` for the
open-ended last page).

**Default backend** is resolved from the config alone, so a config always gives the same
backend: `connectorx` for `mssql`, `postgres`, `mysql`, `oracle`, `sqlite`; `pyarrow`
for `db2` and any other dialect. There is no runtime fallback. (`sqlalchemy` is a
designed backend that is not built yet: the config rejects it with `ConfigError`; use
`pyarrow`.)

#### Static validation rules

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

### Events and modes

The Lambda event selects the config and the mode. Unknown event keys fail with
`ConfigError`, and a key that belongs to another kind of event is rejected too, so a
mistyped event fails instead of being ignored.

| Key | Used by |
|---|---|
| `source_id` | Every event (unless `config` is given): config file `${CONFIG_URI}/<source_id>.json`; a plain name, no `/`, not starting with `.`. |
| `config` | Every event: an inline config instead of `source_id`. |
| `mode` | `incremental` (default), `full`, `backfill`, `validate`, `plan`, `finalize`. |
| `tables` | Every event except `finalize`: a non-empty list of table names to run; unknown names fail. `finalize` names its one table in `table` instead, and rejects `tables`. |
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

#### Time budget and re-invocation

An incremental run gets a budget of Lambda's remaining time minus `SAFETY_MARGIN_S`
(default 120, never under 60 s); `max_time_s` caps it further. The result says
`time_limited`; the orchestrator re-invokes while `is_empty` is false.

| Table | What happens at the budget |
|---|---|
| keyset | stops **between pages**; no query is left running |
| traditional-backend `stream` (Postgres, SQL Server, Oracle, Db2) | dlt cuts the stream; closing the cursor ends the query |
| ConnectorX `stream`, or MySQL `stream` on the traditional backend | **never cut**: a dropped ConnectorX stream leaves its query running on the server with the connection open, and closing PyMySQL's cursor blocks until the query ends. The Lambda budget skips these tables (logged), and `max_time_s` is rejected with them. A table that does not fit one invocation must use keyset |

### Chunking: stream, keyset, or offset paging

- **`stream`** (default): one query, ordered by the cursor, batches streamed. It holds a
  snapshot or locks for the query's whole duration, and cannot be split across
  invocations on ConnectorX or MySQL.
- **`keyset`**: repeated short queries, each an index range scan seeded by the last row
  of the previous page (`order_by` = cursor, then primary key). Use it for big tables,
  for time-budgeted split loading, and for Oracle tables larger than memory. It cannot
  check that the `order_by` columns are indexed: **ask the DBA to confirm an index on
  exactly those columns, in that order**, or every page is a scan.
- **Offset paging** (`plan: snapshot` + page events) is for big **snapshots** that need a
  Step Function. Caveats: OFFSET is **not consistent across invocations**, so rows that
  change while a snapshot runs can be duplicated or missed; and deep offsets get slower.
  Prefer keyset wherever a unique sortable key exists; use paging only when the table
  has no usable key, or its key is not indexed. Ask the DBA to confirm the `primary_key`
  (or `order_by`) is indexed, since every page sorts by it.

Paged snapshots and parallel backfill windows run under their own function and
`pipeline_name` (see [Orchestration](#orchestration)).

#### Partitioned reads

`partition: {"column": ..., "num": N}` reads with N parallel ConnectorX connections.
Opt-in, never automatic. The column must be an integer column; the config must resolve
to `connectorx` and `chunking: stream`; it is rejected in `incremental` mode. On Oracle
the column must be case-insensitive (stored upper case, written lower case in the
config), because ConnectorX upper-cases it. On SQL Server it cannot be combined with
`isolation_level`.

### Preflight and `validate`

Every run (and `mode: validate`, which stops there) reflects each table about to run and
fails **before anything is extracted** with `PreflightError`, listing every offending
column. The two fixes are the same everywhere: set `"backend": "pyarrow"` on the table,
or exclude the column with `columns` (or cast it in `query`).

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

### Errors

The handler unwraps these from dlt's `PipelineStepFailed`, so the Lambda `errorType` is
the class name. Nothing lands and cursor state holds for every one of them (state only
persists with a successful load).

| `errorType` | When | Fix |
|---|---|---|
| `ConfigError` | Static validation (including `backend: sqlalchemy`, not built yet), or a bad event (unknown key, bad mode, a snapshot table in a cursor mode, partition in `incremental`, paging without a key, unknown table in `tables`). The message says how to fix it | Fix the config or event |
| `PreflightError` | Reflection checks failed (table above); also a bad `partition` column. Metric `PreflightFailed` | `"backend": "pyarrow"` on the table, exclude the column with `columns`, cast it in `query`, or upgrade the server |
| `ConnectorXFailed` | ConnectorX raised at any point, including mid-stream and for a `query` table's unreadable types. Metric `ConnectorXFailed` | `"backend": "pyarrow"` on that table. This changes bronze types, so the next run fails with `BackendChanged` unless the table lands under a new `name` |
| `BackendChanged` | The table's resolved backend differs from the last successful run's. Fails before extraction. Metric `BackendChanged` | Land the table under a new `name` (silver unions old and new), or set `backend` back to the previous one |
| `NotImplementedError` | `auth.type: kerberos` | Use `password` (see [docs/KERBEROS.md](docs/KERBEROS.md)) |
| `PipelineStepFailed` | dlt wrapped an error that is not one of ours. A source column type change on an existing bronze column surfaces as this, with `DataValidationError` as its cause | Restore the column's type, or land the table under a new `name` |
| `ValueError` | The event has neither an inline `config` nor a plain `source_id` | Fix the event |
| `KeyError` | A secret `{"env": ...}` reference names an unset environment variable, or `CONFIG_URI` is unset | Set the variable |

EMF metrics (namespace `db_bronze`, dimensions `pipeline` and `table`): `RowsExtracted`
and `NullCursorRows` after each successful run, and one of `PreflightFailed`,
`ConnectorXFailed`, `BackendChanged` (value 1) when that error ends a run. Logs are
JSON: one `run start`, one `run done` carrying rows and the backend per table.

### Memory sizing

ConnectorX's stream channel is **unbounded**: if Parquet writing is slower than the
database read, batches pile up in memory, and `batch_size` and the Lambda memory are the
only controls (no backpressure is built). Oracle's ConnectorX source reads the whole
result before the first batch, so peak memory is the whole result: an Oracle table
larger than memory must use keyset (or `pyarrow`). Size each function from a measured
`validate`-plus-run on its largest table (peak RSS). Reference points: the fixed cost of
a run is about 590 MiB for the image container under emulation (an overestimate) and
about 254 MiB RSS in-process, so 1 GB leaves room; the per-table figure on the largest
real table has **not** been measured yet.

### The database image

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

## Deploying on Lambda

Build one image per package (and per architecture, for api) and use it for every source
of that package. A source is a config document in S3 plus a function.

| Setting | api_bronze | db_bronze | Why |
|---|---|---|---|
| Package | `docker/Dockerfile.api` (arm64 or x86_64) | `docker/Dockerfile.db` (x86_64 only) | dlt with pyarrow is far over the 250 MB zip limit |
| Handler (image default) | `api_bronze.handler.handler` | `db_bronze.handler.handler` | |
| Timeout | 900 s | 900 s; at least the margin plus the slowest page | Split loading uses the remaining time; runs that cannot stop early must finish in it |
| Memory | Start at 1–2 GB; measure. A page is held several times over (bytes, text, parsed, Arrow): size to the largest page | ~1 GB covers the fixed cost; measure the largest table ([Memory sizing](#memory-sizing)) | |
| Ephemeral storage | 2–10 GB | 2–10 GB | dlt buffers files in `/tmp` before upload |
| Reserved concurrency | **1** per incremental function | **1** per incremental function | Two concurrent runs of one pipeline both read state, and the last writer wins |
| Network | Outbound to the API | VPC with a route to the database, and to S3 and Secrets Manager (endpoints or NAT) | |
| IAM | Read `CONFIG_URI`; list/read/write the bronze and quarantine prefixes; `secretsmanager:GetSecretValue` on the referenced secrets | Same, bronze prefix only (incl. `_manifests`) | Confirm the minimal S3 action set on the first real-S3 run |

Environment:

| Variable | Meaning |
|---|---|
| `CONFIG_URI` | Where `source_id` configs live (fsspec URL, e.g. `s3://config-bucket/sources`); config is read from `${CONFIG_URI}/${source_id}.json`. Required with `source_id`. |
| `SAFETY_MARGIN_S` | Seconds kept free before the Lambda timeout. Default 120; the budget is never under 60 s. |
| `LOG_LEVEL` | Default `INFO`. |
| `ORACLE_CLIENT_LIB_DIR` | db image only, set by the image: Instant Client directory. When set, oracledb enters thick mode at import and a broken client fails the cold start. |
| `DATA_WRITER__COMPRESSION` | Set from `parquet_compression` on every run; don't set it by hand. |

Both images set `DLT_DATA_DIR=/tmp/dlt`, `DLT_PROJECT_DIR=/tmp` (only `/tmp` is
writable) and turn dlt telemetry off.

### Orchestration

- **Steady state:** schedule `{"source_id": ...}`. With split loading, a Step Function
  loops while `is_empty` is false.
- **Separate pipeline name for paged and parallel runs.** Backfill windows run through
  the same function execute one at a time. To run windows in parallel, or to run a db
  paged snapshot, deploy a second function with a copy of the config whose
  `pipeline_name` differs (e.g. `vendor_orders_backfill`, `core_banking_snapshot`) and
  no reserved concurrency. Backfills never write the cursor, but they still write dlt
  state rows, so they must never share a pipeline name with a concurrently running
  incremental.
- **db paged snapshots** run `plan`, a `Map` over the pages, then `finalize`:
  [examples/db_snapshot.asl.json](examples/db_snapshot.asl.json).
- **Every invocation is a cold start** by design: the local working directory is dropped
  and state is restored from S3 each time. A warm container behaves exactly like a cold
  one.

### Alarms

- Lambda `Errors` per function.
- api: a log metric filter on `BrokenPageError` separates "vendor sent garbage" from
  other failures. Watch for new objects under `_quarantine/`. In silver, alert on rows
  routed to quarantine.
- db: the EMF metrics `PreflightFailed`, `ConnectorXFailed`, `BackendChanged` per table
  ([Errors](#errors)); watch `NullCursorRows`.

### Terraform

`terraform/modules/api_bronze_source` and `terraform/modules/db_bronze_source` encode
this section (function, IAM derived from the config, schedules, the split-loading loop,
alarms; for db also the VPC security group and the paged-snapshot state machine), and
`terraform/examples/` deploys the sample sources. See
[terraform/README.md](terraform/README.md).

### Operations

- **Re-run a period:** run a `backfill` window. It is append-only, and silver dedups.
- **Reset a source's cursor:** use a new `pipeline_name`. Its first incremental run
  starts from `initial_value`. Do not delete `_dlt_loads/` objects: silver uses them to
  decide which data files are complete. Never delete bronze data prefixes to do this
  either: dlt also wipes local state when its dataset disappears.
- **Adding a source:** a config JSON in `CONFIG_URI`, a function (image +
  event/schedule), and IAM. For a database, run `{"mode": "validate"}` first.
- **Upgrading dlt** (pinned at 1.32.0 on purpose): the api envelope relies on
  RESTClient internals (the response hook replacing the default `raise_for_status`,
  and every paginator parsing through `response.json()`), and both packages rely on
  dlt's incremental and Arrow behaviour. Re-run the whole test suite, the integration
  suite and both image smoke tests before upgrading.

## Silver (Spark 4.1 / Glue 6.0)

`silver/silver_transform.py` is a reference for **API** bronze. Each source defines its
fields:

```python
from silver_transform import Field, latest_per_id, read_bronze, transform

FIELDS = [
    Field("status", "$.status", "string"),
    Field("amount", "$.detail.amount.value", "decimal(18,2)"),
    Field("updated_at", "$.updatedAt", "timestamp"),
]
bronze = read_bronze(spark, "s3://bronze/bronze/vendor", "orders_raw", "incremental", dates)
if bronze is not None:
    silver, quarantine = transform(bronze, FIELDS, known_keys=["orderId"])
    current = latest_per_id(silver)  # then MERGE into Iceberg v2 (sketch: merge_into_iceberg)
```

- `transform` parses `payload` with `try_parse_json` and extracts each field with
  `try_variant_get`. VARIANT exists only in memory; Iceberg gets typed columns (v2).
- Rows with a bronze `error`, or a payload Spark cannot parse (e.g. `NaN`), go to
  `quarantine` with a `reason`.
- `_rescued_data` is a JSON object of fields that are present but fail their cast.
  `_unknown_keys` lists top-level keys no field covers. Every row carries `_bronze`:
  file, load id, url, page, index and fetch time.
- Set `spark.sql.session.timeZone=UTC`.

There is no database silver transform: database bronze is already typed, and the
contract above (completeness, manifests, `lag` deduplication, backend changes) is what a
silver job must follow.

## Testing

Tests need no network or AWS: the API tests run an in-process fake REST API and use a
local directory as the bucket; the db unit tests run end to end on SQLite on both
backends.

| Layer | Command |
|---|---|
| Lint + every unit test | `mise run check` |
| Unit tests without Spark/Java | `mise run test:unit` |
| Spark (silver) tests | `mise run test:spark` |
| One test | `mise run test tests/test_modes.py::test_name` |
| db integration in docker-compose: Postgres, MySQL and SQL Server by default (Oracle with `--image`/`--inside`, Db2 with `--inside` or on x86_64 Linux; `--engines` picks) | `mise run test:integration [--engines a,b] [--keep]` |
| The same suite through the Lambda emulator in the db image | `mise run test:integration --image` |
| pytest inside the db image (Oracle ConnectorX spikes, Db2 off x86_64) | `mise run test:integration --inside` |
| Image smoke tests | `mise run docker:smoke`, `mise run docker:smoke --db` |
| Split benchmark | `mise run bench` |
| Terraform (needs the registry, no AWS credentials) | `mise run tf` |

SQL Server needs Rosetta on arm64; Db2 runs on x86_64 only (DESIGN-db.md section 10).
`tests/test_db_readme.py` fails when a db config key, event key, mode, `errorType` or
metric is missing from the [Database sources](#database-sources-db_bronze) section, so
update it in the same change.

## Open items

1. Run against **real S3** and a **real Lambda** (db: in a VPC against production
   databases): IAM minimal set, s3fs credentials from the function role, timing,
   cold-start import time. The Terraform has not been applied to a real account.
2. **Lambda memory**: api with the largest realistic page, db with peak RSS on the
   largest real table; set the function memory from the measurement.
3. **Glue 6.0** run of the silver transform (tested locally on pyspark 4.1.1 only), the
   **Iceberg v2 MERGE** (untested sketch), and Glue reading typed db bronze.
4. **Audit requirement (api):** bronze is verbatim per *record*, not per *response*.
   Top-level response fields (`pagination`, totals) and the whitespace between records
   are not kept. If exact response bytes are ever needed for a dispute, add a separate
   raw zone for those vendors only (see DESIGN-api.md §8).
5. Silver: decide the Iceberg table layout, MERGE cadence, and the quarantine table's
   retention. There is no database silver transform yet.
6. S3 lifecycle rules for `_dlt_loads`/`_dlt_pipeline_state`: one small object each per
   run.
7. Database versions accepted but not run: **Oracle 19c/21c** (images do not start on
   arm64; run the suite on x86_64) and **Db2 11.1/10.5**.
8. **Kerberos** database auth is a placeholder ([docs/KERBEROS.md](docs/KERBEROS.md)).
