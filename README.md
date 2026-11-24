# api-bronze

Land REST API data in an S3 bronze layer **raw**, with dlt doing the plumbing, on
AWS Lambda. Each API record becomes one Parquet row whose `payload` is the
record's JSON text, cut **verbatim** from the response body: same bytes, never
parsed and re-serialised. dlt still provides pagination, incremental cursor
state (restored from S3 on every run), the `mode=/year=/month=/day=` layout and
completeness markers. A Spark job (Glue 6.0) parses the payload into typed
silver columns later.

Why it is built this way, what else was considered, and the evidence behind each
decision are in [DESIGN.md](DESIGN.md). Read it before changing the design.

## Status

| Part | State |
|---|---|
| `src/api_bronze` (ingestion library + Lambda handler) | Tested: 65 tests against an in-process fake API, local filesystem as the bucket |
| `docker/Dockerfile` | Built (linux/arm64) and smoke-tested through the Lambda Runtime Interface Emulator (`scripts/smoke_image.py`) |
| `silver/silver_transform.py`: read, completeness filter, parse, quarantine split, rescue, dedup | Tested locally with pyspark 4.1.1 on bronze written by this library |
| `silver/silver_transform.py`: `merge_into_iceberg`, `main` | **Untested sketch**: no Iceberg catalog was available |
| Real S3, real Lambda, Glue 6.0, Iceberg write, memory with large pages | **Not yet validated**: see [Open items](#open-items) |

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
silver/silver_transform.py   bronze -> silver reference (Spark 4.1 / Glue 6.0)
tests/            pytest suite, fake API in conftest.py
docker/           Lambda image + pinned, hashed requirements.lock
scripts/          smoke_image.py (image via emulator), bench_split.py
examples/         example source configs and invocation events
.mise/tasks/      mise tasks (bash): setup, lint, check, bench, test/*, docker/*
```

## Quick start

Tools (Python 3.12, [uv](https://docs.astral.sh/uv/), Java 21 for Spark) and
tasks are managed with [mise](https://mise.jdx.dev/). `mise tasks` lists them;
each one is a bash script under `.mise/tasks/`.

```sh
mise install                # python, uv, java
mise run setup              # library + pytest + ruff (--silver adds pyspark)
mise run test               # 63 tests; the Spark test module skips without pyspark
mise run test:spark         # + Spark tests
mise run lint

# Lambda image (lock + build), then invoke it locally through the emulator
mise run docker:smoke       # docker:build -p linux/amd64 for x86_64 functions
```

Run a source locally against any reachable API:

```python
from api_bronze import RunRequest, SourceConfig, run_pipeline

cfg = SourceConfig.from_json(open("examples/vendor_orders.json").read())
print(run_pipeline(cfg, RunRequest("incremental")))
```

## The bronze contract

This is what silver (and anyone else) can rely on.

### Paths

```
<bucket_url>/<dataset_name>/<resource name>/mode=<mode>/year=YYYY/month=MM/day=DD/<load_id>.<file_id>.parquet
<bucket_url>/<dataset_name>/_dlt_loads/<pipeline_name>__<load_id>.jsonl          completed loads
<bucket_url>/<dataset_name>/_dlt_pipeline_state/...                              cursor state (dlt-owned)
<quarantine_url>/_quarantine/<pipeline_name>/<endpoint>/year=/month=/day=/<ts>_page<N>.body + .meta.json
```

- The date is the **load date**. Backfills use the **window start** instead, so
  a backfill window is self-describing and can be re-run on its own.
- **Completeness:** a data file belongs to a finished load only if its
  `<load_id>` (the part of the file name before the second-to-last dot) has an
  entry in `_dlt_loads` with `status = 0`. Silver must skip any other file: it
  belongs to an in-flight or failed load and will be picked up later.
- A day with no data has no prefix. Treat a missing path as "nothing to do".
- Bronze is **append-only**. Every resource uses `write_disposition="append"`,
  and there is deliberately no way to configure `replace`: a `replace` load
  deletes the whole table folder, including other `mode=` partitions.

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

Parquet is zstd-compressed by default. `(file, page_seq, record_index)`
identifies a row exactly, which is how silver points back to bronze.

**Delivery is at-least-once.** The same record lands again when it is updated,
when backfill windows overlap, and when the API's `since` filter is inclusive.
Silver deduplicates on `record_id` + `updated_at_utc`.

### Broken-response policy

| What the API returned | What happens |
|---|---|
| 2xx, but not JSON, empty, truncated, records key missing / `null` / not an array, or the records key duplicated | Raw body + metadata written to `_quarantine/`. The run **fails** with `BrokenPageError`, **nothing** from the run lands (not even earlier good pages), and the cursor does not move, so the next run retries. A page that stays broken fails every run: alarm on it. |
| A record that isn't an object, or whose id or cursor is missing / unparseable | Lands verbatim with `error` set and a null cursor. The run continues. Good records advance the cursor. |
| `NaN` / `Infinity` inside a record | Lands verbatim like any record (`error` null). Spark cannot parse it, so silver quarantines it. |
| Body is valid JSON but not valid UTF-8 | One row for the whole page: bytes in `payload_raw`, `error` set. The run continues. |
| `"data": []` | Normal: no rows. A quiet run writes no files and no load marker. |
| HTTP 4xx / 5xx | dlt retries per its runtime config, then the run fails. Nothing is quarantined. |

## Config reference

One JSON document per source = one dlt pipeline = one Lambda function.
Unknown keys are rejected, so typos fail loudly. Examples: `examples/`.

```jsonc
{
  "pipeline_name": "vendor_orders",   // STABLE: part of the state identity
  "dataset_name": "vendor",           // STABLE: part of the state identity; folder under bucket_url
  "bucket_url": "s3://bronze/bronze", // STABLE: changing any of these three silently starts from scratch
  "quarantine_url": null,             // default: bucket_url
  "parquet_compression": "zstd",      // zstd | snappy | gzip | none
  "max_time_s": null,                 // optional cap per run for split loading (see Modes)
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
  (`json_link`, `header_link`, `offset`, `page_number`, `cursor`,
  `header_cursor`, `single_page`; auth `bearer`, `api_key`, `http_basic`,
  `oauth2_client_credentials`). See the dlt 1.32 REST API source docs for each
  type's options. `auto` detection is rejected: it can guess wrong silently.
- **Secret references.** Any value under `client.auth` or `client.headers` may
  be `{"env": "VAR"}`, `{"secret_id": "id"}`, `{"secret_id": "id", "key":
  "json_key"}`, optionally with `"format": "Bearer {}"`. Secrets Manager values
  are cached per container; rotation applies from the next cold start.
- **Several resources in one config** share one client and one run. A broken
  page in any of them fails the whole run, so nothing lands for any of them.
  Give endpoints that should fail independently their own config.

## Run modes (the Lambda event)

| Event | Mode | Window | Cursor state | Path |
|---|---|---|---|---|
| `{"source_id": "x"}` | incremental | `[last cursor, ...)`; `initial_value` on the very first run | Read and advanced | `mode=incremental/<load date>` |
| `{"source_id": "x", "mode": "full"}` | full | `[initial_value, run start)` | Untouched | `mode=full/<load date>` |
| `{"source_id": "x", "mode": "backfill", "window_start": "...", "window_end": "..."}` | backfill | `[window_start, window_end)` | Untouched | `mode=backfill/<window start date>` |
| `{"config": {...}, ...}` | any | | | Inline config instead of `source_id`, for tests and ad hoc runs |

- Endpoints without `incremental` can only run as `mode=full`.
- **Upper bounds.** When the API has an `end_param`, the window is sent to it.
  Without one, the API returns everything after the start and records at or
  past the end are dropped client-side. That is correct (filtering is per
  record) but fetches more.
- **Split loading.** If every resource is `sorted_ascending`, an incremental run
  stops extracting when the time budget runs out (the Lambda's remaining time
  minus `SAFETY_MARGIN_S`, or `max_time_s` if smaller). It stops between pages,
  and the cursor advances to what landed. The orchestrator re-invokes while
  `is_empty` is false. For unsorted APIs there is no limit: a run must finish
  inside the Lambda timeout, because stopping early would skip older records on
  later pages.
- **Cursor ties.** If the API's `since` is **exclusive**, a record with exactly
  the last landed timestamp that arrives later (or sits on the next page when a
  split-loading run is cut) is missed. Prefer inclusive `since`; the re-landed
  duplicate is removed by silver.

The handler returns:

```json
{"pipeline_name": "vendor_orders", "mode": "incremental", "window_start": null, "window_end": null,
 "load_ids": ["1795437864.2080688"], "rows": {"orders_raw": 4}, "is_empty": false,
 "time_limited": false, "started_at": "2026-11-23T12:44:24.174+00:00"}
```

Failures raise, so the invocation is marked failed and the Lambda `Errors`
metric fires. A broken page surfaces as `errorType: "BrokenPageError"` with the
quarantine path in the message.

## Deploying on Lambda

Build one image per architecture and use it for every API source. A source is a
config document in S3 plus a function.

| Setting | Value | Why |
|---|---|---|
| Package | Container image (`docker/Dockerfile`) | dlt with pyarrow is far over the 250 MB zip limit |
| Handler | `api_bronze.handler.handler` (image default) | |
| Timeout | 900 s | Split loading uses the remaining time; unsorted APIs must finish in it |
| Memory | Start at 1–2 GB; measure | A page is held several times over (bytes, text, parsed, Arrow). Size to the largest page |
| Ephemeral storage | 2–10 GB | dlt buffers files in `/tmp` before upload |
| Reserved concurrency | **1** per incremental function | Two concurrent runs of one pipeline both read state, and the last writer wins |
| Env `CONFIG_URI` | e.g. `s3://config-bucket/sources` | Config is read from `${CONFIG_URI}/${source_id}.json` |
| Env `SAFETY_MARGIN_S` | default 120 | Time kept free before the timeout |
| Env `LOG_LEVEL` | default INFO | |
| IAM | Read `CONFIG_URI`; list/read/write the bronze and quarantine prefixes; `secretsmanager:GetSecretValue` on the referenced secrets | Confirm the minimal S3 action set on the first real-S3 run |

The image sets `DLT_DATA_DIR=/tmp/dlt`, `DLT_PROJECT_DIR=/tmp` (only `/tmp` is
writable) and turns dlt telemetry off.

**Orchestration**

- **Steady state:** schedule `{"source_id": ...}`. With split loading, a Step
  Function loops while `is_empty` is false.
- **Backfills:** run windows through the same function and they execute one at
  a time. To run windows in parallel, deploy a second function with a copy of
  the config whose `pipeline_name` differs (e.g. `vendor_orders_backfill`) and
  no reserved concurrency. Backfills never write the cursor, but they still
  write dlt state rows, so they must not share a pipeline name with a
  concurrently running incremental.
- **Every invocation is a cold start** by design: the local working directory
  is dropped and state is restored from S3 each time. A warm container behaves
  exactly like a cold one.

**Alarms:** Lambda `Errors` per function. A log metric filter on
`BrokenPageError` separates "vendor sent garbage" from other failures. Watch for
new objects under `_quarantine/`. In silver, alert on rows routed to quarantine.

## Silver (Spark 4.1 / Glue 6.0)

`silver/silver_transform.py` is a reference. Each source defines its fields:

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

- `transform` parses `payload` with `try_parse_json` and extracts each field
  with `try_variant_get`. VARIANT exists only in memory; Iceberg gets typed
  columns (v2).
- Rows with a bronze `error`, or a payload Spark cannot parse (e.g. `NaN`), go
  to `quarantine` with a `reason`.
- `_rescued_data` is a JSON object of fields that are present but fail their
  cast. `_unknown_keys` lists top-level keys no field covers. Every row carries
  `_bronze`: file, load id, url, page, index and fetch time.
- Set `spark.sql.session.timeZone=UTC`.

## Operations

- **`BrokenPageError` alarm:** open the quarantined `.body` and `.meta.json`.
  Common causes are an HTML maintenance page, a 200 carrying an error object,
  or a truncated body. The function fails every run until the API recovers;
  nothing is lost, because the cursor has not moved. If the vendor changed the
  response shape, fix `records_path` in the config.
- **Rows with `error`** are expected occasionally. Silver routes them to its
  quarantine table. A sudden rise usually means the vendor changed a field name
  (`missing id`, `bad <cursor>`).
- **Re-run a period:** run a `backfill` window. It is append-only, and silver
  dedups.
- **Reset a source's cursor:** use a new `pipeline_name`. Its first
  incremental run starts from `initial_value`. Do not delete `_dlt_loads/`
  objects: silver uses them to decide which data files are complete. Never
  delete bronze data prefixes to do this either: dlt also wipes local state
  when its dataset disappears.
- **Adding a source:** a config JSON in `CONFIG_URI`, a function (image +
  event/schedule), and IAM. No code change, unless the API needs a paginator
  or auth that dlt does not have.
- **Upgrading dlt** (pinned at 1.32.0 on purpose): the envelope relies on
  RESTClient internals. These are the response hook replacing the default
  `raise_for_status`, and every paginator parsing through `response.json()`.
  Re-run the whole test suite and the image smoke test before upgrading.

## Open items

1. Run against **real S3** and a **real Lambda**: IAM minimal set, s3fs
   credentials from the function role, timing, cold-start import time.
2. **Lambda memory** with the largest realistic page; set the function memory
   from the measurement.
3. **Glue 6.0** run of the silver transform (tested locally on pyspark 4.1.1
   only), and the **Iceberg v2 MERGE** (untested sketch).
4. **Audit requirement:** bronze is verbatim per *record*, not per *response*.
   Top-level response fields (`pagination`, totals) and the whitespace between
   records are not kept. If exact response bytes are ever needed for a dispute,
   add a separate raw zone for those vendors only (see DESIGN.md §8).
5. Silver: decide the Iceberg table layout, MERGE cadence, and the quarantine
   table's retention.
6. S3 lifecycle rules for `_dlt_loads`/`_dlt_pipeline_state`: one small object
   each per run.
