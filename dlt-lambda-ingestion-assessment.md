# dlt on AWS Lambda — ingestion feasibility assessment

**Subject:** Evaluating dlt (dlthub) as the primary ingestion mechanism into the data lake, under a Lambda-only compute constraint
**Library version assessed:** dlt 1.32.0
**Date of measurements:** 22 November 2026

---

## 1. Summary and recommendation

dlt is a viable primary ingestion library for this platform, but **not as a zip-packaged Lambda**. The recommended shape is:

1. **Package as container images on ECR**, not zip archives. The zip size limits are breached by any dlt build that writes Parquet, and native database drivers (ODBC for SQL Server, Instant Client for Oracle) cannot be installed into a zip cleanly in any case.
2. **Build a small number of shared images — two — not one image per source.** dlt's sources are declarative, so a single handler can serve many sources driven by configuration. Lambda *functions* are cheap config and should be one per source; *images* are the expensive artefact and should be few.
3. **Write append-only Parquet to a bronze landing area. Do not write Iceberg from the ingestion function.** Promote bronze to silver Iceberg in a separate, serialised step. Parquet applies to API sources as well as databases — dlt does not emit raw responses in any format, so where the original bytes are genuinely required the answer is a separate raw zone, not a different bronze format.
4. **Express the existing `mode` / year / month / day path convention through dlt's `layout` and `extra_placeholders`.** It maps cleanly. Bronze is addressed by path, not catalogued, so no crawler or partition registration is needed — but the layer must be append-only, because a `replace` load destroys sibling partitions.
5. **Use dlt's native partitioning and split-loading mechanisms** for large initial loads and backfills. They replace hand-written windowing logic.
6. **Rely on dlt's destination-backed state** for incremental cursors. It survives Lambda's ephemeral filesystem, with one hard constraint: one concurrent invocation per pipeline name.

The main residual risks are the 15-minute execution ceiling for very large tables, and the maturity of dlt's direct Iceberg write path. Both are addressed below.

---

## 2. The constraint

Ingestion must run on Lambda. Sources range from REST APIs to relational databases. A prior internal investigation found that an unpacked dlt package would not fit a zip-based Lambda deployment. This document confirms that finding, quantifies it, and sets out the alternative.

---

## 3. Packaging: measured footprint

### 3.1 The limits

| Item | Documented quota |
|---|---|
| .zip package, uploaded via API or console | 50 MB zipped |
| .zip package contents, including layers and custom runtimes | 250 MB unzipped |
| Layers per function | 5 (counting toward the same 250 MB) |
| Container image | 10 GB uncompressed |
| `/tmp` ephemeral storage | 512 MB to 10,240 MB |

Source: AWS Lambda quotas documentation. The critical detail is that **Lambda layers share the 250 MB budget** — moving a large dependency into a layer buys nothing.

### 3.2 Measured package sizes

All builds installed with `pip --target`, targeting `manylinux2014_x86_64`, Python 3.12, wheels only — i.e. the way a Lambda build would resolve them.

| Build | Unzipped | Zipped |
|---|---|---|
| `dlt` (core only) | 72 MB | 21 MB |
| `dlt[sql_database]` | 100 MB | 29 MB |
| `dlt[filesystem]` | 119 MB | 42 MB |
| `dlt[parquet]` | 212 MB | 63 MB |
| `dlt[filesystem,parquet]` | **259 MB** | **84 MB** |
| `dlt[athena,filesystem]` | **265 MB** | **86 MB** |
| `dlt[filesystem,parquet,sql_database]` + pymssql + oracledb | **320 MB** | — |
| `dlt[filesystem,pyiceberg]` | **383 MB** | — |

**Conclusion: any build that writes Parquet exceeds both zip limits.** The dominant contributor is pyarrow at 140 MB, of which `libarrow.so` alone is 63 MB. botocore contributes a further 31 MB.

This matches dltHub's own published guidance, which recommends Docker packaging for Lambda on exactly these grounds.

### 3.3 How far trimming gets you

An aggressive trim was attempted on `dlt[filesystem,parquet]` to test whether zip is salvageable:

- removed `pyarrow/tests`, `pyarrow/include`, `pyarrow/src`, the Flight engine, static libraries and `.pyi` stubs
- pruned `botocore/data` to the four service models actually used (s3, sts, secretsmanager, sso)
- stripped debug symbols from every shared object
- removed `__pycache__`

**Result: 259 MB → 132 MB unzipped, 39 MB zipped.** A real Parquet load was executed successfully against this build, so it does work.

A pyarrow-free variant for JSON-only landing is much smaller: **`dlt[filesystem]` trimmed is 44 MB unzipped / 10 MB zipped**, and was verified to load JSONL to a filesystem destination.

### 3.4 Two trimming traps found

These cost time if discovered in CI rather than here:

- **dlt's `.dist-info` cannot be deleted.** `dlt/version.py` resolves the library version through `importlib.metadata`. Stripping package metadata produces `PackageNotFoundError: No package metadata was found for dlt` at import.
- **`libarrow_substrait.so` cannot be deleted.** `pyarrow.lib` links it at load time even though nothing in dlt uses Substrait. `libarrow_flight.so` (19 MB) is safe to remove; Substrait is not.

### 3.5 Position on trimming

Trimming is **not** recommended as the production path. At 132 MB there is no headroom for application code or drivers, it must be revalidated on every dlt and pyarrow upgrade, and it fails entirely the moment pyiceberg (383 MB) or a native driver is added. It is worth documenting as a fallback for the subset of sources that only land raw JSON.

---

## 4. Why containers, beyond size

Package size is the visible constraint. Native drivers are the decisive one.

dlt's `sql_database` source reaches SQL Server through SQLAlchemy with the `mssql+pyodbc` driver. pyodbc itself is 1 MB, but it is useless without unixODBC and `msodbcsql18` — operating-system packages, not Python wheels. Oracle thick mode requires Instant Client.

In a container image these are three `RUN` lines. In a zip archive they require hand-bundling shared objects into `/opt`, authoring `odbcinst.ini`, and setting `LD_LIBRARY_PATH` — then repeating the exercise at every driver CVE. For a mixed API-and-database estate this alone settles the packaging decision.

---

## 5. Deployment topology: one image, many functions

**Do not build one image per ingestion.** The image is the expensive artefact; the Lambda function is cheap configuration.

This works because dlt's sources are declarative:

- `rest_api_source()` accepts a dictionary — base URL, auth, paginator, resources, write disposition, primary key.
- `sql_database()` / `sql_table()` accept credentials, table names, backend and incremental settings.

Neither requires bespoke Python per source, so one handler can dispatch on an event payload plus a configuration document held in S3, DynamoDB or SSM Parameter Store.

### 5.1 Proposed images

| Image | Contents | Serves |
|---|---|---|
| `ingest-api` | dlt core, filesystem, parquet, requests | REST, GraphQL, file-based sources |
| `ingest-db` | the above plus SQLAlchemy, unixODBC + msodbcsql18, oracledb, psycopg, pymysql | All relational sources |

### 5.2 Functions on top

One Lambda function per source, all pointing at the same image URI, differing only in:

- environment variables (which config document to load)
- IAM role (least privilege per source — a hard requirement in a regulated environment)
- memory and timeout
- reserved concurrency
- alarms and log group

Lambda's `ImageConfig.Command` allows the handler to be overridden per function from a single image, so separate entry-point images are unnecessary.

A single combined image would also be workable — roughly 1–1.5 GB against a 10 GB ceiling. The argument for splitting is CVE surface, patch cadence and change control, not size.

### 5.3 Architecture

```
  REST APIs        Databases
      |                |
      +--------+-------+
               |
     dlt ingestion functions
   (one image, one function per source)
               |
               v
      Bronze landing on S3
       (append-only parquet)
               |
               v
       Promote to silver
     (serialised, single writer)
               |
               v
         Iceberg tables
       (Glue catalog, Athena)
```

---

## 6. Destination design

### 6.1 Do not write Iceberg from the ingestion function

dlt can write Iceberg via the filesystem destination using pyiceberg, delegating catalog resolution to pyiceberg's `load_catalog` (which supports Glue, REST and SQL catalogs). Two reasons to avoid it here:

**Open defects against 1.32.0.** As of late November 2026:

- On the filesystem destination with `table_format="iceberg"`, a column that is new in a load is silently dropped when the load normalises into more than one Parquet file. The values are lost and the column is not added to the Iceberg table, although dlt's own schema records it. (dlt-hub/dlt issue 4674.)
- Every load job issues its own `createNamespace` call, which REST catalogs count as a write. A many-table source therefore generates one catalog write per job. (dlt-hub/dlt issue 4662.)

**Architectural reason.** The Iceberg commit is a serialisation point. Fanning ingestion out across concurrent Lambda invocations produces commit conflicts and retries against the same table.

### 6.2 Recommended pattern

Ingestion functions write **append-only Parquet into bronze**, partitioned by load date. A separate, serialised step promotes bronze into silver Iceberg. Ingestion stays parallel; commits stay single-writer. The promote step can be a Glue job, an Athena `MERGE`, or a Lambda with reserved concurrency of 1 — decided by data volume, not by packaging.

### 6.3 If dlt must own the Iceberg write

Use the **athena** destination rather than filesystem + pyiceberg. It stages Parquet on S3 and has Athena perform the Iceberg write through the Glue catalog, so the heavy work happens in Athena rather than in the function. Note that a filesystem staging destination is mandatory (dlt configures one automatically if not set), and for Iceberg tables dlt round-trips through a staging table before copying into the final table — budget for the additional S3 and Athena cost.

---

### 6.4 File format for API sources, and what dlt does to a payload

A reasonable objection to writing Parquet is that for an API source the natural bronze artefact is the raw JSON response. The objection rests on a premise that does not hold: **dlt never writes the raw response, in any file format.**

By the time dlt writes a file it has already applied its naming convention, inferred and coerced types, flattened nested objects and unnested arrays into child tables. Choosing `jsonl` over `parquet` changes the encoding, not the content.

**Demonstrated.** Input payload:

```json
{"id": 101, "customerRef": "ACC-99", "updatedAt": "2026-11-22T09:00:00Z",
 "amount": {"value": "1250.00", "currency": "ZAR"},
 "tags": ["priority", "reviewed"],
 "meta": {"source": {"system": "core", "node": 7}}}
```

Written by dlt with `loader_file_format="jsonl"` — the most raw option available:

```
payments/<load_id>.<file_id>.jsonl.gz
{"id":101,"customer_ref":"ACC-99","updated_at":"2026-11-22T09:00:00+00:00",
 "meta__source__system":"core","meta__source__node":7,
 "amount__value":"1250.00","amount__currency":"ZAR",
 "_dlt_load_id":"...","_dlt_id":"qoze+c9mI0J2dA"}

payments__tags/<load_id>.<file_id>.jsonl.gz
{"value":"priority","_dlt_parent_id":"qoze+c9mI0J2dA","_dlt_list_idx":0,"_dlt_id":"..."}
{"value":"reviewed","_dlt_parent_id":"qoze+c9mI0J2dA","_dlt_list_idx":1,"_dlt_id":"..."}
```

Keys renamed to snake_case, the timestamp rewritten from `Z` to `+00:00`, nested objects flattened with a `__` separator, the array split into a child table with parent and index keys, and dlt's own columns added.

### 6.5 The real question: are the original bytes required?

This is a replayability and audit decision, not a format decision.

**Reasons the original bytes matter in this environment:**

- The data is not re-fetchable — retention windows, rate limits, or pay-per-call endpoints.
- "What exactly did the vendor send us on a given date" is a plausible dispute or regulatory question. A normalised file cannot answer it; the timestamp rewrite above is a small illustration of the fidelity already lost.
- Reprocessing is needed because dlt's inference or the chosen nesting behaviour turns out to be wrong, without re-hitting the source.

**Reasons Parquet remains correct for bronze regardless:**

- The silver Glue job would otherwise pay for JSON parsing on every run, indefinitely.
- dlt has already performed the schema inference. Writing JSONL discards that work and forces silver to repeat it each run.
- The source being an API does not change what the silver layer needs to consume efficiently.

### 6.6 Recommendation: a separate raw zone, applied selectively

Treat these as two layers rather than one format choice:

| Zone | Content | Written by |
|---|---|---|
| `raw/` | The vendor's bytes, immutable, gzipped, one object per response page | The Lambda, directly |
| `bronze/` | dlt-normalised Parquet, as specified in section 7 | dlt |

**Implementation without a second API call.** The `rest_api` source supports `response_actions`, which accepts a callable passed to the `requests` library as a response hook. The callable receives the `Response` object before dlt processes it, so it can write `response.content` to S3 and return the response unchanged. One pass over the API, no duplicate calls, no duplicate rate-limit consumption. Note that dlt marks `response_actions` as experimental, so pin the dlt version and cover it with a test.

Use the same partition convention as bronze — `raw/<source>/<endpoint>/year=.../month=.../day=.../<load_id>.<seq>.json.gz` — so one addressing scheme serves both zones.

**Apply it selectively.** Add the raw zone for APIs where the data is not re-fetchable or where an audit or dispute requirement exists — in practice, the vendor-facing sources rather than internal ones. A raw zone that is never read is pure storage and operational cost.

### 6.7 Lighter alternatives if a second zone is not wanted

| Option | Effect | Cost |
|---|---|---|
| `@dlt.resource(max_table_nesting=0)` | One table; nested objects and arrays retained intact as JSON-typed columns rather than flattened and split out. Verified: `amount`, `tags` and `meta` all survive whole. Keys are still renamed and types still coerced, so this is not raw | Silver must parse the retained structures |
| Envelope resource | Yield one row per response, e.g. `{"payload": response.text, "endpoint": ..., "fetched_at": ...}`. The raw body survives as a string column inside Parquet while dlt continues to manage state and incremental loading | Bronze is not directly queryable without parsing |

**Format can also be chosen per source.** `@dlt.resource(file_format=...)` is a per-resource hint that supersedes the `loader_file_format` passed to `pipeline.run()`, so a single pipeline can write some resources as JSONL and others as Parquet if the decision genuinely differs by source.

## 7. S3 path layout and partitioning

### 7.1 Requirement

The house convention is Hive-style `year` / `month` / `day` prefixes plus a `mode` partition taking `full`, `incremental` or `backfill`.

This is **not** a catalogue contract. Nothing crawls bronze and no Athena table is registered over it. The scheme exists so that ingestions are not dumped into a single flat directory, and so the silver Glue job has a predictable, addressable location to read for a given table, mode and date. The daily silver run normally reads the incremental prefix for that date; a full reload or a backfill lands in its own prefix and can be processed deliberately, in the correct mode, without disturbing the regular stream.

### 7.2 dlt supports this directly

The filesystem destination accepts a `layout` string with placeholders, plus `extra_placeholders` that map names to a value **or to a callable evaluated at runtime**, and `current_datetime` to control the date tokens.

Recommended layout:

```
{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}
```

```python
from dlt.destinations import filesystem

destination = filesystem(
    bucket_url="s3://<bronze-bucket>/bronze",
    layout="{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}",
    extra_placeholders={"mode": event["mode"]},  # "full" | "incremental" | "backfill"
    current_datetime=partition_datetime,  # optional, see 7.5
)
```

Verified output:

```
bronze/events/mode=incremental/year=2026/month=11/day=22/1795375461.0553806.b10a3e15ca.jsonl.gz
```

### 7.3 Placeholder reference

**Date and time** (names follow pendulum format tokens):
`{YYYY} {YY} {Y}` · `{MMMM} {MMM} {MM} {M}` · `{DD} {D}` · `{HH} {H} {mm} {m} {ss} {s}` · `{dddd} {ddd} {dd} {d}` · `{SSSS} {SSS} {SS} {S}` · `{Q}`

**Standard:**
`{schema_name} {table_name} {load_id} {file_id} {ext} {curr_date} {timestamp} {timestamp_ms} {load_package_timestamp} {load_package_timestamp_ms}`

**Custom:** anything supplied via `extra_placeholders`, as a literal string or a callable — which is how `mode` is computed per invocation from the event payload.

### 7.4 Constraint: `{table_name}` must come first

Only `{schema_name}` may precede `{table_name}`. Attempting to place the mode partition above the table name is rejected at pipeline sync:

```
CantExtractTablePrefix: Cannot extract unique table prefix in
layout=mode={mode}/{table_name}/... Only ('schema_name',) are allowed
before {table_name} but you have ['mode'] present.
```

This is not a limitation in practice — it is also what Athena requires, namely a single table location per table with `mode`, `year`, `month`, `day` as partition keys beneath it.

### 7.5 Which date the partition reflects

dlt's date placeholders render the **load time**, not any timestamp inside the data. Resolution order is: explicit `current_datetime`, then the load package timestamp, then now. A naive value is treated as UTC, and dlt uses UTC rather than the process timezone by design, so a timezone shift cannot split a single load across two prefixes.

Two options for backfills:

| Option | Behaviour | Trade-off |
|---|---|---|
| Leave `current_datetime` unset | Backfill files land under today's date with `mode=backfill` | Simplest; the Glue run is pointed at the prefix explicitly |
| Set `current_datetime` to the window start | Backfill partitions mirror the data dates | A single failed window is independently re-runnable and re-processable later |

**Recommendation:** leave the default for incremental and full runs; set `current_datetime` to the window start for backfills.

Because bronze is not catalogued, the prefix is the only metadata there is. A backfill sitting under its own data date is self-describing and independently re-runnable; one sitting under the run date tells a later reader nothing about what it contains. The consequence is that a multi-window backfill scatters across several date prefixes, so the silver job must accept a date *range* for backfill mode rather than a single day.

### 7.6 Critical: bronze must be append-only

**Verified behaviour:** a `write_disposition="replace"` load executed with `mode=full` **deleted the entire `mode=incremental` partition**. dlt truncates the whole table directory (`bronze/events/`), because the table prefix is everything up to and including `{table_name}`. dlt has no concept of the mode or date partitions below that point, so the physical isolation provided by the layout is not respected by replace semantics.

Consequences:

- **Never use `replace` against the bronze landing area.** Use `append` for every mode, including `full`.
- A full reload is expressed as `mode=full` plus append. The promote step decides that a full load supersedes earlier data for that table, by load ID or by partition.
- If a genuine truncate is ever required, perform it as a scoped S3 prefix delete outside dlt, against the prefix actually intended.

This also means the `mode` partition carries real semantic weight downstream: it is the signal to the promote step about how to interpret the files, not merely a physical separator.

### 7.7 dlt's internal tables bypass the layout

`_dlt_loads`, `_dlt_pipeline_state` and `_dlt_version` do **not** follow the custom layout. Each is written to a flat folder directly under the dataset:

```
bronze/_dlt_loads/<pipeline>__<load_id>.jsonl
bronze/_dlt_pipeline_state/<pipeline>__<load_id>__<schema_hash>.jsonl
bronze/_dlt_version/<pipeline>__<load_id>__<schema_hash>.jsonl
```

Verified, and explicit in the dlt source (`get_table_prefix`: "dlt tables do not respect layout (for now)").

This is desirable — state restore remains a single cheap prefix listing rather than a scan across date partitions. The "for now" qualifier means the behaviour should be pinned to a tested dlt version and re-checked on upgrade.

The silver job simply does not read these prefixes. One of them is nevertheless useful to it — see 7.9.

### 7.8 No catalogue over bronze

Because bronze is addressed by path rather than queried through a catalogue, a whole class of work disappears: no crawler, no `ALTER TABLE ADD PARTITION`, no partition projection, no Glue partition records, and none of the cost, latency or failure modes attached to them. dlt needs to do nothing beyond writing files to the correct prefix, which it does.

Two points worth keeping:

- **Retain the `key=value` form anyway.** It costs nothing, it is self-describing to anyone reading the bucket by hand, and it leaves the option of pointing an Athena table at a bronze prefix for ad-hoc debugging without moving a single file. If that is ever wanted, Athena partition projection is the cheap route — `projection.mode.type=enum` with the three mode values, `projection.{year,month,day}.type=integer` with ranges and digit counts, `projection.enabled=true`, and a `storage.location.template` carrying a placeholder for every partition column and terminated by a forward slash. That is an available option, not a requirement, and nothing in the ingestion design depends on it.
- **The silver job owns the read path.** It is given table, mode and date (or date range) and constructs the prefix. That contract should be written down somewhere both sides can see it, because with no catalogue there is nothing to discover it from.

### 7.9 Knowing that a prefix is complete

Without a catalogue there is no commit boundary, so the silver job could in principle read a prefix while a Lambda is still writing into it, or after one has died halfway through. dlt already supplies the marker needed to prevent this, at no extra cost:

- Every data file is named `{load_id}.{file_id}.{ext}`, so the load ID is embedded in the object key.
- dlt writes an object into `bronze/_dlt_loads/` **only when a load completes**, and its name carries the same load ID.

The silver job's read therefore becomes: list the target prefix, list `bronze/_dlt_loads/`, and process only the files whose load ID appears in both. Files belonging to an in-flight or failed load are skipped, and are picked up on a later run once that load completes. This gives an atomic, catalogue-free commit boundary — and it is the same mechanism dlt itself uses to decide which pipeline state is current.

Two smaller operational points follow:

- A day on which no data arrives produces no prefix at all. The silver job must treat a missing path as "nothing to do" rather than as an error.
- A parallel backfill produces one load ID per window. All of them must be present in `_dlt_loads` before that backfill is treated as whole.

### 7.10 File naming and compression

File names are `{load_id}.{file_id}.{ext}`, where `load_id` is an epoch timestamp and `file_id` a random suffix. Names are therefore unique across concurrent writers, which is what makes the append-only model safe when parallel `end_value` backfill windows write into the same table.

dlt gzips JSONL by default (`.jsonl.gz`). Parquet is written with its own internal compression. Note that the date in the path reflects load time and is independent of any timestamp column inside the Parquet file.

### 7.11 Worked example

| Scenario | `mode` | `current_datetime` | Resulting prefix |
|---|---|---|---|
| Nightly incremental | `incremental` | unset (run date) | `events/mode=incremental/year=2026/month=11/day=22/` |
| Initial full load | `full` | unset (run date) | `events/mode=full/year=2026/month=11/day=22/` |
| Backfill, window 2025-03 | `backfill` | `2025-03-01` | `events/mode=backfill/year=2025/month=03/day=01/` |

All three append. The silver job is invoked with the table, the mode and the date (or date range), constructs the prefix from them, filters to completed load IDs as in 7.9, and applies the strategy the mode implies: merge for incremental, supersede-by-table for full, merge-into-window for backfill.

---

## 8. Large initial loads and backfills

dlt provides native equivalents of the two patterns already in use internally: a batched query loop, and a Step Function that splits ingestion into windows.

### 8.1 Partitioning — `end_value` (replaces Step Function windowing)

```python
sql_table(
    table="transactions",
    incremental=dlt.sources.incremental(
        "updated_at",
        initial_value=window_start,
        end_value=window_end,
    ),
    backend="pyarrow",
    chunk_size=50000,
)
```

Key properties, per dlt's documentation and confirmed by test:

- **Incremental resources with `end_value` set do not use the state.** The backfill is stateless, so windows can run in parallel without racing each other. A test run with `end_value` left the cursor unwritten; the subsequent unbounded run wrote it normally.
- **Ranges are half-closed** — `initial_value` inclusive, `end_value` exclusive — so chained windows neither overlap nor leave gaps.
- **Deduplication is disabled** while `end_value` is set.
- After the backfill, resume normal incremental loading by using the final window's `end_value` as the new `initial_value`.

Window boundaries are still computed by the caller: take min and max of the cursor column, split into N ranges, and pass each range to a function invocation. A Step Function `Map` state over the ranges reproduces the existing design, minus the hand-written window SQL.

### 8.2 Split loading — `row_order` + `add_limit` (replaces the batched query loop)

```python
messages = sql_table(
    table="messages",
    incremental=dlt.sources.incremental("created_at", row_order="asc"),
)
while not pipeline.run(messages.add_limit(max_time=720)).is_empty:
    ...
```

- `add_limit` in 1.32.0 has the signature `(max_items=None, max_time=None, count_rows=False)`. Verified directly against the installed library.
- **`row_order="asc"` is mandatory.** Without it, rows will be skipped.
- `add_limit` counts *yields* (pages/batches), not records, unless `count_rows=True`. It closes the generator rather than skipping items, and the final page is not trimmed — expect slightly more rows than the limit implies.

### 8.3 Mapping onto Lambda

| Scenario | Mechanism | Orchestration |
|---|---|---|
| Initial load / backfill | `initial_value` + `end_value` per window | Step Function `Map` over precomputed windows, parallel |
| Steady-state incremental | `row_order="asc"` + `add_limit(max_time=N)` | Single function, loop until `is_empty`, concurrency 1 |

Set `max_time` to the function timeout minus a safety margin (for example 720s on a 900s function). Return `is_empty` from the handler and let a Step Function `Choice` state re-invoke until it is true. The orchestrator holds no state of its own — each invocation advances the cursor independently.

### 8.4 Crash behaviour

dlt's `run()` first synchronises with the destination, then checks whether the previous run's data was fully processed. If not, it normalises and loads the pending items **and exits** without extracting anything new. This is dlt's resumption mechanism, but it depends on the pipeline working directory surviving, which `/tmp` will not across a cold start.

For an append-only bronze layer the simpler answer is to let a failed window re-extract: load packages are atomic per load ID and state only advances on completed loads, so re-running a failed window is safe. If stronger resumption is wanted later, the working directory can be persisted to S3 at the end of each invocation.

### 8.5 Concurrency rules that follow

- `end_value` partitions: **parallel-safe**, because no state is touched.
- Split loading with `add_limit`: **stateful**, therefore one invocation at a time per pipeline name.

---

## 9. Pipeline state

### 9.1 What it is

The pipeline state is a Python dictionary holding each resource's incremental `last_value`, deduplication hashes, and anything written via `dlt.current.resource_state()`. It lives in the pipeline working directory and is also written to the destination, so that a clean start can restore it. This is the same mechanism that makes dlt work under Airflow, where every task receives a fresh filesystem.

### 9.2 `restore_from_destination`

Every `pipeline.run()` begins by calling `sync_destination()`. That restores state and schemas from the destination when the remote state carries a higher version number than the local copy, replacing local state, schemas and intermediate files. The setting defaults to on. Disabling it is intended for environments that keep the working directory between runs — the opposite of Lambda, so it should be left enabled.

### 9.3 Where it is stored

State is written to the `_dlt_pipeline_state` table at the destination, identified by **pipeline name + destination location (from credentials) + dataset name**. Restoring works by joining `_dlt_pipeline_state` to `_dlt_loads` and taking the newest *completed* load — which is why a half-finished invocation cannot advance a cursor.

On the filesystem destination these appear as JSONL objects:

```
bronze/_dlt_loads/<pipeline>__<load_id>.jsonl
bronze/_dlt_pipeline_state/<pipeline>__<load_id>__<schema_hash>.jsonl
bronze/_dlt_version/<pipeline>__<load_id>__<schema_hash>.jsonl
```

An actual state row contains: `version`, `engine_version`, `pipeline_name`, `state` (a compressed blob), `created_at`, `version_hash`, `_dlt_load_id`.

### 9.4 Verification performed

A pipeline with an incremental cursor was run, then run again from a completely empty `pipelines_dir` to simulate a fresh execution environment. The second run restored the cursor correctly from the destination and loaded zero rows. **Lambda's ephemeral `/tmp` is therefore not an obstacle to incremental loading**, provided the conditions below hold.

### 9.5 Operating conditions and hazards

| Item | Detail |
|---|---|
| **Identity** | Pipeline name, destination location and dataset name must all stay stable. Changing any one silently creates a new pipeline, i.e. a full reload. Pin them in configuration. |
| **Concurrency** | Two concurrent invocations of the same pipeline both read the same state and both write a new one. Last writer wins and the other's advance is lost. Enforce reserved concurrency of 1, or SQS FIFO with the pipeline name as message group ID. |
| **Known defect** | `dlt.pipeline(restore_from_destination=False)` passed as a keyword argument is silently ignored; `pipeline.config.restore_from_destination` remains `True` (dlt-hub/dlt issue 4584, open). If the setting is ever needed, apply it through config or environment variable and assert the value. |
| **Growth** | Each run writes one state object and one loads object. A five-minute pipeline produces roughly 105,000 small S3 objects per year. Harmless, but plan an S3 lifecycle rule. dlt's own guidance is not to place large collections in pipeline state. |
| **Reset lever** | If the destination dataset is dropped while local state remains, dlt wipes the local state. Useful as a deliberate reset; hazardous if someone deletes the bronze prefix by accident. |

---

## 10. Lambda runtime configuration checklist

| Area | Setting |
|---|---|
| Working directory | `DLT_PROJECT_DIR`, `DLT_DATA_DIR`, `DLT_PIPELINE_DIR` all set to `/tmp`. dlt otherwise writes under the user home directory, which is read-only. |
| Ephemeral storage | 2–10 GB. dlt buffers extracted and normalised files on local disk before loading. Bound `buffer_max_items` and `file_max_items` so a wide table cannot fill `/tmp`. |
| Secrets | Read from Secrets Manager at init and inject into the environment under dlt's `SOURCES__` / `DESTINATION__` prefixes. Lambda masks neither environment variables nor files. |
| Concurrency | Reserved concurrency 1 per stateful pipeline; unrestricted for `end_value` partitions. |
| Cold start | Lambda lazy-loads container images, pulling only the blocks touched at startup, so image size matters less than expected. The real cost is importing dlt and pyarrow — keep those at module scope so they land in the init phase. |
| SnapStart | Now supported for container images, but **not** with ephemeral storage above 512 MB. For most dlt pipelines this rules it out. Treat it as a conscious trade: large `/tmp` or SnapStart, not both. |
| SQL backend | `sqlalchemy` (default, no extra dependencies) for small tables; `pyarrow` for stable destination typing and throughput. Avoid `connectorx` — it ignores `chunk_size`, which is incompatible with a bounded-duration function. |

### Timeouts

The synchronous ceiling remains 15 minutes. AWS now offers a 90-minute timeout, but only for **asynchronous and event-source-mapping invocations on Lambda Managed Instances**; synchronous invocations still cap at 15 minutes and the init phase remains capped at 15 minutes regardless. Lambda Managed Instances launched in a limited region set (N. Virginia, Ohio, Oregon, Tokyo, Ireland) — **confirm availability in the target region before designing around it.** Cape Town was not in the launch set.

The split-loading pattern in section 8.2 removes the dependency on long timeouts entirely, which is the more robust answer.

---

## 11. Portability note

Keep the Lambda handler a thin wrapper around a `run_pipeline(config)` function. The same container image then runs unchanged as an ECS/Fargate task if any single source eventually outgrows the 15-minute window. This costs nothing now and avoids a rewrite later.

It is also worth testing the underlying constraint. "Ingestion must be Lambda only" is worth clarifying — whether it protects against long-lived compute in the account, enforces a VPC policy, or is a cost control. The answer may widen the options without changing the code.

---

## 12. Validation plan

In priority order:

1. **Build the `ingest-db` image (section 13.4) and prove a `mssql+pyodbc` connection from a VPC-attached Lambda.** Highest-risk unknown; roughly a half-day spike. Settle the `packages.microsoft.com` egress question for the build pipeline at the same time, and confirm `oracledb` thin mode satisfies the bank's Oracle authentication requirements so no Instant Client is needed.
2. **Run the largest realistic table through a single invocation with `backend="pyarrow"`.** Measure wall time, peak memory and peak `/tmp` usage. This number sizes everything downstream.
3. **Prove state restore end to end** with the real destination: run, kill an invocation deliberately, re-run. Confirm no duplicates and no gaps.
4. **Prove the layout end to end**: run one load in each mode against a real bucket, confirm the prefixes, confirm no `replace` disposition is reachable from the handler, and confirm the silver job can locate a given table/mode/date prefix and correctly skips files belonging to an incomplete load.
5. **Prove the split-loading loop** with `add_limit(max_time=...)` across two or more invocations against a real table.
6. **Decide which API sources warrant a raw zone** and prove the `response_actions` tee against one of them, confirming no duplicate API calls and no change to the bronze output.
7. **Only then** decide whether the bronze-to-silver promote step is an Athena `MERGE` or a Glue job.

---

## 13. Reference implementation

Illustrative, not production code. Pinned versions are examples — pin whatever the build resolves and record it in a lockfile.

### 13.1 Repository layout

```
.
├── docker/
│   ├── Dockerfile.api
│   └── Dockerfile.db
├── requirements/
│   ├── base.txt
│   ├── api.txt
│   └── db.txt
└── src/
    ├── handlers/
    │   ├── api.py            # Lambda entry point for API sources
    │   └── db.py             # Lambda entry point for database sources
    └── ingest/
        ├── config.py         # load and validate the source config document
        ├── secrets.py        # Secrets Manager -> dlt environment variables
        ├── destination.py    # bronze filesystem destination, layout and mode
        ├── pipeline.py       # the portable core: build and run
        ├── raw.py            # optional raw-zone response hook (6.6)
        └── sources/
            ├── api.py        # builds a rest_api_source from config
            └── database.py   # builds a sql_table resource from config
```

Both images contain the **same** `src/` tree. Only the requirements file and the OS packages differ, so a bug is fixed in one place and the API and database paths cannot drift apart.

### 13.2 Requirements

`requirements/base.txt`
```
dlt[filesystem,parquet]==1.32.0
boto3==1.40.*
```

`requirements/api.txt`
```
-r base.txt
```

`requirements/db.txt`
```
-r base.txt
dlt[sql_database]==1.32.0
pyodbc==5.2.0
oracledb==3.4.0
psycopg[binary]==3.2.9
PyMySQL==1.1.1
```

Notes:

- `rest_api_source` ships in dlt core, so the API image needs nothing beyond `base.txt`.
- `boto3` is pinned explicitly rather than relied on from the base image. dlt pulls `botocore` transitively via `s3fs`/`aiobotocore`, but not `boto3`.
- `oracledb` runs in **thin mode** by default and requires no Oracle Instant Client. Only thick mode does, so nothing Oracle-specific is installed in the image. Confirm thin mode covers the authentication method the bank uses before relying on this.

### 13.3 `docker/Dockerfile.api`

```dockerfile
FROM public.ecr.aws/lambda/python:3.12

COPY requirements/ /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/api.txt \
      --target "${LAMBDA_TASK_ROOT}" \
 && rm -rf /tmp/requirements

COPY src/ "${LAMBDA_TASK_ROOT}/"

ENV DLT_PROJECT_DIR=/tmp \
    DLT_DATA_DIR=/tmp \
    DLT_PIPELINE_DIR=/tmp \
    PYTHONDONTWRITEBYTECODE=1

CMD ["handlers.api.handler"]
```

The `CMD` is the default handler. Each Lambda function can override it through `ImageConfig.Command`, which is what lets one image serve many functions (5.2).

### 13.4 `docker/Dockerfile.db`

```dockerfile
FROM public.ecr.aws/lambda/python:3.12

# Microsoft ODBC Driver 18 for SQL Server.
# The Lambda Python 3.12 image is Amazon Linux 2023 (microdnf, symlinked as dnf).
# msodbcsql18 is published in Microsoft's RHEL 9 repository; the
# amazonlinux/2023 repository does NOT carry it — verified against
# packages.microsoft.com, which lists msodbcsql17/18, mssql-tools18 and
# unixODBC under rhel/9/prod only.
RUN printf '%s\n' \
      '[packages-microsoft-com-prod]' \
      'name=Microsoft Production' \
      'baseurl=https://packages.microsoft.com/rhel/9/prod/' \
      'enabled=1' \
      'gpgcheck=1' \
      'repo_gpgcheck=1' \
      'gpgkey=https://packages.microsoft.com/keys/microsoft.asc' \
      > /etc/yum.repos.d/mssql-release.repo \
 && ACCEPT_EULA=Y dnf install -y msodbcsql18 unixODBC \
 && dnf clean all \
 && rm -rf /var/cache/dnf

COPY requirements/ /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/db.txt \
      --target "${LAMBDA_TASK_ROOT}" \
 && rm -rf /tmp/requirements

COPY src/ "${LAMBDA_TASK_ROOT}/"

ENV DLT_PROJECT_DIR=/tmp \
    DLT_DATA_DIR=/tmp \
    DLT_PIPELINE_DIR=/tmp \
    PYTHONDONTWRITEBYTECODE=1

CMD ["handlers.db.handler"]
```

Three points:

- The `msodbcsql18` RPM registers the driver in `/etc/odbcinst.ini` itself. No hand-written `odbcinst.ini` and no `LD_LIBRARY_PATH` juggling — which is precisely the work that the zip-packaging route would have required (section 4).
- The repo file is written with `printf` rather than fetched with `curl`, so the build does not depend on `curl` being present in the minimal base image.
- **If egress to `packages.microsoft.com` is blocked from the build environment**, mirror the RPM into the internal artefact repository and install from there. In a regulated build pipeline this is the likely path, and it should be settled before the first image build rather than during it.

### 13.5 `src/ingest/destination.py`

```python
from datetime import datetime
from typing import Optional

from dlt.destinations import filesystem

LAYOUT = "{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"
MODES = ("full", "incremental", "backfill")


def bronze(bucket_url: str, mode: str, partition_dt: Optional[datetime] = None):
    """Bronze destination. `partition_dt` overrides the date in the path and is
    used for backfills so the prefix mirrors the data window (section 7.5)."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}, expected one of {MODES}")
    return filesystem(
        bucket_url=bucket_url,
        layout=LAYOUT,
        extra_placeholders={"mode": mode},
        current_datetime=partition_dt,
    )
```

### 13.6 `src/ingest/pipeline.py`

```python
from datetime import datetime
from typing import Any, Dict, Optional

import dlt

from .destination import bronze

PIPELINES_DIR = "/tmp/dlt"


def build(cfg: Dict[str, Any], mode: str, partition_dt: Optional[datetime] = None):
    return dlt.pipeline(
        pipeline_name=cfg["pipeline_name"],  # stable: part of the state identity (9.3)
        destination=bronze(cfg["bucket_url"], mode, partition_dt),
        dataset_name=cfg["dataset"],  # stable: part of the state identity
        pipelines_dir=PIPELINES_DIR,
    )


def run(pipeline, source, file_format: str = "parquet") -> Dict[str, Any]:
    # write_disposition is hard-coded: a `replace` load would delete sibling
    # mode partitions (section 7.6). It is deliberately not configurable.
    info = pipeline.run(
        source,
        write_disposition="append",
        loader_file_format=file_format,
    )
    return {
        "is_empty": info.is_empty,
        "load_ids": list(info.loads_ids),
        "has_failed_jobs": info.has_failed_jobs,
    }
```

### 13.7 `src/ingest/secrets.py` and `config.py`

```python
# secrets.py
import json
import os

import boto3

_sm = boto3.client("secretsmanager")


def inject(secret_id: str, prefix: str) -> None:
    """Expose a secret to dlt as environment variables rather than TOML.

    {"username": "svc", "password": "..."} with
    prefix "SOURCES__SQL_DATABASE__CREDENTIALS__" becomes
    SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME, ...__PASSWORD.
    """
    payload = json.loads(_sm.get_secret_value(SecretId=secret_id)["SecretString"])
    for key, value in payload.items():
        os.environ[f"{prefix}{key.upper()}"] = str(value)
```

```python
# config.py
import json

import boto3

_s3 = boto3.client("s3")
REQUIRED = ("pipeline_name", "dataset", "bucket_url")


def load(config_bucket: str, source_id: str) -> dict:
    body = _s3.get_object(Bucket=config_bucket, Key=f"sources/{source_id}.json")["Body"]
    cfg = json.loads(body.read())
    missing = [k for k in REQUIRED if k not in cfg]
    if missing:
        raise ValueError(f"config {source_id} is missing {missing}")
    return cfg
```

### 13.8 `src/ingest/sources/database.py`

```python
from typing import Any, Dict

import dlt
from dlt.sources.sql_database import sql_table


def build(cfg: Dict[str, Any], mode: str, event: Dict[str, Any]):
    table = cfg["table"]

    if mode == "backfill":
        # end_value makes the resource stateless, so windows run in parallel (8.1)
        incremental = dlt.sources.incremental(
            table["cursor"],
            initial_value=event["window_start"],
            end_value=event["window_end"],  # half-open: end is exclusive
        )
    else:
        # row_order is mandatory for split loading or rows are skipped (8.2)
        incremental = dlt.sources.incremental(table["cursor"], row_order="asc")

    return sql_table(
        schema=table["schema"],
        table=table["name"],
        incremental=incremental,
        backend=table.get("backend", "pyarrow"),
        chunk_size=table.get("chunk_size", 50_000),
    )
```

Credentials are not passed here: `secrets.inject()` has already placed them in the environment under dlt's `SOURCES__SQL_DATABASE__CREDENTIALS__` prefix, so they never appear in a config document, a log line or a stack trace.

### 13.9 `src/ingest/sources/api.py` and `raw.py`

```python
# sources/api.py
from typing import Any, Dict

from dlt.sources.rest_api import rest_api_source

from ..raw import tee_to_raw


def build(cfg: Dict[str, Any]):
    api_cfg = dict(cfg["rest_api"])

    if cfg.get("capture_raw"):
        hook = tee_to_raw(cfg["raw_bucket"], cfg["pipeline_name"])
        for resource in api_cfg["resources"]:
            endpoint = resource["endpoint"]
            if isinstance(endpoint, str):  # shorthand form
                endpoint = resource["endpoint"] = {"path": endpoint}
            endpoint.setdefault("response_actions", []).append(hook)

    return rest_api_source(api_cfg)
```

```python
# raw.py
import gzip
import re
import uuid
from datetime import datetime, timezone

import boto3

_s3 = boto3.client("s3")
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def tee_to_raw(bucket: str, source: str):
    """A requests response hook: persist the untouched body to the raw zone.

    Runs before dlt parses anything, so no second API call and no extra
    rate-limit consumption (section 6.6). response_actions is marked
    experimental in dlt — pin the version and cover this with a test.
    """

    def _hook(response, *args, **kwargs):
        now = datetime.now(timezone.utc)
        endpoint = _SAFE.sub("-", response.request.path_url.lstrip("/")) or "root"
        key = (
            f"raw/{source}/{endpoint}/"
            f"year={now:%Y}/month={now:%m}/day={now:%d}/"
            f"{now.timestamp():.6f}.{uuid.uuid4().hex[:10]}.json.gz"
        )
        _s3.put_object(Bucket=bucket, Key=key, Body=gzip.compress(response.content))
        return response  # must be returned or the hook has no effect

    return _hook
```

### 13.10 `src/handlers/db.py`

```python
import os
from datetime import datetime

from ingest import config, pipeline as pl, secrets
from ingest.sources import database

SAFETY_MARGIN_S = 120
MIN_BUDGET_S = 60


def handler(event, context):
    cfg = config.load(os.environ["CONFIG_BUCKET"], event["source_id"])
    mode = event.get("mode", "incremental")

    secrets.inject(cfg["secret_id"], "SOURCES__SQL_DATABASE__CREDENTIALS__")

    partition_dt = datetime.fromisoformat(event["window_start"]) if mode == "backfill" else None
    pipe = pl.build(cfg, mode, partition_dt)
    source = database.build(cfg, mode, event)

    if mode != "backfill":
        # bound the run by the time actually left, not by a fixed constant (8.3)
        budget = context.get_remaining_time_in_millis() / 1000 - SAFETY_MARGIN_S
        source = source.add_limit(max_time=max(MIN_BUDGET_S, budget))

    return pl.run(pipe, source)  # {"is_empty": ..., "load_ids": [...], ...}
```

`handlers/api.py` is the same shape, calling `ingest.sources.api.build(cfg)` and injecting secrets under the API's own prefix. The orchestrator re-invokes while `is_empty` is false (8.3).

Imports sit at module scope so dlt and pyarrow are loaded during the init phase rather than on every invocation (section 10).

### 13.11 Terraform: one image, many functions

```hcl
resource "aws_lambda_function" "ingest" {
  for_each = local.sources

  function_name = "ingest-${each.key}"
  package_type  = "Image"
  image_uri     = each.value.kind == "db" ? var.db_image_uri : var.api_image_uri
  role          = aws_iam_role.ingest[each.key].arn

  timeout     = 900
  memory_size = each.value.memory_mb

  ephemeral_storage {
    size = each.value.tmp_mb            # 2048-10240, see section 10
  }

  # stateful split loading must not run concurrently (9.5);
  # stateless end_value backfills may (8.5)
  reserved_concurrent_executions = each.value.mode == "backfill" ? -1 : 1

  image_config {
    command = [each.value.handler]      # one image, many entry points
  }

  environment {
    variables = { CONFIG_BUCKET = var.config_bucket }
  }

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.ingest.id]
  }
}
```

Adding a source is a new entry in `local.sources`, a config document in S3 and an IAM role. It is not a new image, a new build or a new CI pipeline.

---

## 14. Appendix: how the measurements were produced

Packages were installed with:

```
pip install --target <dir> \
  --platform manylinux2014_x86_64 \
  --python-version 3.12 \
  --only-binary=:all: \
  --implementation cp \
  "<package spec>"
```

Sizes are `du -sm` on the resulting directory (unzipped) and on a `zip -r` archive of its contents (zipped), excluding application code. Functional checks — a Parquet load, a JSONL load, incremental state restore from an empty working directory, and `end_value` backfill statelessness — were executed against the installed library on Linux x86_64, Python 3.12, dlt 1.32.0.

Figures and open issues are current as at 22 November 2026 and should be re-checked against the dlt release notes before implementation begins.
