# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`api_bronze`: a dlt-based AWS Lambda that lands REST API records into an S3 bronze layer as Parquet, **one row per record, with the record's JSON text cut verbatim** from the response body (never parsed and re-serialised). A separate Spark/Glue job (`silver/silver_transform.py`) parses bronze into typed silver columns.

README.md is the operator-facing spec (bronze contract, config reference, run modes, deployment). DESIGN-api.md explains every design decision and the evidence behind it. Read DESIGN-api.md before changing behaviour. `dlt-lambda-ingestion-assessment.md` is the earlier feasibility study (background only).

`db_bronze` is the sibling package for relational sources: typed Parquet with source column names, same bronze layout. Its operator spec is README-db.md and its design record is DESIGN-db.md; read DESIGN-db.md before changing db_bronze behaviour. `tests/test_db_readme.py` fails when a config key, event key, mode, errorType or metric is missing from README-db.md, so update README-db.md in the same change.

## Commands

Tooling (Python 3.12, uv, Temurin 21 for Spark) is pinned in `mise.toml`. Tasks are bash scripts in `.mise/tasks/`; the path is the task name (`.mise/tasks/docker/build` → `docker:build`, `test/_default` → `test`). Add new tasks there as executable bash files with `#MISE description=...` headers (and `#USAGE` for flags). Don't add inline `[tasks]` to `mise.toml`.

```sh
mise install                               # tools
mise run setup                             # uv sync; --silver adds pyspark
mise run lint                              # ruff check (--fix). Code is NOT ruff-formatted: don't run `ruff format`
mise run test                              # all tests; Spark tests auto-skip without pyspark
mise run test:unit                         # no Spark/Java
mise run test:spark                        # installs silver group, runs Spark tests
mise run test tests/test_modes.py::test_name   # extra args go to pytest (also test:unit / test:spark)
mise run check                             # lint + test
mise run docker:lock                       # regenerate docker/requirements.lock from uv.lock (--db: requirements-db.lock)
mise run docker:build                      # lock + image (-t tag, -p linux/arm64|linux/amd64; --db: db_bronze image, amd64 only)
mise run docker:smoke                      # build + invoke via Lambda Runtime Interface Emulator (--db: db_bronze image)
mise run test:integration                  # db_bronze against docker-compose engines (-m integration; plain `test` skips them)
mise run test:integration --image          # same suite through the emulator in the db image (adds Oracle)
mise run test:integration --inside         # pytest inside the image on the compose network (Oracle ConnectorX spikes, Db2 off x86_64)
mise run test:integration --engines mssql  # limit engines; --keep leaves them up; --no-build reuses the image; -t tag
mise run bench                             # split benchmark
```

`docker/requirements.lock` and `docker/requirements-db.lock` must be regenerated whenever dependencies change; the image installs with `--require-hashes` (`docker:build` does it automatically).

Tests need no network or AWS: `tests/conftest.py` runs an in-process fake REST API (`EventsAPI`, routable bytes overrides, 500s, auth checks) and uses a local directory as the bucket (`file://`). `tests/test_silver.py` imports `silver_transform` by inserting `silver/` on `sys.path`.

db_bronze unit tests (`tests/test_db_*.py`) run end to end on SQLite (a file in `tmp_path`) on both backends. `tests/integration/` needs Docker; SQL Server needs Rosetta on arm64, Db2 and its tests are x86_64 only (see DESIGN-db.md section 10).

## Architecture

Request flow: `handler.handler` → `config.SourceConfig` / `RunRequest` → `pipeline.run_pipeline` → `source.make_resource` (one dlt resource per configured endpoint) → `envelope.envelope_records` → `split.split_records_parsed`.

- **`split.py`** — the core trick. Uses `json.JSONDecoder.raw_decode` to find record boundaries and slices the original text, so payload bytes are identical to what the vendor sent. Rejects duplicate records keys (json.loads would take the last, a naive scanner the first).
- **`envelope.py`** — drives `RESTClient.paginate` and turns each page into an Arrow table matching `RECORD_SCHEMA`. Yielding Arrow tables bypasses dlt's normaliser (no flattening/renaming/coercion). Implements the broken-response policy: unusable 2xx page → raw body written to `_quarantine/` via fsspec and `BrokenPageError` (whole run fails, nothing lands, cursor holds); bad individual record → row lands with `error` set and null cursor; invalid UTF-8 → single `payload_raw` row.
- **`source.py`** — builds dlt resources. Run mode (`incremental` / `full` / `backfill`) maps to `initial_value`/`end_value` on `dlt.sources.incremental` over the `updated_at_utc` column. Endpoints without `incremental` are snapshot resources (mode=full only).
- **`pipeline.py`** — portable core (handler is a thin wrapper so the same code could run on ECS). Calls `pipe.drop()` every invocation so state is always restored from the destination (warm container == cold start). Bronze path layout set via filesystem `layout` + `extra_placeholders={"mode": ...}`; backfills date the path by window start. Split loading (`add_limit(max_time=...)`) applies only in incremental mode when every resource is `sorted_ascending`.
- **`handler.py`** — loads config from `${CONFIG_URI}/${source_id}.json` or inline `config`, computes time budget from Lambda remaining time minus `SAFETY_MARGIN_S`, and unwraps `BrokenPageError` from dlt's `PipelineStepFailed` so it becomes the Lambda `errorType`.
- **`config.py`** — strict dataclasses; unknown keys are rejected. **`secrets.py`** resolves `{"env": ...}` / `{"secret_id": ...}` references in `client.auth` and `client.headers` (Secrets Manager values cached per container).

## Invariants and dlt gotchas (don't break these)

- **dlt is pinned at exactly 1.32.0.** The envelope relies on RESTClient internals: passing `hooks` replaces the default `raise_for_status` (the hook calls it itself), and every paginator parses via `response.json()`, which the hook swaps for stdlib `json` so pagination and splitting use one parser (simplejson rejects `NaN`; stdlib accepts it). Upgrading requires the full test suite plus the image smoke test.
- The `dlt.sources.incremental(...)` **must be the resource argument's default value** (hence `# noqa: B008`). Moving it into the function body silently disables state and filtering.
- requests' `JSONDecodeError` is not a `json.JSONDecodeError`; catch both.
- RESTClient turns a missing/`null` records key into an empty page with only a warning; `split.py` is what makes that a hard failure.
- Bronze is **append-only**. Never add `write_disposition="replace"` — it deletes the entire table folder including other `mode=` partitions.
- `pipeline_name`, `dataset_name`, `bucket_url` form the state identity; changing any silently restarts from `initial_value`.
- Parquet compression is set via env `DATA_WRITER__COMPRESSION` (dlt default is snappy; we default to zstd).
- Silver completeness: a data file counts only if its `load_id` has a `status = 0` entry in `_dlt_loads`.

## db_bronze architecture

Request flow: `handler.handler` → `config.SourceConfig` / `RunRequest` → `pipeline.run_pipeline` → `preflight.preflight` → `source.make_resource` (one dlt resource per table) → `extract.read_connectorx` / `read_pyarrow` → Arrow batches with `_extracted_at` appended. `validate`, `plan` and `finalize` modes return before any extraction.

- **`config.py`** — strict dataclasses; `EVENT_KEYS` lists every event key, anything else is rejected. Default backend comes from the dialect alone (`default_backend`), never from runtime.
- **`query.py`** — SQLAlchemy Core statements: cursor window, keyset page, offset page, snapshot, count. **`urls.py`** builds both the SQLAlchemy and ConnectorX URLs from the discrete connection fields.
- **`extract.py`** — the two backends. pyarrow converts rows to Arrow from reflected types; ConnectorX batches are yielded as they come, and `RuntimeError`/`PanicException` (a `BaseException`) around the ConnectorX call become `ConnectorXFailed`.
- **`preflight.py`** — rule tables over reflected types (decimal limits, per-dialect deny list, `MIN_SERVER_VERSION`). **`source.py`** holds the schema contract and the backend-change guard (`BackendChanged`).
- **`plan.py`, `manifest.py`** — `plan` and `finalize` events for Step Function orchestration; manifests list the loads of a full snapshot.
- **`errors.py`** — every handler-visible error derives from `DbBronzeError`; the handler finds it in dlt's `PipelineStepFailed` chain and its class name becomes the Lambda `errorType`. Add new types there, with a `metric`.

## db_bronze invariants (don't break these)

- `dlt.sources.incremental(...)` **must be the resource argument's default value** in `source.make_resource` (hence `# noqa: B008`), as in api_bronze.
- Bronze is **append only**: `write_disposition="append"`, never `replace`.
- **Freeze contract**: `SCHEMA_CONTRACT` is `data_type: freeze`, so a type change on an existing column fails the run. This is why the backend is stored in resource state and a change fails first with `BackendChanged`: the backends produce different Arrow types. Switching backend means a new table `name`.
- Every naive timestamp column gets the resource hint `{"timezone": False}`. Without it dlt labels naive values UTC without shifting them. `naive_timezone` converts cursor bounds only, never data.
- **Never cut a ConnectorX stream short** (nor a MySQL traditional stream): the query keeps running on the server with the connection open. Split loading is keyset only for those; `time_budget` skips them and validation rejects `max_time_s` beside them.
- `lag` re-lands its whole window every run (dlt deduplicates nothing under `lag`); silver deduplicates on (`primary_key`, cursor) by latest `_extracted_at`.
- A `full` load counts only if a manifest lists it; the manifest is written after every load succeeded.
- Paged snapshots and parallel backfills use their own `pipeline_name`, never the incremental's.
- Rules that depend on a type or engine need integration evidence: DESIGN-db.md labels each finding *researched*, *prototyped* or *integration*. Keep the label honest.

## Status

Unit/integration tested locally and image smoke-tested via the emulator. Not yet validated: real S3, real Lambda, Glue 6.0. db_bronze additionally passed its integration suite on Postgres, MySQL, SQL Server, Oracle and Db2, with the unverified items listed in README-db.md "Open items". `silver_transform.merge_into_iceberg` and `main` are untested sketches. See README "Open items".

## Agent skills

### Issue tracker

Issues live as local markdown files under `.scratch/<feature>/` (no remote tracker). See `.agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`), recorded as `Status:` lines in issue files. See `.agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` + `docs/adr/` at repo root. See `.agents/domain.md`.
