# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`api_bronze`: a dlt-based AWS Lambda that lands REST API records into an S3 bronze layer as Parquet, **one row per record, with the record's JSON text cut verbatim** from the response body (never parsed and re-serialised). A separate Spark/Glue job (`silver/silver_transform.py`) parses bronze into typed silver columns.

README.md is the operator-facing spec (bronze contract, config reference, run modes, deployment). DESIGN.md explains every design decision and the evidence behind it. Read DESIGN.md before changing behaviour. `dlt-lambda-ingestion-assessment.md` is the earlier feasibility study (background only).

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
mise run docker:lock                       # regenerate docker/requirements.lock from uv.lock
mise run docker:build                      # lock + image (-t tag, -p linux/arm64|linux/amd64)
mise run docker:smoke                      # build + invoke via Lambda Runtime Interface Emulator
mise run bench                             # split benchmark
```

`docker/requirements.lock` must be regenerated whenever dependencies change; the image installs with `--require-hashes` (`docker:build` does it automatically).

Tests need no network or AWS: `tests/conftest.py` runs an in-process fake REST API (`EventsAPI`, routable bytes overrides, 500s, auth checks) and uses a local directory as the bucket (`file://`). `tests/test_silver.py` imports `silver_transform` by inserting `silver/` on `sys.path`.

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

## Status

Unit/integration tested locally and image smoke-tested via the emulator. Not yet validated: real S3, real Lambda, Glue 6.0. `silver_transform.merge_into_iceberg` and `main` are untested sketches. See README "Open items".
