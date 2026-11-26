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

## Documentation

This README is the entry point; everything else lives under [docs/](docs/). Start with
the **[documentation guide](docs/README.md)**: it maps tasks (onboarding a source,
handling a failed run, writing silver, changing behaviour) to the right document.

The most used documents:

- [docs/bronze-contract.md](docs/bronze-contract.md): what bronze looks like and what silver can rely on
- API sources: [configuration](docs/api/configuration.md), [run modes](docs/api/runs.md), [broken responses](docs/api/broken-responses.md), [design](docs/api/design.md)
- Database sources: [configuration](docs/db/configuration.md), [modes](docs/db/modes.md), [preflight](docs/db/preflight.md), [errors](docs/db/errors.md), [types](docs/db/types.md), [design](docs/db/design.md)
- [docs/deployment.md](docs/deployment.md), [docs/testing.md](docs/testing.md), [docs/open-items.md](docs/open-items.md)

## Status

| Part | State |
|---|---|
| `src/api_bronze` (library + Lambda handler) | Tested against an in-process fake API, local filesystem as the bucket |
| `src/db_bronze` (library + Lambda handler) | Tested end to end on SQLite on both backends; integration suite passed on Postgres, MySQL, SQL Server, Oracle and Db2 in docker-compose, in-process and through the image (exceptions in [open items](docs/open-items.md)) |
| `docker/Dockerfile.api`, `docker/Dockerfile.db` | Built and smoke-tested through the Lambda Runtime Interface Emulator (`scripts/smoke_image.py`, `scripts/smoke_db_image.py`) |
| `silver/silver_transform.py`: read, completeness filter, parse, quarantine split, rescue, dedup | Tested locally with pyspark 4.1.1 on bronze written by `api_bronze` |
| `silver/silver_transform.py`: `merge_into_iceberg`, `main` | **Untested sketch**: no Iceberg catalog was available |
| `terraform/` | Planned against a mocked AWS provider only; never applied to a real account |
| Kerberos database auth | **Placeholder**: parses, fails at run start ([docs/db/kerberos.md](docs/db/kerberos.md)) |
| Real S3, real Lambda, Glue 6.0, Iceberg write, memory on real data | **Not yet validated**: see [open items](docs/open-items.md) |

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
schemas/          JSON Schemas for both config documents (editor and CI validation)
terraform/        deployment modules + examples (platform, api and db sources)
docs/             all other documentation; start at docs/README.md
.mise/tasks/      mise tasks (bash): setup, lint, check, bench, config/*, test/*, docker/*, tf/*, dev/*
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
mise run test:integration       # db_bronze against docker-compose engines (see docs/testing.md)

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
