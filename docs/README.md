# Documentation guide

Start here to find the right document. The [project README](../README.md) gives the
overview, status and quick start; everything else is in this folder.

## How the docs are organised

There are three kinds of document:

- **Operator docs** say what the code accepts and does: config keys, events, modes,
  errors. A key that is not documented here is rejected by the code. `tests/test_db_docs.py`
  fails if the db operator docs miss a key, mode, error or metric.
- **Design docs** (`api/design.md`, `db/design.md`, `db/engines.md`) explain why it is
  built this way, what else was considered, and the evidence. Read them before changing
  behaviour.
- **Background** (`background/`) is the earlier feasibility study. It is kept for
  history and is not maintained.

```
docs/
  README.md               this guide
  bronze-contract.md      what bronze looks like; what silver can rely on (both packages)
  deployment.md           running on Lambda (both packages)
  silver.md               the Spark / Glue silver transform
  testing.md              tests, config schemas, the db integration harness
  open-items.md           what has not been validated yet
  api/                    REST API sources (api_bronze)
  db/                     relational database sources (db_bronze)
  background/             feasibility study (history only)
```

## Start from what you want to do

| I want to... | Read |
|---|---|
| Understand what this project is | [../README.md](../README.md), then [bronze-contract.md](bronze-contract.md) |
| Onboard a new **REST API** source | [api/configuration.md](api/configuration.md) → [api/runs.md](api/runs.md) → [deployment.md](deployment.md) |
| Onboard a new **database** source | [db/configuration.md](db/configuration.md) → [db/modes.md](db/modes.md) → run `{"mode": "validate"}` ([db/preflight.md](db/preflight.md)) → [deployment.md](deployment.md) |
| Load a big table or a historical period | [db/modes.md](db/modes.md) (keyset, paged snapshots, backfills), [deployment.md](deployment.md#orchestration) |
| Deal with a failed run or an alarm | API: [api/broken-responses.md](api/broken-responses.md). Database: [db/errors.md](db/errors.md). Both: [deployment.md](deployment.md#alarms) |
| Re-run a period or reset a cursor | [deployment.md](deployment.md#operations) |
| Write or run a silver job | [bronze-contract.md](bronze-contract.md), then [silver.md](silver.md) |
| Know what type a database column lands as | [db/types.md](db/types.md) |
| Size a function's memory or timeout | [deployment.md](deployment.md#function-settings), [deployment.md](deployment.md#memory-sizing) |
| Deploy with Terraform | [../terraform/README.md](../terraform/README.md), with [deployment.md](deployment.md) |
| Run or add tests | [testing.md](testing.md) |
| Change api_bronze behaviour | [api/design.md](api/design.md) first |
| Change db_bronze behaviour | [db/design.md](db/design.md) and [db/engines.md](db/engines.md) first |
| Add Kerberos database auth | [db/kerberos.md](db/kerberos.md) |
| See what is still unproven | [open-items.md](open-items.md) |

## Every document

**Both packages**

| Document | Contents |
|---|---|
| [bronze-contract.md](bronze-contract.md) | S3 paths, completeness (`_dlt_loads`, manifests), append-only and at-least-once delivery, deduplication, state identity, API row schema, database row rules |
| [deployment.md](deployment.md) | Function settings, environment variables, the two images, memory sizing, orchestration, alarms, Terraform, day-to-day operations |
| [silver.md](silver.md) | The reference Spark transform for API bronze; what a database silver job must follow |
| [testing.md](testing.md) | Test commands, config JSON Schemas, what the tests cover, the docker-compose integration harness |
| [open-items.md](open-items.md) | Not yet validated: real S3 and Lambda, Glue, memory on real data, older database versions |

**REST API sources (`api_bronze`)**

| Document | Contents |
|---|---|
| [api/configuration.md](api/configuration.md) | Config reference: client, paginators, auth, secret references, resources, incremental cursors |
| [api/runs.md](api/runs.md) | Events and run modes (incremental, full, backfill), split loading, handler result |
| [api/broken-responses.md](api/broken-responses.md) | What happens to bad pages and bad records; quarantine; operating the `BrokenPageError` alarm |
| [api/design.md](api/design.md) | Why records are kept verbatim, options considered, design, cost, dlt gotchas, limitations, test coverage |

**Database sources (`db_bronze`)**

| Document | Contents |
|---|---|
| [db/configuration.md](db/configuration.md) | Config reference: connection, defaults, tables, cursors, chunking, partitions; default backend; static validation rules |
| [db/modes.md](db/modes.md) | Events and modes (incremental, full, backfill, validate, plan, finalize), time budget, stream / keyset / offset paging, partitioned reads, `lag` duplicates, NULL cursors |
| [db/preflight.md](db/preflight.md) | Checks run before every extraction and by `validate`; minimum server versions |
| [db/errors.md](db/errors.md) | Every Lambda `errorType` with its fix; EMF metrics; logs |
| [db/types.md](db/types.md) | The Parquet type each source type lands as, per engine and backend |
| [db/engines.md](db/engines.md) | ConnectorX, dlt and driver behaviour per engine: the research behind the rules |
| [db/design.md](db/design.md) | Requirement, scope, decisions D1–D12, runtime design, packaging, spikes S1–S9 |
| [db/kerberos.md](db/kerberos.md) | What Kerberos support would take (not implemented) |

**Background**

| Document | Contents |
|---|---|
| [background/dlt-lambda-ingestion-assessment.md](background/dlt-lambda-ingestion-assessment.md) | The original feasibility study of dlt on Lambda (history only) |

## Keeping the docs current

- Change the operator doc in the same change as the code. For db_bronze,
  `tests/test_db_docs.py` enforces this for `db/configuration.md`, `db/modes.md`,
  `db/preflight.md` and `db/errors.md`.
- Record a new design decision or finding in the package's `design.md` (or
  `db/engines.md`), labelled with its evidence: *researched*, *prototyped* or
  *integration*.
- Put a new document in this folder and add it to this guide. Keep the repo root to the
  project README.
