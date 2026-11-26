# Testing

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
| Config files against the JSON Schemas | `mise run config:validate examples/*.json` |
| Terraform (needs the registry, no AWS credentials) | `mise run tf` |


SQL Server needs Rosetta on arm64; Db2 runs on x86_64 only (see [db integration harness](#db-integration-harness)).

## Config schemas

`schemas/api_bronze.config.schema.json` and `schemas/db_bronze.config.schema.json`
(JSON Schema draft 2020-12) describe the two config documents for editors and CI.
`mise run config:validate <file>...` checks files against them, picking the schema by
the document's `client` (API) or `connection` (database) key. `config.py` stays
authoritative: the Lambda never reads the schemas. A config must not carry a
`"$schema"` key (unknown keys are rejected); map the schema to config files in the
editor instead (VS Code: `json.schemas` with `fileMatch`).

The db schema includes the rules that tie a table to its dialect and resolved backend
(ConnectorX support, isolation levels, Kerberos, `partition`, `max_time_s`). What JSON
Schema cannot express is checked only by `config.py` (and `validate` mode): unique
table/resource names, keyset `order_by` starting with the cursor and lying inside
`columns`, a real IANA `naive_timezone`, and calendar ranges inside timestamps.
`tests/test_config_schema.py` fails when a schema misses a dataclass field or enum
value, or when the schema and `config.py` disagree on a config (including every
combination of dialect, backend, isolation level, auth, `partition`, `max_time_s` and
chunking), so change the schema in the same change as `config.py`.
`tests/test_db_docs.py` fails when a db config key, event key, mode, `errorType` or
metric is missing from the db operator docs (`docs/db/configuration.md`, `modes.md`,
`preflight.md`, `errors.md`), so update them in the same change.

## What the tests cover

- api: the per-file table in [api/design.md](api/design.md#9-what-the-tests-prove).
- db: the layers below. The landed type matrix the integration suite pins is in
  [db/types.md](db/types.md).

| Layer | What | Where it runs |
|---|---|---|
| Unit | Config validation, default-backend resolution, URL building (SQLAlchemy and ConnectorX), query compilation for every dialect (offline `compile()`), keyset and paging SQL, the preflight rules against fake reflected metadata, `naive_timezone` conversion, error wrapping (including a synthetic `PanicException`), the backend-change guard. An end-to-end run against **SQLite** on both the `connectorx` and `pyarrow` backends: modes, state restore, lag, keyset, plan/page/finalize, manifests. | Anywhere. No network, no Docker. |
| Integration (`-m integration`) | The same scenarios against docker-compose: Postgres, MySQL 8.4, SQL Server 2022, Oracle Free, Db2 Community (x86 only). Type matrices per engine, including the deny-listed and high-scale decimal columns. Server-version floors. Spike measurements ([db/design.md](db/design.md#spikes)). | Dev machine or CI with Docker |
| Image | The integration scenarios invoked through the Lambda Runtime Interface Emulator inside the built `db-bronze` image (`mise run test:integration --image`; `docker:smoke --db` runs a SQLite smoke test), proving drivers, Instant Client and ODBC registration. | Dev machine or CI with Docker |

Kerberos is not tested ([db/design.md](db/design.md#decisions) D11). The db layers run through `test:integration`, plus `--db` flags on `docker:lock`, `docker:build` and `docker:smoke`.

## db integration harness

The integration harness lives in `tests/integration/`: `compose.yaml` (project
`db-bronze-it`; host ports and images overridable through `DB_BRONZE_IT_*`
variables, e.g. `DB_BRONZE_IT_PG_IMAGE=postgres:11` for a version floor run) and
a `conftest.py` that gives every test a fresh database per engine and runs the
handler into a `file://` bucket. The tests carry the `integration` marker, which
the default pytest run deselects, so `mise run test` never needs Docker.
`mise run test:integration` brings the engines up, runs the suite and takes them
down (`--keep` leaves them running). Later engines join by adding a service and
an `Engine` entry, plus their entry in the per-engine tables of the scenario
modules (the type matrix, `UNREADABLE`, `TRADITIONAL_STREAM_STOPS_EARLY`).
`COMPOSE_PROJECT_NAME` and the port variables run a second stack beside the first.
`DB_BRONZE_IT_ENGINES` (e.g. `mssql`) limits a run, and the services it brings up,
to some engines.

SQL Server (`mssql`, 2022 by default, `DB_BRONZE_IT_MSSQL_IMAGE` to change it) is
amd64 only. On an arm64 host it needs Rosetta (Docker Desktop, or colima with
`--vz-rosetta`): under QEMU user-mode emulation `sqlservr` refuses to start
("Invalid mapping of address ... in reserved address space") or segfaults. The
host side reaches it through pyodbc and ODBC Driver 18 (`msodbcsql18`, from
Microsoft's Homebrew tap on macOS), which loads OpenSSL 1.1 or 3 from Homebrew's
`openssl` prefix; where that now links OpenSSL 4, run pytest with
`DYLD_LIBRARY_PATH=$(brew --prefix openssl@3)/lib`. macOS strips `DYLD_*` variables
when it starts `/usr/bin/env` or `/bin/bash`, so they do not reach pytest through
`mise run test:integration`: bring the service up with `docker compose`, then run
`DB_BRONZE_IT_ENGINES=mssql DYLD_LIBRARY_PATH=... .venv/bin/python -m pytest -m integration`.

The handler-level scenarios of the SQLite suite run against every engine on both
backends (`test_it_modes.py`, `test_it_keyset.py`, `test_it_paged.py`,
`test_it_preflight.py`, `test_it_partition.py`): modes, `lag`, keyset and the
time budget, preflight and `validate`, the error types, paged snapshots and
partitioned reads. `mise run test:integration --image` builds the image and runs
the same suite inside it: the `run` fixture then invokes the handler through the
Runtime Interface Emulator on the compose network, with the buckets in a folder
mounted at the same path, and a failed invocation raises an exception named after
the Lambda `errorType`. Tests that only exercise the host (the spikes, the
ConnectorX ground truth of the type matrix) run unchanged. The session prints the
peak memory of the run. On an arm64 host the amd64 image runs under QEMU, where the
emulator (a Go binary) crashes every 20 to 30 invocations; the harness mounts the
arm64 emulator from the arm64 variant of the same AWS base image instead, which
runs natively, so only the handler itself is emulated.

**Oracle** is the compose service `oracle` (Oracle AI Database Free
23.26, the full image: `-lite` has no XDB for XMLTYPE and no USERS tablespace),
in the compose profile `oracle`, so host runs do not start it. Each test gets a
fresh user, i.e. a schema, on `FREEPDB1`. Its handler needs thick-mode oracledb
and Instant Client ([db/design.md](db/design.md#decisions) D10), so on the host its tests skip; seeding works from the
host in thin mode. `mise run test:integration --image` runs the Oracle scenarios
through the emulator. `mise run test:integration --inside` runs pytest itself
inside the image on the compose network (`DB_BRONZE_IT_NETWORK=compose`), so the
handler and ConnectorX run in-process with Instant Client: that is what the
spike tests calling ConnectorX directly need on Oracle. `--engines oracle` sets
`DB_BRONZE_IT_ENGINES`.

**Db2** is the compose service `db2` (Db2 Community 12.1.5,
`DB_BRONZE_IT_DB2_IMAGE` to change it), in the compose profile `db2`, privileged,
x86_64 only. It creates database `BRONZE` on first start (about two minutes under
Rosetta), and each test gets a fresh schema in it, made current with the
`CurrentSchema` driver option. Everything a Db2 test does, seeding included, goes
through ibm_db, which has x86_64 Linux wheels only, so on any other host the Db2
tests skip with that reason, in image mode too. `mise run test:integration --inside
--engines db2` runs them inside the image; on an x86_64 Linux host they run with
every run. ConnectorX does not read Db2 (the config rejects the combination), so
every test that drives ConnectorX skips on Db2 (the `connectorx` marker, or the
`backend` parameter), and Db2 runs the rest on pyarrow. Under colima's Rosetta the
image's setup fails to create the database: binfmt is registered there without
preserve-argv0, so `su - db2inst1` is not a login shell and never reads the
instance profile (`db2start: command not found`). The compose file mounts an
`/etc/environment` with `BASH_ENV` pointing at the profile, which su applies; on an
x86_64 host it changes nothing. The image's fenced user cannot read the instance
directory there either (SQL1646N), so the harness drops a test's schema object by
object rather than through `SYSPROC.ADMIN_DROP_SCHEMA`. The 12.1.5 setup's own `CREATE DATABASE` can
also segfault under Rosetta (seen once at review, 25 Nov 2026) while the log still says
"Setup has completed"; the healthcheck then never passes. Running
`su - db2inst1 -c '. ~/sqllib/db2profile; db2 create db BRONZE'` in the container
creates it, and the waiting run carries on.
