# Design: relational database bronze with dlt on Lambda (`db_bronze`)

**Status:** implemented and tested (25 November 2026). The operator spec is
[configuration.md](configuration.md), [modes.md](modes.md), [preflight.md](preflight.md),
[errors.md](errors.md), [types.md](types.md) and [../bronze-contract.md](../bronze-contract.md);
the engine research is [engines.md](engines.md). Spikes S1, S4 and S5 were answered by a throwaway SQLite prototype on
24 November 2026 and folded in below. S1 (server side), S2, S3, S6, S7 and S9 were answered
by the integration suite (`tests/integration/`) on 24 November 2026: the full scenario suite
ran on Postgres and MySQL (host and image), then SQL Server, Oracle (inside the image) and
Db2, settling each engine's deny list.

*Verified:* the unit suite (SQLite, both backends) and the integration suite on Postgres
11/12/17, MySQL 8.0/8.4, SQL Server 2017/2019/2022, Oracle Free 23.26 and Db2 11.5/12.1,
on the host and through the Lambda emulator in the image. After the review fixes (25 Nov
2026) the suite was rerun on Postgres 17, MySQL 8.4 and SQL Server 2022 on the host, and
on Oracle Free 23.26 and Db2 12.1 inside the image. *Not verified:* Oracle 19c/21c
(accepted by the preflight minimum, Oracle 19, by decision, but never run, S9) and Db2
11.1/10.5 (the Db2 minimum is a floor from evidence, S9); peak memory on a
real large table (S7); real S3, real Lambda in a VPC, Glue 6.0; Kerberos (placeholder).

This is the database counterpart of `api_bronze` (see [../api/design.md](../api/design.md)). It records
the requirement, the facts that constrain the design, every decision taken and
why, and the spikes ([Spikes](#spikes)) with their answers.

## Evidence labels

- *researched*: read from library source or vendor documentation at the
  versions named, not yet executed in this repo.
- *prototyped*: executed in a throwaway prototype on SQLite (dlt 1.32.0,
  connectorx 0.4.6), not yet covered by this repo's tests.
- *integration*: measured against a real engine by the integration suite
  (`tests/integration/`), and pinned there by a test.

Items still unverified are listed under *Not verified* above and in [../open-items.md](../open-items.md).


---

## Requirement

- Ingest tables from relational databases on **AWS Lambda** (container image)
  using **dlt 1.32.0**, into the same S3 **bronze** layer and path convention
  as `api_bronze`.
- Engines that must be covered: **Microsoft SQL Server, MySQL, Db2, Oracle,
  PostgreSQL**. SQLite is supported for tests. Other SQLAlchemy dialects
  work through the traditional backend without special handling.
- Use **ConnectorX** (Arrow streaming) where the engine supports it, and the
  traditional SQLAlchemy path where it does not.
- Production will need **Kerberos**. v1 only reserves a place for it and
  documents how to add it ([kerberos.md](kerberos.md)).

## Scope

**In v1**

- Package `src/db_bronze/`, image `docker/Dockerfile.db`, lock
  `docker/requirements-db.lock`.
- Modes `incremental`, `full`, `backfill`, plus the orchestration events
  `plan`, `finalize` and `validate`.
- Config validation and a reflection-based preflight.
- Keyset chunking, offset paging for large snapshots, opt-in ConnectorX
  partitioned reads.
- Unit tests on SQLite, docker-compose integration tests for the five engines,
  and the same tests run inside the built image.
- An example Step Function definition (ASL) under `examples/`.

**Out of v1**

- Implementing Kerberos: placeholder and documentation only.
- MSSQL `rowversion` cursors.
- Db2 for z/OS and IBM i: these need a Db2 Connect licence file. Only Db2 for
  LUW is targeted.
- A database silver transform. Only the bronze contract is documented.
- Terraform was out of v1; modules and examples have since been added under
  `terraform/` (see [terraform/README.md](../../terraform/README.md)).

---

## Decisions

| # | Decision | Rejected alternative and why |
|---|---|---|
| D1 | Sibling package `src/db_bronze/` in this repo. Code that is the same in both packages and touches **no dlt internals** lives in `src/bronze_common/` (revised at review, 25 Nov 2026): strict config building (`config.strict_builder`, each package passing its own `ConfigError`), secret lookup and the Secrets Manager cache (`secrets`), and the handler's config loading, Lambda time budget (`SAFETY_MARGIN_S`, `MIN_BUDGET_S`) and error unwrapping (`handler`). Both images ship it. Everything dlt-facing (resources, incremental, destination layout) stays in each package. | A shared module that also held dlt-facing code would tie both packages to the same dlt internals (D1 as first written, which kept no shared module until it was clear what they really share). Copying the shared helpers, as before the review, let the two copies drift. |
| D2 | Bronze is **typed Arrow**: the types the backend returns, the source column names, dlt's normaliser bypassed. | Letting dlt normalise renames columns and re-coerces types. Casting everything to string pushes all typing into silver. |
| D3 | The backend is a **config parameter**. The default comes from the config alone: ConnectorX where the engine supports it, pyarrow otherwise. Opting out is a config change. | Automatic runtime fallback would land the same table with different types from one run to the next. A canonical cast layer would duplicate type logic. |
| D4 | **No runtime fallback.** A ConnectorX failure fails the run with instructions for switching backend. | Falling back silently changes types (D3). |
| D5 | ConnectorX compatibility rules are **validation**: static checks when the config loads, column-level checks in a preflight at the start of every run. | Checking only at runtime cannot catch silent decimal truncation. |
| D6 | The backend each table resolves to is stored in state. A change since the last successful run **fails before extraction** with `BackendChanged`, explaining that bronze types would change and that the fix is to land under a new table `name` (a new bronze prefix; silver unions old and new). Revised 24 Nov 2026 after S5. | WARN and proceed (the original D6): the freeze contract fails the run anyway, because the backends produce different decimal types, and the operator gets a confusing `DataValidationError`. Evolving types for one run: one bronze folder would hold mixed Parquet types that Spark schema merge cannot reconcile. |
| D7 | Opt-in **keyset chunking** over indexed columns for large tables. | A single long query holds locks or snapshots for its whole duration (ORA-01555, MSSQL blocking). |
| D8 | Large snapshot tables are paged with **ORDER BY + OFFSET by a Step Function**, using `plan` and `finalize` events. | Limiting v1 to tables that fit in one invocation. |
| D9 | One **x86_64** image with every driver, about 1.2–1.5 GB. | Splitting by architecture: Db2 has no aarch64 wheel. Splitting by engine: least privilege is enforced per function through IAM, not by the image. |
| D10 | Oracle always runs in **thick** mode. | Mixing thin and thick: thick mode is process-wide once enabled, and Instant Client is in the image anyway for ConnectorX. |
| D11 | Kerberos: placeholder slot plus [kerberos.md](kerberos.md) only. | Building it now, before any project needs it. |
| D12 | `_extracted_at` is the only added column. The backend used appears in logs and metrics only. | A `_backend` column. Source db and schema columns, which are constant per config. |

### Deliberate deviations (recorded at review, 25 Nov 2026)

Each of these goes beyond, or against, what this document first specified. All are kept
on purpose, and each rests on evidence from the integration suite or a review decision.

| Deviation | Why | Evidence |
|---|---|---|
| `max_time_s` is rejected for a **MySQL** `stream` table on the traditional backend, not only for ConnectorX streams ([configuration.md](configuration.md#static-validation-rules)). | Closing PyMySQL's unbuffered cursor reads the rest of the result first, so a cut stream blocks until its query has run to the end, exactly as a dropped ConnectorX stream does. | S1, integration (MySQL 8.0/8.4); [Chunking](#chunking-d7). |
| `sql_variant` is a preflight finding on **every** backend, not only ConnectorX ([preflight.md](preflight.md)). | pyodbc reads a `sql_variant` only as the bytes of its value, without its type, so no backend lands it losslessly. | Integration (SQL Server 2017/2019/2022); [engines.md](engines.md#traditional-drivers). |
| **Db2 and Oracle type overrides** in reflection and extraction: Db2 DECFLOAT as exact text, BINARY/VARBINARY and CHAR/VARCHAR FOR BIT DATA as binary, DOUBLE as double, native Decimals and CURRENT SCHEMA as the default schema (`extract.prepare_engine`, `source.reflect`); Oracle NUMBER without precision read as Decimal, TIMESTAMP WITH TIME ZONE read through `SYS_EXTRACT_UTC`. | Without them those types land wrongly or lose digits silently: ibm_db_sa reflects them wrongly or not at all, and Oracle's driver hands NUMBER over as float and drops the offset of TIMESTAMP WITH TIME ZONE. | Integration type matrix, Oracle and Db2 ([types.md](types.md)); [engines.md](engines.md#traditional-drivers). |
| `tests/test_db_docs.py` (first `tests/test_db_readme.py`): a test that the db operator docs (`docs/db/`) document every config key, event key, mode, enumerated value, error type and metric, and the server minimums as the code has them. | Not in the test plan ([../testing.md](../testing.md)). It keeps the operator spec from falling behind the code. | Review (25 Nov 2026). |
| `finalize` rejects `"tables"` ([Orchestration](#orchestration-d8)). | It names its one table in `table`; a `tables` list beside it could only repeat or contradict it. | Review. |
| `backend: sqlalchemy` is rejected when the config loads ([configuration.md](configuration.md#static-validation-rules); [Backends](#backends)). | It is not built; failing at config load beats failing mid-run with `NotImplementedError`. | Review (25 Nov 2026). |
| The Oracle preflight minimum is **19**, below the only version the suite ran on (23). | 19c is Oracle's long-term release; refusing it would block every 19c source. 19c and 21c stay **unverified** (S9). | Review (25 Nov 2026). |

---

## Validation (D5)

ConnectorX compatibility rules are validation, in two stages, both failing before
anything is extracted:

- **When the config loads (static):** every rule fails with `ConfigError` and a message
  that says how to fix it. The list is [configuration.md](configuration.md#static-validation-rules).
  The rules with an engine behind them: `isolation_level` beside ConnectorX is accepted
  only where ConnectorX can apply it (Postgres, and SQL Server without `partition`; S3);
  `max_time_s` is rejected beside any stream that cannot stop early (ConnectorX `stream`,
  and MySQL `stream` on the traditional backend; S1); `partition` is rejected in
  `incremental` mode because partition batches interleave and break ascending row order;
  `backend: sqlalchemy` is rejected because it is designed but not built.
- **Preflight at the start of every run:** reflection-based column checks and server
  minimums ([preflight.md](preflight.md)). This is the only thing standing between a
  ConnectorX table and silent decimal truncation, which a runtime check cannot catch.
  The same checks run, without extracting, as `{"mode": "validate"}`.


---

## Runtime design

### Request flow

`handler.handler` → `config.SourceConfig` / `RunRequest` → `pipeline.run_pipeline` →
`preflight.preflight` → `source.make_resource` (one dlt resource per table)
→ `extract.<backend>` → Arrow batches with `_extracted_at` appended.

Each table becomes a custom `@dlt.resource` rather than dlt's `sql_table`. Three
things need it: keyset chunking and offset paging control the query loop,
preflight owns reflection, and ConnectorX errors have to be wrapped. Two rules
carry over from `api_bronze`:

- `dlt.sources.incremental(...)` **must be the resource argument's default value**. Building it in the function body silently disables state and filtering.
- `write_disposition="append"`, hard-coded. `replace` deletes sibling `mode=` partitions.

Every per-engine difference is recorded in one table, `dialects.py` (review, 25 Nov
2026): the config's name for the engine (`postgres`, `db2`), SQLAlchemy's
(`postgresql`, `ibm_db_sa`), the URL driver, the connection keys that locate the
database, the ConnectorX scheme and what ConnectorX can apply there, and each
quirk the code works around (text timestamps on SQLite, cast inlined timestamps on
SQL Server, a text server version on Db2, and so on). Code asks the entry for a
property instead of comparing dialect names; per-engine hooks (`extract.prepare_engine`,
the URL option translation, Db2's reflection fix-up) are dispatch tables keyed by
the entry's name. A dialect name with no entry gets a generic one: the traditional
backend only.

Queries are built with SQLAlchemy Core, so dialect differences (quoting,
`LIMIT` vs `FETCH FIRST`/`OFFSET ... FETCH`, `TOP`) come from the dialect, not
from string templates. For ConnectorX the statement is compiled to a string with
`literal_binds`, as dlt does. Whether each engine's inlined literals select exactly
what the bound statement does (S2), and the `CAST(... AS DATETIME2)` workaround for
SQL Server, is in [engines.md](engines.md#inlined-literals-s2).

### Backends

| Backend | Extraction |
|---|---|
| `connectorx` | `cx.read_sql(url, sql, return_type="arrow_stream", batch_size=...)`. Each `RecordBatch` is yielded. `RuntimeError` and `pyo3_runtime.PanicException` are caught **narrowly around the ConnectorX call only** and re-raised as `ConnectorXFailed(table, reason)`. |
| `pyarrow` | SQLAlchemy streaming cursor (`stream_results` / `yield_per`), with rows converted to Arrow per batch using the reflected column types. |
| `sqlalchemy` | *Designed, not built.* The same query, rows yielded as Arrow without type hints, for odd dialects where the pyarrow path cannot infer a type. The config rejects it with `ConfigError` until it is built (review decision, 25 Nov 2026). |

Bronze uses dlt's `direct` naming convention, so source column names survive
(*prototyped*, S5). The schema contract is
`{"tables": "evolve", "columns": "evolve", "data_type": "freeze"}`: a new source
column lands, and a type change on an existing column fails the run
(*prototyped*, S5). The filesystem destination sets
`warn_unsafe_layout_separators=False`: dlt warns that `direct` allows `/` in
table names, but bronze names are validated lowercase identifiers.

How each engine's driver values land on the `pyarrow` backend (text for what Arrow
cannot take, the Oracle and Db2 overrides, the `timezone: false` hint for naive
timestamps) is in [types.md](types.md).

### Cursor handling

- Cursor state holds values **as the source represents them** (naive timestamps stay naive). Conversion through `naive_timezone` happens only at the boundaries: config `initial_value`, event window bounds, and `plan` output.
- The WHERE clause is `cursor >= :start [AND cursor < :end]` (half-open, as in `api_bronze`), ANDed with `where`. With `include_null_cursor` it becomes `(... OR cursor IS NULL)`.
- The query's lower bound is `cursor.start_value`, which dlt already steps back by `lag`.
- Without `lag`, dlt deduplicates rows at the boundary by `primary_key`. With `lag`, dlt deduplicates nothing, so each run re-lands every row in the lag window (*prototyped*, S4). Bronze accepts these exact re-reads, and silver removes them ([../bronze-contract.md](../bronze-contract.md)). Deduplicating ourselves was rejected because the state would grow with the number of rows in the lag window.
- Incremental reads are ordered by the cursor (`row_order="asc"`), which is what makes split loading safe.

### Chunking (D7)

- **`stream`**: one query, `ORDER BY cursor`, batches streamed.
- **`keyset`**: repeated short queries, each an index range scan:

  ```sql
  SELECT <cols> FROM t
  WHERE <cursor window> AND <where>
    AND (c1 > :v1 OR (c1 = :v1 AND c2 > :v2))   -- expanded form: Oracle and MSSQL
  ORDER BY c1, c2                               -- have no row-value comparison
  FETCH FIRST :page_size ROWS ONLY              -- dialect-specific, via SQLAlchemy
  ```

  The last row of each page seeds the next. Ties on the cursor are broken by the primary key inside one run, and by dlt's boundary deduplication across runs. Whether the `order_by` columns are indexed cannot be checked portably; [modes.md](modes.md#chunking-stream-keyset-or-offset-paging) tells the operator to confirm it with the DBA.

- **Time budget** (incremental only). Keyset checks the deadline **itself, between pages**, and never closes a page mid-read, so no query is left running. `add_limit(max_time=...)` is not used for keyset, because it closes the generator mid-page.
  - ConnectorX `stream` tables are **never split-loaded**. Dropping their reader returns at once, but the query runs on to the end in a detached thread with the connection open (*prototyped*, S1). On Lambda the frozen container keeps that DB session open.
  - Validation rejects `max_time_s` alongside such a table ([configuration.md](configuration.md#static-validation-rules)). The Lambda-derived budget skips such tables, and that is logged. A ConnectorX table that does not fit one invocation must use `keyset`.
  - The traditional backends' `stream` keeps `add_limit(max_time=...)`, as in `api_bronze`, where closing the stream ends the query. *Integration (S1):* on **Postgres** closing psycopg's server-side cursor ends the query at once. On **MySQL** closing PyMySQL's unbuffered cursor reads the rest of the result first, so the close blocks until the query has run to the end: a MySQL `stream` table is treated like a ConnectorX one (never split-loaded; validation rejects `max_time_s`, and the Lambda-derived budget skips it). On **SQL Server** closing pyodbc's cursor ends the query at once, so a traditional `stream` table is split-loaded as on Postgres. On **Oracle** closing oracledb's cursor ends the query at once, likewise.

### Orchestration (D8)

The events and modes are specified in [modes.md](modes.md).

Every event except `finalize` also accepts `"tables": [...]` to run a subset. **Deviation (review, 25 Nov 2026):** `finalize` rejects `tables`. It writes the manifest of exactly one table, named by `table`; a `tables` list beside it could only repeat `table` or contradict it, so it is treated like any other key that belongs to another kind of event (a mistyped event fails instead of being ignored).

**Offset paging caveats.** OFFSET paging is not
consistent across invocations: rows that change while a snapshot is running
can be duplicated or missed, and deep offsets get slower. Prefer keyset
wherever a unique sortable key exists. Use offset paging only when the table
has no usable key, or when its key is not indexed.

**Parallelism.** Paged snapshots and parallel backfill windows run under a
**separate function and pipeline name** (e.g. `core_banking_backfill`). That is
the `api_bronze` rule: these runs still write dlt state rows, so they must
never share a pipeline name with a concurrently running incremental.

An example ASL (`examples/db_snapshot.asl.json`) shows the snapshot flow:
`plan` → `Map` over pages (with a concurrency limit) → `finalize` with the
collected `load_ids`.

### Failures and observability

The error types, metrics and logs are specified in [errors.md](errors.md). Every
handler-visible error derives from `DbBronzeError` in `db_bronze.errors` (including the
one `PreflightError`), so each emits its metric and the structured error log. The handler
unwraps them from dlt's `PipelineStepFailed`, as `api_bronze` already does for
`BrokenPageError`.

**Backend change guard (D6).** The resolved backend is written to each
resource's dlt state. When it differs from the previous successful run, the
`BackendChanged` metric is emitted and the run fails with `BackendChanged`
before anything is extracted. A table landing under a new `name` has no
previous backend, so the guard passes.

### State

Identical to `api_bronze`:

- `pipe.drop()` on every invocation, so state is always restored from the destination.
- One concurrent run per pipeline name.
- Every run starts from the newest state attached to a completed load.

### Memory

ConnectorX's stream channel is unbounded, so `batch_size` and Lambda memory are
the only controls. Memory is sized per function from a measured validation run
(peak RSS on the largest table). No custom backpressure is built.

---

## Kerberos (placeholder only, D11)

Not implemented. What a project would need is in [kerberos.md](kerberos.md); the
`docker/Dockerfile.db` carries a commented-out block for it.

---

## Packaging

- `pyproject.toml` gains an optional extra `db`:
  - `connectorx>=0.4.6`
  - `sqlalchemy`
  - `pyodbc`
  - `oracledb`
  - `psycopg[binary]`
  - `PyMySQL`
  - `ibm_db` and `ibm_db_sa`, both with the marker `sys_platform == 'linux' and platform_machine == 'x86_64'`, so `uv sync` still works on a Mac.
- `mise run docker:lock --db` exports `docker/requirements-db.lock`. The API image stays free of database dependencies.
- `docker/Dockerfile.db`, based on `public.ecr.aws/lambda/python:3.12` (Amazon Linux 2023), `--platform linux/amd64` only:
  - `unixODBC` and `msodbcsql18` (`ACCEPT_EULA=Y`) from the RHEL 9 repo. The repo URL is the build argument `MSSQL_REPO_URL`.
  - Oracle Instant Client **Basic** (not Basic Lite, which supports fewer character sets) from `ORACLE_IC_URL`, checked against `ORACLE_IC_SHA256`, unpacked under `/opt/oracle/instantclient`. It also needs `libaio` (works on AL2023, S6). The directory is on `LD_LIBRARY_PATH` for ConnectorX and also registered with `ldconfig`, in case the Lambda service sets its own `LD_LIBRARY_PATH`. `ORACLE_CLIENT_LIB_DIR` points at it, and `db_bronze.handler` calls `oracledb.init_oracle_client(lib_dir=...)` at import when it is set: a broken Instant Client fails the cold start rather than falling back to thin mode. Unset (local runs, unit tests), oracledb stays thin.
  - `pam`: ibm_db's bundled clidriver links `libpam.so.0`, which the Lambda base image does not ship (found by the smoke test).
  - Python dependencies from the lock with `--require-hashes` and `--only-binary :all:` (every pinned package has an x86_64 wheel; ibm_db's is `manylinux_2_34`, which AL2023's glibc 2.34 just meets).
  - The same ENV as the API image (`DLT_DATA_DIR`, `DLT_PROJECT_DIR`, telemetry off).
  - `CMD ["db_bronze.handler.handler"]`.
- Build arguments exist so a regulated pipeline can point at internal mirrors without editing the Dockerfile.
- Db2 for z/OS or IBM i needs a Db2 Connect licence file copied into `clidriver/license/`. Documented, not done.

---

## Spikes

| # | Question | If the answer is bad |
|---|---|---|
| S1 | Does dropping a ConnectorX `arrow_stream` reader mid-query block until the query completes? | **Answered (prototype, SQLite):** no block, but the producer detaches and reads to the end with the connection open. Split loading requires `keyset` for ConnectorX tables ([Chunking](#chunking-d7)). **Server side (integration, Postgres and MySQL):** after the drop the query is still active in `pg_stat_activity` and in the MySQL processlist. Closing the traditional backend's streaming cursor ends the query at once on Postgres, but on MySQL the close reads the rest of the result and blocks until the query ends, so MySQL `stream` tables are not split-loaded either ([configuration.md](configuration.md#static-validation-rules); [Chunking](#chunking-d7)). **SQL Server (integration):** the dropped ConnectorX query is still in `sys.dm_exec_requests`; closing pyodbc's cursor ends the query at once, so traditional `stream` tables are split-loaded there. **Oracle (integration):** ConnectorX reads the whole result before the first batch, so after a drop nothing runs; closing oracledb's cursor ends the query at once. **Db2 (integration):** ConnectorX does not apply; closing ibm_db's cursor ends the query at once (it is gone from `MON_GET_ACTIVITY`), so traditional `stream` tables are split-loaded there. |
| S2 | Does `literal_binds` compile datetime and decimal literals correctly for mssql, oracle and db2? | **Answered for Postgres and MySQL (integration):** yes; microsecond, time-zone-aware, date and wide-decimal literals select the same rows as bound parameters ([Request flow](#request-flow), [engines.md](engines.md#inlined-literals-s2)). **SQL Server (integration): no** for `datetime` and `smalldatetime` (a six-decimal text literal fails to convert), so cursor bounds are inlined as `CAST(... AS DATETIME2)` (`DATETIMEOFFSET` with an offset), which selects exactly what the bound query does on every SQL Server timestamp type; decimal and date literals are fine ([Request flow](#request-flow), [engines.md](engines.md#inlined-literals-s2)). **Oracle (integration):** yes for timestamps (to the microsecond), dates and wide decimals, directly and through a handler backfill window on both backends. A time-zone-aware literal fails (`ORA-01830`), but ConnectorX cannot read TIMESTAMP WITH TIME ZONE anyway, and the pyarrow backend binds it ([Request flow](#request-flow), [engines.md](engines.md#inlined-literals-s2)). **Db2 (integration):** moot, since Db2 never reaches ConnectorX and its queries are bound; inlined anyway, microsecond timestamps and 28-digit decimals select exactly what the bound cursor and keyset queries do. Measuring it found that the *bound* side was wrong: ibm_db_sa sent decimals through a float, so a decimal keyset seed lost digits, and so did every DECIMAL value read. Db2 engines now use native Decimals ([engines.md](engines.md#traditional-drivers); [Request flow](#request-flow), [engines.md](engines.md#inlined-literals-s2)). |
| S3 | Can ConnectorX apply `isolation_level` or session settings for each engine? | **Answered for Postgres and MySQL (integration):** Postgres yes, through libpq `options` in the ConnectorX URL, so validation accepts `isolation_level` with `connectorx` there. MySQL no, so it stays rejected ([engines.md](engines.md#connectorx-046); [engines.md](engines.md#connection-settings-per-engine); [configuration.md](configuration.md#static-validation-rules)). **SQL Server (integration):** yes, as a `SET TRANSACTION ISOLATION LEVEL` ahead of the query in the same batch (its URL ignores unknown parameters), except on `partition` reads, which ConnectorX parses as a single SELECT; validation accepts the combination without `partition`. **Oracle (integration):** no. The ConnectorX URL ignores query parameters, multi-statement SQL is rejected and no hint sets the level, so `isolation_level` with `connectorx` stays rejected; the traditional backend applies it. |
| S4 | Do dlt incremental `lag` and boundary deduplication by `primary_key` work with Arrow items from a custom resource? | **Answered (prototype):** deduplication works without `lag`. With `lag`, dlt deduplicates nothing, so the lag window is re-landed on every run. Decision: bronze accepts it and silver removes the duplicates ([Cursor handling](#cursor-handling); [../bronze-contract.md](../bronze-contract.md)). |
| S5 | Do `direct` naming and `data_type: freeze` apply to Arrow items as expected? | **Answered (prototype):** yes, and freeze also catches decimal precision changes, which led to revising D6. Naive timestamps need `timezone: false` hints ([Backends](#backends)). |
| S6 | Do Instant Client (Basic) and `libaio` work on AL2023? Can thick-mode oracledb and ConnectorX share `libclntsh` in one process? | Pin an Instant Client version known to work; isolate Oracle in its own image. **First part answered (image smoke test, 24 Nov 2026, linux/amd64 under emulation):** Instant Client Basic 23.26.4.0.0 with `libaio-0.3.111` (AL2023 package) loads. In one process, oracledb 26.1.0 enters thick mode (client 23.26.4) and ConnectorX 0.4.6 then loads `libclntsh` too: an Oracle connection to a closed port fails with `ORA-12541` from OCI, not `DPI-1047`. Same image: unixODBC 2.3.9 (AL2023) with msodbcsql18 18.7.1.1 from the RHEL 9 repo, listed by `pyodbc.drivers()`; ibm_db 3.3.0 imports once `pam` is installed. **Second part answered (integration):** yes. Against Oracle Free 23.26, one invocation lands one table through thick-mode oracledb and another through ConnectorX in the same process, inside the image under the emulator, and the whole Oracle suite runs both backends in one pytest process inside the image. |
| S7 | What is peak memory for the largest real table under ConnectorX streaming? | Smaller `batch_size`, keyset chunking, or more Lambda memory. **First input (integration, 24 Nov 2026):** the whole Postgres and MySQL scenario suite (tables of tens of rows, so this is the fixed cost of a run, not the data) peaks at **588 MiB** for the image container across the session (cgroup `memory.peak`: emulator plus the warm Python runtime, amd64 under QEMU, where jemalloc cannot return memory, so an overestimate) and **254 MiB** RSS for the host pytest process running the handler in-process. A 1 GB function has room for the fixed cost; the per-table figure still needs a run on the largest real table. The SQL Server suite peaks at **318 MiB** for the image container, amd64 under Rosetta rather than QEMU, and 124 MiB RSS for the host process. |
| S8 | Is egress to packages.microsoft.com and the Oracle download host allowed from the build pipeline? | Internal mirrors through the build arguments. |
| S9 | What are the per-dialect minimum server versions? | **Answered for Postgres and MySQL (integration):** the whole integration suite passes on Postgres 11.16, 12.22 and 17.12, and on MySQL 8.0.46 and 8.4.13. Preflight minimums: Postgres 11, MySQL 8.0. Older series were not run (Postgres 10 and MySQL 5.7 are long out of vendor support). **SQL Server (integration):** the whole suite, both backends, passes on 2017 (14.0.3570.2, CU 31), 2019 (15.0.4510.2) and 2022 (16.0.4325.4, CU 28), on the host, and on 2022 inside the image as well. Preflight minimum: SQL Server 2017 (version 14); SQL Server on Linux, and so the compose stack, starts there. **Oracle (integration):** the whole suite passes on Oracle Free 23.26 (`23.26.0.0`), which first set the preflight minimum to Oracle 23. **Review decision (25 Nov 2026):** the minimum is lowered to **Oracle 19**, so 19c (the long-term release) and 21c are accepted, but they are **unverified**: no integration run has proven them. The older series exist only as x86_64 images (`gvenzl/oracle-xe:21-slim-faststart`, `:18-slim-faststart`), and on this arm64 host their PMON does not start under Rosetta (`ORA-00443`). **Still open:** run `DB_BRONZE_IT_ORACLE_IMAGE=gvenzl/oracle-xe:21-slim-faststart DB_BRONZE_IT_ORACLE_SERVICE=XEPDB1 mise run test:integration --inside --engines oracle` (and `:18`) on an x86_64 Docker host, and raise the minimum if they fail; the matrix leaves out the 23ai BOOLEAN and JSON below 23. 19c, the long-term release, has no public image. **Db2 (integration):** the whole Db2 suite (pyarrow) passes inside the image on Db2 Community 12.1.5.0 and on 11.5.5.1, the oldest Community image published (`icr.io/db2_community/db2` has 11.5.5.1 to 12.1.5.0), so the preflight minimum is **Db2 11.5**. Older series (11.1, 10.5) have no Community image and were not run. To repeat the 11.5 run: `DB_BRONZE_IT_DB2_IMAGE=icr.io/db2_community/db2:11.5.5.1 mise run test:integration --inside --engines db2`; under colima's Rosetta the RHEL 7 based 11.5 image does not create the database by itself (its su skips pam_env, so the compose fix does not reach it), so once its log says "Setup has completed" run `su - db2inst1 -c '. ~/sqllib/db2profile; db2start; db2 create db BRONZE'` in the container. |

Still not validated, as for `api_bronze`: real S3, real Lambda in a VPC
against the production databases, and Glue 6.0 reading typed bronze.

---

## Implementation order

1. Config, validation and URL building, with unit tests.
2. Query builder (stream, keyset, paging) with offline compile tests for every dialect.
3. Resource and backends; end-to-end on SQLite for both backends; modes and state.
4. Preflight and `validate`; the backend-change guard; error types and handler unwrapping.
5. `plan`, `finalize` and manifests; the example ASL.
6. `Dockerfile.db`, lock and mise tasks; the image smoke test.
7. The docker-compose integration suite; run spikes S1–S9 and fold the results back into this document.
8. Operator spec (now `docs/db/`), `docs/db/kerberos.md`, `CLAUDE.md` update.
