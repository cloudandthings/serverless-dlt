# Design: relational database bronze with dlt on Lambda (`db_bronze`)

**Status:** implemented and tested (25 November 2026); operator spec in `README-db.md`. Spikes S1, S4
and S5 answered by prototype on 24 November 2026 (branch
`prototype/db-bronze-spikes`, `src/db_bronze/PROTOTYPE_spikes/FINDINGS.md`) and
folded in below. S1 (server side), S2, S3 and S9 answered for Postgres and MySQL
by the integration suite (`tests/integration/`, ticket 13) on 24 November 2026.
Ticket 14 ran the full scenario suite on Postgres and MySQL, on the host and in the
image, settled their deny lists and took a first peak-memory reading (S7).
Ticket 15 ran it on SQL Server 2022, settled the SQL Server deny list and answered
S1 (server side), S2, S3 and S9 for SQL Server (2017, 2019 and 2022), on 24 November 2026.
Ticket 16 did the same for Oracle (Free 23.26, inside the image), settled its deny list
and answered S2, S3, S6 and S9 for it. Ticket 17 did the same for Db2 (11.5 and 12.1).

*Verified:* the unit suite (SQLite, both backends) and the integration suite on Postgres
11/12/17, MySQL 8.0/8.4, SQL Server 2017/2019/2022, Oracle Free 23.26 and Db2 11.5/12.1,
on the host and through the Lambda emulator in the image. *Not verified:* Oracle 19c/21c
(accepted by the preflight minimum, Oracle 19, by decision, but never run, S9) and Db2
11.1/10.5 (the Db2 minimum is a floor from evidence, S9); SQL Server and
Oracle integration were not rerun after the last shared harness edits; peak memory on a
real large table (S7); real S3, real Lambda in a VPC, Glue 6.0; Kerberos (placeholder).

This is the database counterpart of `api_bronze` (see `DESIGN-api.md`). It records
the requirement, the facts that constrain the design, every decision taken and
why, and the items that still need a spike before implementation can be called
done.

Evidence labels:

- *researched*: read from library source or vendor documentation at the
  versions named, not yet executed in this repo.
- *prototyped*: executed in a throwaway prototype on SQLite (dlt 1.32.0,
  connectorx 0.4.6), not yet covered by this repo's tests.
- *integration*: measured against a real engine by the integration suite
  (`tests/integration/`), and pinned there by a test.
- *to verify*: an assumption the implementation must prove with a test or a
  spike (collected in section 11).

Nothing in this document is *verified* in the `DESIGN-api.md` sense yet. Labels
are upgraded as tests land.

---

## 1. Requirement

- Ingest tables from relational databases on **AWS Lambda** (container image)
  using **dlt 1.32.0**, into the same S3 **bronze** layer and path convention
  as `api_bronze`.
- Engines that must be covered: **Microsoft SQL Server, MySQL, Db2, Oracle,
  PostgreSQL**. SQLite is supported for tests. Other SQLAlchemy dialects
  work through the traditional backend without special handling.
- Use **ConnectorX** (Arrow streaming) where the engine supports it, and the
  traditional SQLAlchemy path where it does not.
- Production will need **Kerberos**. v1 only reserves a place for it and
  documents how to add it (section 8).

## 2. Scope

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
- Terraform.

---

## 3. Facts that shape the design (*researched*, 24 Nov 2026)

### 3.1 ConnectorX 0.4.6

| Topic | Finding |
|---|---|
| Version | 0.4.6 (2026-09-17). Wheels for cp310–cp314, `manylinux_2_28` x86_64 and aarch64. The wheel is about **200 MB** unpacked. |
| Streaming | `cx.read_sql(conn, query, return_type="arrow_stream", batch_size=N)` returns a `pyarrow.RecordBatchReader`. Introduced in 0.4.4. |
| **Pin ≥ 0.4.6** | In 0.4.5 a producer error ended the stream silently, so a failed read looked like a short, successful one. 0.4.6 joins the producer thread and raises. |
| Memory | The producer pushes into an **unbounded** channel. If Parquet writing is slower than the database read, batches accumulate in memory. *Prototyped:* holding a reader 2 s without consuming buffered a whole 3M-row result (+250 MiB). *Integration (Oracle):* the Oracle source reads the **whole result before the first batch** arrives (600 rows at 10 ms each: first batch at 7.4 s, the last at 7.41 s), so `batch_size` only slices it and peak memory is the whole result. An Oracle table larger than memory must use `keyset`. |
| **Dropping a stream** | *Prototyped (S1):* `close()`/`del` on the reader returns at once, but the producer thread **detaches and reads the query to the end**, holding the connection open (33 s query: idle at +34 s). On Lambda the frozen container keeps the DB session, snapshot and locks open until the next thaw. *Integration (S1):* on Postgres the query is still `active` in `pg_stat_activity` after the drop, on MySQL it is still in the processlist, and on SQL Server it is still in `sys.dm_exec_requests`. On SQL Server the first batch arrives only after most of the query has run. On **Oracle** the stream does not stream (row above): by the time the first batch arrives the query has finished and the session is gone, so a drop leaves nothing running. |
| Engines | postgres, redshift, mysql, mssql, oracle, sqlite, bigquery, clickhouse, duckdb, trino. Trino cannot stream. **Db2 is not supported.** |
| Oracle | Compiled into the wheel, but it loads `libclntsh.so` at runtime, so it **needs Oracle Instant Client**. |
| Kerberos | **MSSQL only.** The Linux wheel bundles MIT krb5 and supports `trusted_connection=true`. Postgres (tokio-postgres) and MySQL (mysql crate) have no GSSAPI. Oracle external auth through OCI is plausible but untested. |
| Unmapped types | Raise `pyo3_runtime.PanicException`. It derives from **`BaseException`**, so `except Exception` does not catch it. Other errors raise `RuntimeError`. *Integration:* a server error in the middle of an `arrow_stream` also surfaces as `PanicException` ("cx writer failed"), on the batch read. |
| Type gaps | **Oracle:** RAW, ROWID, INTERVAL, TIMESTAMP WITH LOCAL TIME ZONE, NCLOB, BFILE, XMLTYPE. *Integration (settled by ticket 14):* **Postgres:** interval, money, timetz, macaddr, macaddr8, xml, cidr, varbit, the geometric types (point, line, lseg, box, path, polygon, circle), tsvector, tsquery, `"char"`, pg_lsn, composite types, and arrays of any element type except smallint, integer, bigint, boolean, real, double precision, numeric, varchar and text (those arrays are read as `large_list`; a numeric element is `decimal128(38,10)`, so the decimal rule applies to it; a value with more dimensions than one fails the read). Postgres inet, `bit(n)`, ranges, enums, `name` and `oid` are read. **MySQL:** the geometry types (geometry, point, linestring, polygon and their multi- and collection forms); every other MySQL type is read. *Integration (settled by ticket 15):* **MSSQL:** sql_variant, xml and the CLR types (hierarchyid, geography, geometry) panic ("not yet implemented"). Two more are read **wrongly, without an error**: `money` arrives as a double, too narrow for its 19 digits (`922337203685477.5807` lands as `922337203685477.6`), and `datetimeoffset` arrives with its offset applied twice (`08:00+02:00` lands as `04:00Z`, not `06:00Z`). smallmoney (10 digits) survives as a double. *Integration (ticket 16, Oracle Free 23.26):* **Oracle:** every researched entry is confirmed (RAW, ROWID, INTERVAL DAY TO SECOND and YEAR TO MONTH, TIMESTAMP WITH LOCAL TIME ZONE, NCLOB, BFILE, XMLTYPE all raise), and the list was short of **TIMESTAMP WITH TIME ZONE** (a type-check error in 0.4.6) and the 23ai **BOOLEAN** and **JSON**. NUMBER, FLOAT, BINARY_FLOAT/DOUBLE, VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, BLOB, DATE and TIMESTAMP(n) are read. The preflight deny list matches these (section 6.2). |
| **Decimals** | Always produced as `Decimal128(38,10)` and limited to about 28 significant digits. **A column with scale above 10 loses digits silently, without raising an error.** *Integration:* MySQL `decimal(65,30)` loses them silently; Postgres `numeric(38,12)` fails with `RuntimeError`; and a Postgres **`numeric` without precision** loses every digit past the 10th decimal place silently (`1.123456789012345678` lands as `1.1234567890`). Oracle is the same: `NUMBER(p, s)` with s > 0 lands as `decimal128(38,10)`, `NUMBER(38,12)` fails with `RuntimeError`, and `NUMBER` without precision loses digits silently. But Oracle **`NUMBER(p, 0)`** (and `INTEGER`) is read as **int64** whatever p, so it holds no decimal to lose: a value wider than int64 fails the read loudly (`PanicException`, `ParseIntError { kind: PosOverflow }`). |
| Integer widths | *Integration:* on Postgres, `arrow_stream` widens `smallint` and `integer` to int64 and `real` to double (the plain `arrow` return type does not). On MySQL the source widths stay (int8, int16, int32, float). On SQL Server `tinyint`, `smallint` and `int` widen to int64 and `real` to double. A SQL Server `datetime` arrives as the 1/300 s it holds (`.123` as `.123333`), where pyodbc rounds it to the millisecond. |
| Session settings | *Integration (S3):* the Postgres URL takes libpq's `options`, so `-c default_transaction_isolation=...` (and any other `-c` setting) becomes the session default, on every connection including `partition_on` ones. The MySQL URL rejects unknown parameters (no `init` command), and `transaction_isolation` cannot be set with a `SET_VAR` hint. Multi-statement SQL is rejected on both. Spaces in URL parameters must be `%20`: ConnectorX reads `+` literally. The SQL Server URL **ignores** unknown parameters silently (an `isolation_level` parameter does nothing), but a plain read runs the SQL as a batch, so `SET TRANSACTION ISOLATION LEVEL ...; SELECT ...` applies the level to the query. A `partition_on` read parses the SQL and rejects anything but a single SELECT. The **Oracle** URL also accepts any query parameter and ignores it (`?isolation_level=SERIALIZABLE` changes nothing), but a leading `ALTER SESSION ...;` is rejected (`ORA-00903`), and there is no hint for the isolation level. |
| `partition_on` | Integer column, N parallel connections. Works with `arrow_stream`, but batches from different partitions interleave, so **rows do not arrive in cursor order**. *Integration:* ConnectorX puts the column name into its own range and partition SQL as given, so on Postgres an unquoted mixed-case name is folded to lower case and fails; the name is passed quoted for the engine. On **Oracle** it upper-cases the column even when it is quoted (`"CXTMPTAB_RANGE"."TRANSACTIONID"`), so only a case-insensitive column (stored upper case, written lower case in the config) can be partitioned on: the preflight says so for any other. |
| Column names | *Integration (Oracle):* ConnectorX reports the names the server gives. Oracle gives a case-insensitive column upper case (`ID`), where SQLAlchemy and the config say `id`, so ConnectorX batches are renamed by position to the statement's column names and both backends land the same names. Quoted (mixed-case) names come back exactly. |

### 3.2 dlt 1.32.0

- The `sql_table` ConnectorX backend can stream: `backend_kwargs={"return_type": "arrow_stream"}`. Its docstring saying it ignores `chunk_size` is out of date. It still reflects the table through the SQLAlchemy engine, and it compiles queries with `literal_binds`.
- A resource that yields `pa.Table` or `pa.RecordBatch` is fully supported by `dlt.sources.incremental` through `ArrowIncremental`, which handles filtering and state.
- On Arrow items dlt still applies its **naming convention**, snake_casing column names, unless the convention is changed.
- *Prototyped (S5):* `direct` naming keeps source names exactly (spaces, dashes, case). A `data_type: freeze` contract fails the run with `DataValidationError` on any type change of an existing column, **including a decimal precision/scale change** (`decimal(38,10)` → `decimal(18,4)`) and int64 → int32. Naive → tz-aware on a timestamp is not caught.
- *Prototyped (S5):* naive Arrow timestamps land as `timestamp[us, tz=UTC]`, labelled but **not shifted**, unless the column carries the hint `timezone: False`.
- *Prototyped (S4):* without `lag`, boundary deduplication by `primary_key` works on Arrow items. **With `lag`, dlt disables deduplication entirely** (`compute_deduplication_disabled`: "destination must deduplicate ranges"), so every run re-lands every row in `[last_value − lag, …)`. `cursor.start_value` is already lag-adjusted. Naive cursors stay naive in state.

### 3.3 Traditional drivers

| Engine | Driver | Kerberos (for section 8) |
|---|---|---|
| MSSQL | `pyodbc` + msodbcsql18 + unixODBC (OS packages, from Microsoft's RHEL 9 repo). *Integration (S1):* closing the cursor mid-result ends the query on the server at once. pyodbc reads neither `sql_variant` nor the CLR types (ODBC `SQL_SS_UDT`, -151) without an output converter; db_bronze adds one for the CLR types (section 7.2). | `Trusted_Connection=yes`. SPN `MSSQLSvc/<fqdn>:<port>`. The driver does not renew tickets. `ActiveDirectoryIntegrated` is Entra ID, not on-premises AD. |
| Postgres | `psycopg[binary]` 3.3.x | The bundled libpq is built `--with-gssapi`. `krbsrvname` defaults to `postgres`. |
| MySQL | `PyMySQL` | None. Kerberos needs MySQL Enterprise on the server plus mysql-connector-python with its `gssapi` extra. *Integration (S1):* closing PyMySQL's streaming (unbuffered) cursor early reads the rest of the result, so the close blocks until the query has run to the end. |
| Oracle | `oracledb` in **thick** mode + Instant Client | Thin mode has no Kerberos. Thick mode uses `sqlnet.ora` with `SQLNET.AUTHENTICATION_SERVICES=(KERBEROS5)`. |
| Db2 | `ibm_db` + `ibm_db_sa`. *Integration (Db2 Community 12.1.5 and 11.5.5, ibm_db 3.3.0, ibm_db_sa 0.4.4):* the dialect calls itself `ibm_db_sa`, not `db2`. Three of its defaults are wrong for bronze and are corrected in `prepare_engine` (section 7.2): it reflects tables without `schema` in the connecting user's schema even when `CurrentSchema` makes another schema current; it declares no native Decimal support, so SQLAlchemy sends every DECIMAL through a float both ways (a bound value and every value read lose digits past the 15th, silently); and it does not know DECFLOAT, BINARY or VARBINARY, and types DOUBLE as an unconstrained NUMERIC. It has no AUTOCOMMIT level. Its `server_version_info` is text, `('DB2/LINUXX8664', '12.01.0500')`. Closing ibm_db's cursor mid-result ends the query at once (S1). The server sends a result in blocks of about 64 KB, so a query's first rows can arrive only once a block is full. In the Lambda image the clidriver prints `lscpu: command not found` on every connect, which is harmless. | The bundled clidriver ships the IBMkrb5 plugin. **Wheels exist for x86_64 only**, with no aarch64. |

---

## 4. Decisions

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
| D11 | Kerberos: placeholder slot plus `docs/KERBEROS.md` only. | Building it now, before any project needs it. |
| D12 | `_extracted_at` is the only added column. The backend used appears in logs and metrics only. | A `_backend` column. Source db and schema columns, which are constant per config. |

### 4.1 Deliberate deviations (recorded at review, 25 Nov 2026)

Each of these goes beyond, or against, what this document first specified. All are kept
on purpose, and each rests on evidence from the integration suite or a review decision.

| Deviation | Why | Evidence |
|---|---|---|
| `max_time_s` is rejected for a **MySQL** `stream` table on the traditional backend, not only for ConnectorX streams (section 6.1). | Closing PyMySQL's unbuffered cursor reads the rest of the result first, so a cut stream blocks until its query has run to the end, exactly as a dropped ConnectorX stream does. | S1, integration (MySQL 8.0/8.4); section 7.4. |
| `sql_variant` is a preflight finding on **every** backend, not only ConnectorX (section 6.2). | pyodbc reads a `sql_variant` only as the bytes of its value, without its type, so no backend lands it losslessly. | Integration, ticket 15 (SQL Server 2017/2019/2022); section 3.3. |
| **Db2 and Oracle type overrides** in reflection and extraction: Db2 DECFLOAT as exact text, BINARY/VARBINARY and CHAR/VARCHAR FOR BIT DATA as binary, DOUBLE as double, native Decimals and CURRENT SCHEMA as the default schema (`extract.prepare_engine`, `source.reflect`); Oracle NUMBER without precision read as Decimal, TIMESTAMP WITH TIME ZONE read through `SYS_EXTRACT_UTC`. | Without them those types land wrongly or lose digits silently: ibm_db_sa reflects them wrongly or not at all, and Oracle's driver hands NUMBER over as float and drops the offset of TIMESTAMP WITH TIME ZONE. | Integration type matrix, tickets 16 and 17 (section 10.1); section 3.3. |
| `tests/test_db_readme.py`: a test that README-db.md documents every config key, event key, mode, enumerated value, error type and metric, and the server minimums as the code has them. | Not in the test plan (section 10). It keeps the operator spec from falling behind the code. | Ticket 19. |
| `finalize` rejects `"tables"` (section 7.5). | It names its one table in `table`; a `tables` list beside it could only repeat or contradict it. | Review. |
| `backend: sqlalchemy` is rejected when the config loads (sections 6.1, 7.2). | It is not built; failing at config load beats failing mid-run with `NotImplementedError`. | Review decision D-a. |
| The Oracle preflight minimum is **19**, below the only version the suite ran on (23). | 19c is Oracle's long-term release; refusing it would block every 19c source. 19c and 21c stay **unverified** (S9). | Review decision D-b. |

---

## 5. Configuration

The same style as `api_bronze`: frozen dataclasses, unknown keys rejected,
names matching `^[a-z][a-z0-9_]*$`. **One config document = one database
connection = one pipeline = one Lambda function.** `pipeline_name`,
`dataset_name` and `bucket_url` are the state identity and must stay stable.

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
      "columns": null,
      "where": "BranchCode <> 'TEST'",
      "cursor": {
        "column": "UpdatedAt",
        "type": "timestamp",
        "initial_value": "2020-01-01T00:00:00Z",
        "lag": 300,
        "include_null_cursor": false
      },
      "chunking": {"strategy": "keyset", "order_by": ["UpdatedAt", "TransactionId"], "page_size": 200000}
    },
    {
      "name": "branches",
      "schema": "dbo",
      "table": "Branches",
      "primary_key": ["BranchCode"],
      "backend": "pyarrow"
    }
  ]
}
```

### 5.1 `connection`

| Key | Meaning |
|---|---|
| `dialect` | `mssql`, `postgres`, `mysql`, `oracle`, `db2`, `sqlite`, or another SQLAlchemy dialect name (traditional backend only). |
| `host`, `port`, `database` | Discrete fields. **Oracle** uses `service_name` instead of `database`. **SQLite** uses `path`. Oracle reads the tables of the connecting user's schema unless the table sets `schema`; a case-insensitive Oracle name (stored upper case) is written lower case in the config and lands lower case, as SQLAlchemy names it. **Db2** reads the tables of CURRENT SCHEMA (the connecting user's, or `CurrentSchema` in `driver_options`) unless the table sets `schema`; case-insensitive names behave as on Oracle. |
| `username`, `password` | A literal, or a secret reference as in `api_bronze`: `{"env": ...}` or `{"secret_id": ..., "key": ...}`. Any connection field may be a reference. |
| `auth` | Tagged. `{"type": "password"}` is implemented. `{"type": "kerberos", ...}` parses, then fails at run start (section 8). |
| `driver_options` | Passed to the traditional driver, e.g. ODBC keywords for MSSQL. *Integration (Db2):* ibm_db_sa passes on only `CurrentSchema`, `Security` and the `SSL...` keywords and drops any other silently; the CLI pads a `CurrentSchema` shorter than 8 characters with blanks, which names a different schema, so a short schema is better set per table with `schema`. The code builds **both** the SQLAlchemy URL and the ConnectorX URL from the discrete fields, because the two formats differ (`+driver` and how query parameters are spelled). |
| `isolation_level` | Applied to the traditional backend through SQLAlchemy. With ConnectorX, it is honoured only where the engine allows session settings; elsewhere the combination is rejected by validation (S3). *Integration:* on **Postgres** it reaches ConnectorX as `options=-c default_transaction_isolation=<level>`, joined to any libpq `options` in `driver_options`. On **SQL Server** it goes ahead of the ConnectorX query in the same batch, as `SET TRANSACTION ISOLATION LEVEL <level>; <query>` (any of the five SQL Server levels, SNAPSHOT included), but not on a `partition` read. On **MySQL** and **Oracle** ConnectorX cannot apply it, so it stays rejected (S3). On the traditional backend Oracle applies `SERIALIZABLE` (read back from `v$transaction`). *Integration (Db2):* ibm_db_sa knows only Db2's names and takes `REPEATABLE READ` as Db2's RR, which is the standard's SERIALIZABLE; db_bronze translates the four standard levels (READ UNCOMMITTED → UR, READ COMMITTED → CS, REPEATABLE READ → RS, SERIALIZABLE → RR), and validation rejects SNAPSHOT and AUTOCOMMIT on db2. `SERIALIZABLE` runs at RR (read back from `MON_GET_ACTIVITY`). |
| `naive_timezone` | Default `UTC`. Used **only** to convert cursor values and window bounds between UTC (at the API boundary) and naive source timestamps. Data values are never touched (D2). |

### 5.2 Per table

| Key | Meaning |
|---|---|
| `name` | Bronze table name (the path prefix). |
| `schema` + `table`, or `query` | The source table. `query` is the escape hatch for views and joins, and needs a `cursor` to run incrementally. |
| `primary_key` | Required for `lag`, keyset chunking and offset paging. Also used for dlt's deduplication at the cursor boundary. |
| `columns` | Projection. This is also how a column ConnectorX cannot read is excluded. `null` means all reflected columns. |
| `where` | Static predicate, combined with the cursor filter using AND. |
| `cursor.column`, `cursor.type` | `timestamp` or `integer`. No cursor means a snapshot table, which runs in `full` mode only. |
| `cursor.initial_value` | ISO-8601 (an offset is allowed and converted through `naive_timezone`) or an integer. |
| `cursor.lag` | Seconds, or an integer delta for integer cursors. Re-reads a trailing window to catch transactions that committed late. Passed to dlt incremental `lag`. Requires `primary_key`. **Every run re-lands the whole lag window** (S4); bronze accepts the duplicates and silver removes them (section 12). |
| `cursor.include_null_cursor` | Default `false`. When `true`, adds `OR cursor IS NULL`, which re-reads those rows on every run. |
| `backend` | `connectorx` or `pyarrow`. Overrides `defaults.backend`. When both are null, the default is resolved per D3. `sqlalchemy` (section 7.2) is not built, so the config rejects it (review decision, 25 Nov 2026). |
| `chunking` | `{"strategy": "stream"}` (default) or `{"strategy": "keyset", "order_by": [...], "page_size": N}`. |
| `batch_size` | Rows per Arrow batch (ConnectorX `batch_size`, or the fetch size on the traditional backend). |
| `partition` | `{"column": ..., "num": N}`. ConnectorX `partition_on`. Opt-in, never automatic. |

### 5.3 Default backend resolution (D3)

Resolved from the config alone, so the same config always gives the same backend:

| Dialect | Default backend |
|---|---|
| mssql, postgres, mysql, oracle, sqlite | `connectorx` |
| db2, anything else | `pyarrow` |

Each run logs the resolved backend for every table.

---

## 6. Validation (D5)

### 6.1 When the config loads (static)

Each rule fails with `ConfigError` and a message that says how to fix it.

- `backend: connectorx` with an engine ConnectorX does not support (for example Db2).
- `backend: sqlalchemy`, which is designed (section 7.2) but not built: rejected here rather than failing mid-run (review decision, 25 Nov 2026). The fix is `backend: pyarrow`.
- `backend: connectorx` with `auth.type: kerberos` on any engine except mssql. Oracle stays rejected until the spike in section 8 proves otherwise.
- `isolation_level` on a connection whose tables resolve to `connectorx`, except on Postgres with one of the four standard levels, and on SQL Server with one of its five levels on a table without `partition` (S3). The fix is `backend: pyarrow` or no `isolation_level` (or no `partition`).
- `isolation_level` SNAPSHOT or AUTOCOMMIT on db2, which has neither; the four standard levels are translated to Db2's own names (section 5.1).
- `chunking.strategy: keyset` without `order_by`, or with an `order_by` that does not start with the cursor column. Keyset also requires `primary_key`.
- `cursor.lag > 0` without `primary_key`.
- `partition` combined with anything other than `backend: connectorx`, an integer column, and `chunking: stream`. It is also rejected at run time in `incremental` mode, because it breaks ascending row order.
- `max_time_s` set while any table's stream cannot be stopped early: `backend: connectorx` with `chunking: stream` (cutting it short leaves its query running, S1), or a **MySQL** table on the traditional backend with `chunking: stream` (closing it early blocks until the query has run to the end, S1). The table must use `keyset` to be split-loaded.
- `query` combined with `schema`/`table`, `columns` or `where`.
- A source column named `_extracted_at` (checked again at preflight once columns are known).
- Duplicate `name` values. Names that are not lowercase identifiers.

### 6.2 Preflight at the start of every run

The preflight reflects each table that is about to run, using one SQLAlchemy reflection query per table. It fails the run **before anything is extracted**, with `PreflightError`, and lists every offending column with two fixes: set `"backend": "pyarrow"`, or exclude the column (or cast it in `query`).

Checks:

- **Server version** is at or above a minimum per dialect. The minimums are set from what the integration tests prove, not guessed. A server below the minimum fails; the backend is never switched silently. Proven so far (S9): **Postgres 11**, **MySQL 8.0**, **SQL Server 2017** (14), **Oracle 19** (by decision: 19c and 21c are accepted but **unverified**, only 23 has run, see S9), **Db2 11.5**.
- For tables on ConnectorX:
  - **DECIMAL/NUMERIC with scale > 10 or precision > 28**, and on Postgres and Oracle a **NUMERIC/NUMBER without precision**. This check is mandatory: it is the only thing standing between a table and silent precision loss (section 3.1). Oracle `NUMBER(p, 0)` is exempt whatever p: ConnectorX reads it as int64, and a wider value fails loudly.
  - Column types on the per-dialect deny list from section 3.1. A type SQLAlchemy does not recognise
    (it reflects as `NullType`) is a finding on every server dialect: on Postgres and MySQL these are
    exactly the xml, geometric, pg_lsn, composite and geometry types, none of which ConnectorX reads.
    SQLAlchemy reflects Postgres `"char"` and `name` alike (a plain `String`), so both are findings,
    although ConnectorX reads `name`. A `query` table's column types are not reflected, so a type
    ConnectorX cannot read there fails the run with `ConnectorXFailed` instead. SQL Server's CLR
    types (hierarchyid, geography, geometry) are registered with the reflection under their own names,
    so they are named in the finding; `money` and `datetimeoffset` are findings because ConnectorX
    reads them wrongly without an error (section 3.1). Oracle XMLTYPE and INTERVAL YEAR TO MONTH
    reflect as `NullType` and are caught by the rule for unrecognised types.
- For tables with `partition`: the column exists, is an integer, and on Oracle does not have a case-sensitive (quoted, mixed-case) name, which ConnectorX upper-cases in its range query (section 3.1). These are table findings like the others, so `validate` reports them too.
- On every backend: a type no backend lands losslessly. On SQL Server that is `sql_variant`: pyodbc
  reads only the bytes of its value, without its type. The fix is to exclude the column or cast it in
  `query`.
- Rows with a NULL cursor are **counted and logged** when `include_null_cursor` is false, so the gap is visible. That count is the run's `null_cursor_rows` and its `NullCursorRows` metric (section 7.6).

The same checks run, without extracting, as `{"mode": "validate"}`. Use that when onboarding a new source.

---

## 7. Runtime design

### 7.1 Request flow

`handler.handler` → `config.SourceConfig` / `RunRequest` → `preflight.check` →
`pipeline.run_pipeline` → `source.make_resource` (one dlt resource per table)
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
`literal_binds`, as dlt does. *Integration (S2):* on Postgres and MySQL the
inlined datetime (to the microsecond), time-zone-aware datetime (Postgres
`timestamptz`), date and `numeric(28,10)` literals select exactly the rows the
bound statement does. *Integration (S2), SQL Server:* SQLAlchemy inlines a
timestamp as text with six decimals, and SQL Server converts the text to the
column's type: `datetime` takes three decimals and `smalldatetime` none, so the
query fails (errors 241 and 295), and every incremental and `full` run would
(their bounds carry microseconds). The cursor bounds and keyset seeds are
therefore inlined for SQL Server as `CAST('...' AS DATETIME2)` (`DATETIMEOFFSET`
for a value with an offset). The comparison is then made at `datetime2`
precision, as pyodbc's bound parameters are, so on `datetime`, `smalldatetime`,
`datetime2(7)` and `datetimeoffset` cursors the inlined window selects exactly
the rows the bound one does. Note that a `datetime` stores 1/300 s, so `.127` is
`.12666...` and sits inside a window ending at `.126999`, on both backends. Decimal
and date literals inline correctly as they are. On **Oracle** (*integration*) SQLAlchemy
inlines a timestamp as `TO_TIMESTAMP('2026-11-20 08:00:00.123456', 'YYYY-MM-DD
HH24:MI:SS.FF')`, a date as `TO_DATE(...)` and a decimal as its digits; they select
exactly the rows the bound statement does, through ConnectorX and in a handler
backfill window bounded to the microsecond, on both backends. A time-zone-aware
value renders with its offset but without `TZH:TZM` in the format, which Oracle
rejects (`ORA-01830`); that never reaches ConnectorX, because it cannot read
TIMESTAMP WITH TIME ZONE (preflight), and the pyarrow backend binds it. **Db2**
(*integration*) never reaches ConnectorX, so its queries are always bound; inlined
anyway, a timestamp renders as `'2026-11-20 08:00:00.123456'` and a decimal as its
digits, and the cursor window and keyset seek select exactly what the bound
statements do, once bound decimals stop going through a float (section 3.3).

### 7.2 Backends

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

On the `pyarrow` backend, driver values Arrow cannot take land as text, without
loss: UUIDs, Postgres `inet`/`cidr`/`bit`, `interval` (dlt 1.32 does not take
Arrow durations), `timetz` (as ISO text, keeping the offset), parsed JSON
(re-serialised compactly, as ConnectorX lands it), MySQL `SET` (members in
definition order), and Postgres **`numeric` without precision** (no decimal type
holds every value). Postgres arrays land as lists typed by their element, with
the same conversions applied to each element, so the type never depends on the
values. Types SQLAlchemy does not recognise land as the driver hands them over:
text on Postgres, binary (WKB) for MySQL geometry. SQL Server's CLR types (hierarchyid,
geography, geometry) land as binary, SQL Server's own serialization (what
`CAST(c AS varbinary(max))` gives), through a pyodbc output converter for
`SQL_SS_UDT`. SQL Server `datetimeoffset` lands as `timestamp[us, UTC]`, the
instant (SQLAlchemy reflects it as without time zone, so it is special-cased).
`datetime2` and `time` with 7 decimals are truncated to the microsecond on both
backends, Arrow's finest unit here. MySQL `BIGINT UNSIGNED` lands as uint64 and MySQL `DOUBLE`
(which SQLAlchemy reflects as Decimal) as double.

On **Oracle** (*integration*): `NUMBER` without precision lands as its exact text
(SQLAlchemy reflects it as a float type; reflection asks for Decimals instead).
`NUMBER(19..38)` lands as `decimal128(p, 0)`, wider than int64. **TIMESTAMP WITH
TIME ZONE** is selected through `SYS_EXTRACT_UTC` and lands as `timestamp[us, UTC]`
holding the instant: the driver hands it over as the wall time with the offset
dropped, so `08:00 +02:00` would otherwise land as 08:00 UTC. TIMESTAMP WITH LOCAL
TIME ZONE lands naive, in the session time zone (UTC on Lambda). INTERVAL DAY TO
SECOND lands as Python's text (`1 day, 2:00:00`), INTERVAL YEAR TO MONTH as Oracle's
(`1-2`), BFILE as its locator `<directory>/<file name>`, and 23ai JSON compactly
with its numbers' own digits. Section 10.1 lists every type.

On **Db2** (*integration*, pyarrow only): DECIMAL lands as `decimal128(p, s)` with
every digit, once the engine reads Decimals natively. DECFLOAT lands as its exact
text (no Arrow decimal holds DECFLOAT(34), NaN or Infinity), DOUBLE as double, and
BINARY, VARBINARY and CHAR/VARCHAR **FOR BIT DATA** as binary: the reflection
types FOR BIT DATA as text, so the catalog (code page 0) names those columns, and
landed as text their bytes would fail the run unless they happened to be UTF-8.
TIMESTAMP(12) is truncated to the microsecond. XML lands as the driver serializes
it, with a byte-order mark and an `encoding="UTF-16"` declaration.

Naive source timestamps stay naive in bronze (D2): every timestamp column the
reflection reports as without time zone gets the resource column hint
`{"timezone": false}`. Without the hint dlt labels them UTC without shifting
them, which is the wrong instant whenever `naive_timezone` is not UTC (S5).

### 7.3 Cursor handling

- Cursor state holds values **as the source represents them** (naive timestamps stay naive). Conversion through `naive_timezone` happens only at the boundaries: config `initial_value`, event window bounds, and `plan` output.
- The WHERE clause is `cursor >= :start [AND cursor < :end]` (half-open, as in `api_bronze`), ANDed with `where`. With `include_null_cursor` it becomes `(... OR cursor IS NULL)`.
- The query's lower bound is `cursor.start_value`, which dlt already steps back by `lag`.
- Without `lag`, dlt deduplicates rows at the boundary by `primary_key`. With `lag`, dlt deduplicates nothing, so each run re-lands every row in the lag window (*prototyped*, S4). Bronze accepts these exact re-reads, and silver removes them (section 12). Deduplicating ourselves was rejected because the state would grow with the number of rows in the lag window.
- Incremental reads are ordered by the cursor (`row_order="asc"`), which is what makes split loading safe.

### 7.4 Chunking (D7)

- **`stream`**: one query, `ORDER BY cursor`, batches streamed.
- **`keyset`**: repeated short queries, each an index range scan:

  ```sql
  SELECT <cols> FROM t
  WHERE <cursor window> AND <where>
    AND (c1 > :v1 OR (c1 = :v1 AND c2 > :v2))   -- expanded form: Oracle and MSSQL
  ORDER BY c1, c2                               -- have no row-value comparison
  FETCH FIRST :page_size ROWS ONLY              -- dialect-specific, via SQLAlchemy
  ```

  The last row of each page seeds the next. Ties on the cursor are broken by the primary key inside one run, and by dlt's boundary deduplication across runs. Whether the `order_by` columns are indexed cannot be checked portably; the operator docs say to confirm it with the DBA.

- **Time budget** (incremental only). Keyset checks the deadline **itself, between pages**, and never closes a page mid-read, so no query is left running. `add_limit(max_time=...)` is not used for keyset, because it closes the generator mid-page.
  - ConnectorX `stream` tables are **never split-loaded**. Dropping their reader returns at once, but the query runs on to the end in a detached thread with the connection open (*prototyped*, S1). On Lambda the frozen container keeps that DB session open.
  - Validation rejects `max_time_s` alongside such a table (section 6.1). The Lambda-derived budget skips such tables, and that is logged. A ConnectorX table that does not fit one invocation must use `keyset`.
  - The traditional backends' `stream` keeps `add_limit(max_time=...)`, as in `api_bronze`, where closing the stream ends the query. *Integration (S1):* on **Postgres** closing psycopg's server-side cursor ends the query at once. On **MySQL** closing PyMySQL's unbuffered cursor reads the rest of the result first, so the close blocks until the query has run to the end: a MySQL `stream` table is treated like a ConnectorX one (never split-loaded; validation rejects `max_time_s`, and the Lambda-derived budget skips it). On **SQL Server** closing pyodbc's cursor ends the query at once, so a traditional `stream` table is split-loaded as on Postgres. On **Oracle** closing oracledb's cursor ends the query at once, likewise.

### 7.5 Modes and events

| Event | Behaviour | Path / state |
|---|---|---|
| `{"source_id": "x"}` | incremental. `[last cursor − lag, …)` | `mode=incremental/<load date>`; cursor advances |
| `{"source_id": "x", "mode": "full"}` | full, single invocation. `[initial_value, run start)` for cursor tables, everything for snapshot tables. Writes a manifest automatically on success. | `mode=full/<load date>`; cursor untouched |
| `{"source_id": "x", "mode": "full", "tables": ["t"], "page": {"offset": 0, "limit": 500000}, "snapshot_id": "...", "partition_date": "2026-11-24"}` | One page of a planned snapshot. ORDER BY the primary key or `order_by`. A page with `"limit": null` is open-ended. | `mode=full/<partition_date>` |
| `{"source_id": "x", "mode": "backfill", "window_start": ..., "window_end": ...}` | `[start, end)`. ISO or integer to match the cursor type. Optional `partition` read. | `mode=backfill/<window start date>` |
| `{"source_id": "x", "mode": "plan", "plan": "snapshot" \| "backfill", "tables": [...], ...}` | No extract. **snapshot** (`page_size`): `COUNT(*)`, then `[{table, offset, limit}]` with the last page open-ended, plus a fresh `snapshot_id` and `partition_date`. A table with neither `primary_key` nor keyset `order_by` cannot be paged. **backfill** (`windows` or `window_size`): cursor `MIN`/`MAX`, split into N windows or windows of a given size, `[{table, window_start, window_end}]`, half-open and contiguous; timestamps in UTC. The last window ends just past MAX: `MAX + 1`, or `MAX + 1 minute` for timestamps, because a finer step can round back to MAX at the column's resolution (SQL Server `smalldatetime`). | none |
| `{"source_id": "x", "mode": "finalize", "table": "t", "snapshot_id": ..., "load_ids": [...]}` | Writes `<bucket_url>/<dataset>/_manifests/<table>/<snapshot_id>.json`. | none |
| `{"source_id": "x", "mode": "validate"}` | Static checks plus preflight, nothing extracted. | none |

Every event except `finalize` also accepts `"tables": [...]` to run a subset. **Deviation (review, 25 Nov 2026):** `finalize` rejects `tables`. It writes the manifest of exactly one table, named by `table`; a `tables` list beside it could only repeat `table` or contradict it, so it is treated like any other key that belongs to another kind of event (a mistyped event fails instead of being ignored).

**Offset paging caveats.** These go in the operator docs. OFFSET paging is not
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

### 7.6 Failures and observability

| Error type (Lambda `errorType`) | When | Lands? | Cursor |
|---|---|---|---|
| `ConfigError` | Static validation fails | no | holds |
| `PreflightError` | Reflection checks fail | no | holds |
| `ConnectorXFailed` | ConnectorX raises at any point. The message names the table and the reason, gives the exact fix (`"backend": "pyarrow"` on that table), and warns that switching changes bronze column types. | no | holds |
| `BackendChanged` | A table's resolved backend differs from the one in state (D6). The message says the bronze types would change and gives the fix: a new table `name`. | no | holds |
| `NotImplementedError` | `auth.type: kerberos` | no | holds |

There is one `PreflightError` (`db_bronze.errors`), a `DbBronzeError` like the
others, so a preflight failure emits `PreflightFailed` and the structured error
log like every other failure here.

The handler unwraps these from dlt's `PipelineStepFailed`, as `api_bronze`
already does for `BrokenPageError`.

Logs are structured JSON, with one `run start` and one `run done` record. The
`run done` record includes the backend per table and the row counts.

EMF metrics, dimensioned by pipeline and table:

- `RowsExtracted`
- `NullCursorRows`: with `include_null_cursor` false (the default), the preflight's count of the rows
  the cursor filter never reads (within `where`), so the gap shows even though those rows never
  land; with it true, the NULL-cursor rows the run extracted.
- `ConnectorXFailed`
- `PreflightFailed`
- `BackendChanged`

**Backend change guard (D6).** The resolved backend is written to each
resource's dlt state. When it differs from the previous successful run, the
`BackendChanged` metric is emitted and the run fails with `BackendChanged`
before anything is extracted. A table landing under a new `name` has no
previous backend, so the guard passes.

### 7.7 State

Identical to `api_bronze`:

- `pipe.drop()` on every invocation, so state is always restored from the destination.
- One concurrent run per pipeline name.
- Every run starts from the newest state attached to a completed load.

### 7.8 Memory

ConnectorX's stream channel is unbounded, so `batch_size` and Lambda memory are
the only controls. Memory is sized per function from a measured validation run
(peak RSS on the largest table). No custom backpressure is built.

---

## 8. Kerberos (placeholder only, D11)

- The `auth` schema accepts `{"type": "kerberos", "principal": ..., "keytab": {"secret_id": ...}, "krb5_conf": ...}`. At run start it raises `NotImplementedError` pointing to `docs/KERBEROS.md`.
- `docker/Dockerfile.db` carries a **commented-out** block: `krb5-libs`, `krb5-workstation`, `ENV KRB5_CONFIG=/tmp/krb5.conf KRB5CCNAME=FILE:/tmp/krb5cc`.
- `docs/KERBEROS.md` records the section 3 findings so a project can pick this up without repeating the research:
  - per engine: the driver keywords, SPN conventions, and which backend supports it (ConnectorX: MSSQL only);
  - the two ways to get a ticket: a keytab from Secrets Manager plus `kinit -kt` into a `/tmp` FILE ccache, re-checked on every invocation because warm containers outlive the roughly 10-hour AD tickets; or `KRB5_CLIENT_KTNAME`, letting MIT GSSAPI fetch and renew tickets itself;
  - Lambda gotchas: the KEYRING ccache may not work; set `rdns = false` and `udp_preference_limit = 1`; the VPC DNS must resolve the KDCs;
  - the open spikes: ConnectorX MSSQL against AD, Oracle Kerberos through OCI external auth, whether ibm_db and the ConnectorX-bundled krb5 honour `KRB5_CLIENT_KTNAME`, and whether Instant Client Basic includes the Kerberos adapter.

---

## 9. Packaging

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

## 10. Testing

| Layer | What | Where it runs |
|---|---|---|
| Unit | Config validation, default-backend resolution, URL building (SQLAlchemy and ConnectorX), query compilation for every dialect (offline `compile()`), keyset and paging SQL, the preflight rules against fake reflected metadata, `naive_timezone` conversion, error wrapping (including a synthetic `PanicException`), the backend-change guard. An end-to-end run against **SQLite** on both the `connectorx` and `pyarrow` backends: modes, state restore, lag, keyset, plan/page/finalize, manifests. | Anywhere. No network, no Docker. |
| Integration (`-m integration`) | The same scenarios against docker-compose: Postgres, MySQL 8.4, SQL Server 2022, Oracle Free, Db2 Community (x86 only). Type matrices per engine, including the deny-listed and high-scale decimal columns. Server-version floors. Spike measurements (section 11). | Dev machine or CI with Docker |
| Image | The integration scenarios invoked through the Lambda Runtime Interface Emulator inside the built `db-bronze` image (`docker:smoke --db`), proving drivers, Instant Client and ODBC registration. | Dev machine or CI with Docker |

Kerberos is not tested (D11). New mise tasks: `test:integration`, plus `--db` flags on `docker:lock`, `docker:build` and `docker:smoke`.

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
`DYLD_LIBRARY_PATH=$(brew --prefix openssl@3)/lib`.

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

**Oracle** (ticket 16) is the compose service `oracle` (Oracle AI Database Free
23.26, the full image: `-lite` has no XDB for XMLTYPE and no USERS tablespace),
in the compose profile `oracle`, so host runs do not start it. Each test gets a
fresh user, i.e. a schema, on `FREEPDB1`. Its handler needs thick-mode oracledb
and Instant Client (D10), so on the host its tests skip; seeding works from the
host in thin mode. `mise run test:integration --image` runs the Oracle scenarios
through the emulator. `mise run test:integration --inside` runs pytest itself
inside the image on the compose network (`DB_BRONZE_IT_NETWORK=compose`), so the
handler and ConnectorX run in-process with Instant Client: that is what the
spike tests calling ConnectorX directly need on Oracle. `--engines oracle` sets
`DB_BRONZE_IT_ENGINES`.

**Db2** (ticket 17) is the compose service `db2` (Db2 Community 12.1.5,
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
object rather than through `SYSPROC.ADMIN_DROP_SCHEMA`.

### 10.1 Landed types (*integration*, 24 Nov 2026)

The Parquet type each source type lands as, per backend (Postgres 17.12, MySQL
8.4.13; ConnectorX 0.4.6, dlt 1.32.0). `tests/integration/test_it_types.py` pins
these tables. "preflight" means the column fails the preflight on ConnectorX
(deny list or decimal rule). The same file proves both directions for every
column: each one the preflight denies really fails or loses data on ConnectorX,
and each one it allows lands on ConnectorX.

**Postgres**

| Source type | `pyarrow` | `connectorx` |
|---|---|---|
| smallint, integer, bigint | int64 | int64 |
| numeric(18,4), numeric(28,10) | decimal128(p, s) | decimal128(38, 10) |
| numeric(38,12) | decimal128(38, 12) | preflight (RuntimeError) |
| numeric (no precision) | string | preflight (silent loss) |
| real, double precision | double | double |
| boolean | bool | bool |
| text, varchar, char(n) | string | string |
| date | date32 | date32 |
| timestamp | timestamp[us] | timestamp[us] |
| timestamptz | timestamp[us, UTC] | timestamp[us, UTC] |
| time | time64[us] | time64[us] |
| bytea | binary | large_binary |
| uuid, json, jsonb, inet | string | string |
| bit(n) | string | large_binary |
| integer[], text[], numeric(10,2)[] | list&lt;int64&gt;, list&lt;string&gt;, list&lt;decimal128(10, 2)&gt; | large_list&lt;int32&gt;, large_list&lt;string&gt;, large_list&lt;decimal128(38, 10)&gt; |
| numeric[] (no precision) | list&lt;string&gt; | preflight (silent loss) |
| uuid[], char(n)[], jsonb[] | list&lt;string&gt; | preflight |
| timestamp[] | list&lt;timestamp[us]&gt; | preflight |
| interval, money, timetz, macaddr, macaddr8, xml, cidr, varbit, point, line, lseg, box, path, polygon, circle, tsvector, tsquery, `"char"`, pg_lsn, composite | string | preflight |

**MySQL**

| Source type | `pyarrow` | `connectorx` |
|---|---|---|
| tinyint, boolean | int64 | int8 |
| smallint, year | int64 | int16 |
| mediumint, int | int64 | int32 |
| bigint | int64 | int64 |
| bigint unsigned | uint64 | uint64 |
| tinyint unsigned, int unsigned | int64 | uint8, uint32 |
| decimal(18,4), decimal(28,10) | decimal128(p, s) | decimal128(38, 10) |
| decimal(38,12) | decimal128(38, 12) | preflight (RuntimeError) |
| decimal(65,30) | decimal256(65, 30) | preflight (silent loss) |
| decimal(65,0) | decimal256(65, 0) | preflight (fails or loses digits) |
| float | double | float |
| double | double | double |
| bit(n) | int64 | large_binary |
| varchar, char, text, enum, set, json | string | string |
| date | date32 | date32 |
| datetime, datetime(6), timestamp | timestamp[us] | timestamp[us] |
| time | time64[us] | time64[us] |
| binary, varbinary, blob | binary | large_binary |
| geometry, point, linestring, polygon, multipoint, multilinestring, multipolygon, geometrycollection | binary | preflight |

**SQL Server** (2022, CU 28)

| Source type | `pyarrow` | `connectorx` |
|---|---|---|
| tinyint, smallint, int, bigint | int64 | int64 |
| bit | bool | bool |
| decimal(18,4), decimal(28,10) | decimal128(p, s) | decimal128(38, 10) |
| decimal(38,12), decimal(38,0) | decimal128(p, s) | preflight (decimal rule) |
| money | decimal128(19, 4) | preflight (double, digits lost) |
| smallmoney | decimal128(10, 4) | double |
| float, real | double | double |
| char, varchar, varchar(max), nchar, nvarchar, nvarchar(max), text, ntext, uniqueidentifier | string | string |
| date | date32 | date32 |
| datetime, datetime2, datetime2(6), smalldatetime | timestamp[us] | timestamp[us] |
| datetimeoffset | timestamp[us, UTC] | preflight (offset applied twice) |
| time | time64[us] | time64[us] |
| binary, varbinary, varbinary(max), image | binary | large_binary |
| xml | string | preflight |
| hierarchyid, geography, geometry | binary (SQL Server serialization) | preflight |
| sql_variant | preflight (no backend) | preflight |

**Oracle** (Free 23.26; ConnectorX 0.4.6, oracledb 26.1.0 thick, Instant Client 23.26)

| Source type | `pyarrow` | `connectorx` |
|---|---|---|
| number(10), number(18), integer | int64 | int64 |
| number(38) | decimal128(38, 0) | int64 (a wider value fails loudly) |
| number(18,4), number(28,10) | decimal128(p, s) | decimal128(38, 10) |
| number(38,12) | decimal128(38, 12) | preflight (RuntimeError) |
| number (no precision) | string | preflight (silent loss) |
| float, binary_float, binary_double | double | double |
| varchar2, nvarchar2, char, nchar, clob | string | string |
| blob | binary | large_binary |
| date, timestamp, timestamp(3) | timestamp[us] | timestamp[us] |
| timestamp with time zone | timestamp[us, UTC] (the instant) | preflight |
| timestamp with local time zone | timestamp[us] (session time zone) | preflight |
| raw | binary | preflight |
| rowid, nclob, xmltype | string | preflight |
| interval day to second, interval year to month | string | preflight |
| bfile | string (`<directory>/<file name>`) | preflight |
| boolean, json (23ai) | bool, string | preflight |

**Db2** (Community 12.1.5 for LUW; ibm_db 3.3.0, ibm_db_sa 0.4.4). Pyarrow only: ConnectorX does not read Db2.

| Source type | `pyarrow` |
|---|---|
| smallint, integer, bigint | int64 |
| decimal(18,4), decimal(31,12), decimal(31,0) | decimal128(p, s), every digit |
| decfloat(16), decfloat(34) | string (the exact value) |
| real, double | double |
| boolean | bool |
| char, varchar, clob, graphic, vargraphic, dbclob, xml | string (xml with a byte-order mark and a UTF-16 declaration) |
| char/varchar for bit data, binary, varbinary, blob | binary |
| date | date32 |
| time | time64[us] |
| timestamp, timestamp(0), timestamp(12) | timestamp[us] (timestamp(12) truncated to the microsecond) |

The two backends differ on most integer widths, on binary vs large_binary, and
on every decimal, which is why a backend change fails with `BackendChanged`
(D6).

---

## 11. Spikes and open items

| # | Question | If the answer is bad |
|---|---|---|
| S1 | Does dropping a ConnectorX `arrow_stream` reader mid-query block until the query completes? | **Answered (prototype, SQLite):** no block, but the producer detaches and reads to the end with the connection open. Split loading requires `keyset` for ConnectorX tables (section 7.4). **Server side (integration, Postgres and MySQL):** after the drop the query is still active in `pg_stat_activity` and in the MySQL processlist. Closing the traditional backend's streaming cursor ends the query at once on Postgres, but on MySQL the close reads the rest of the result and blocks until the query ends, so MySQL `stream` tables are not split-loaded either (sections 6.1, 7.4). **SQL Server (integration):** the dropped ConnectorX query is still in `sys.dm_exec_requests`; closing pyodbc's cursor ends the query at once, so traditional `stream` tables are split-loaded there. **Oracle (integration):** ConnectorX reads the whole result before the first batch, so after a drop nothing runs; closing oracledb's cursor ends the query at once. **Db2 (integration):** ConnectorX does not apply; closing ibm_db's cursor ends the query at once (it is gone from `MON_GET_ACTIVITY`), so traditional `stream` tables are split-loaded there. |
| S2 | Does `literal_binds` compile datetime and decimal literals correctly for mssql, oracle and db2? | **Answered for Postgres and MySQL (integration):** yes; microsecond, time-zone-aware, date and wide-decimal literals select the same rows as bound parameters (section 7.1). **SQL Server (integration): no** for `datetime` and `smalldatetime` (a six-decimal text literal fails to convert), so cursor bounds are inlined as `CAST(... AS DATETIME2)` (`DATETIMEOFFSET` with an offset), which selects exactly what the bound query does on every SQL Server timestamp type; decimal and date literals are fine (section 7.1). **Oracle (integration):** yes for timestamps (to the microsecond), dates and wide decimals, directly and through a handler backfill window on both backends. A time-zone-aware literal fails (`ORA-01830`), but ConnectorX cannot read TIMESTAMP WITH TIME ZONE anyway, and the pyarrow backend binds it (section 7.1). **Db2 (integration):** moot, since Db2 never reaches ConnectorX and its queries are bound; inlined anyway, microsecond timestamps and 28-digit decimals select exactly what the bound cursor and keyset queries do. Measuring it found that the *bound* side was wrong: ibm_db_sa sent decimals through a float, so a decimal keyset seed lost digits, and so did every DECIMAL value read. Db2 engines now use native Decimals (sections 3.3, 7.1). |
| S3 | Can ConnectorX apply `isolation_level` or session settings for each engine? | **Answered for Postgres and MySQL (integration):** Postgres yes, through libpq `options` in the ConnectorX URL, so validation accepts `isolation_level` with `connectorx` there. MySQL no, so it stays rejected (sections 3.1, 5.1, 6.1). **SQL Server (integration):** yes, as a `SET TRANSACTION ISOLATION LEVEL` ahead of the query in the same batch (its URL ignores unknown parameters), except on `partition` reads, which ConnectorX parses as a single SELECT; validation accepts the combination without `partition`. **Oracle (integration):** no. The ConnectorX URL ignores query parameters, multi-statement SQL is rejected and no hint sets the level, so `isolation_level` with `connectorx` stays rejected; the traditional backend applies it. |
| S4 | Do dlt incremental `lag` and boundary deduplication by `primary_key` work with Arrow items from a custom resource? | **Answered (prototype):** deduplication works without `lag`. With `lag`, dlt deduplicates nothing, so the lag window is re-landed on every run. Decision: bronze accepts it and silver removes the duplicates (sections 7.3, 12). |
| S5 | Do `direct` naming and `data_type: freeze` apply to Arrow items as expected? | **Answered (prototype):** yes, and freeze also catches decimal precision changes, which led to revising D6. Naive timestamps need `timezone: false` hints (section 7.2). |
| S6 | Do Instant Client (Basic) and `libaio` work on AL2023? Can thick-mode oracledb and ConnectorX share `libclntsh` in one process? | Pin an Instant Client version known to work; isolate Oracle in its own image. **First part answered (image smoke test, 24 Nov 2026, linux/amd64 under emulation):** Instant Client Basic 23.26.4.0.0 with `libaio-0.3.111` (AL2023 package) loads. In one process, oracledb 26.1.0 enters thick mode (client 23.26.4) and ConnectorX 0.4.6 then loads `libclntsh` too: an Oracle connection to a closed port fails with `ORA-12541` from OCI, not `DPI-1047`. Same image: unixODBC 2.3.9 (AL2023) with msodbcsql18 18.7.1.1 from the RHEL 9 repo, listed by `pyodbc.drivers()`; ibm_db 3.3.0 imports once `pam` is installed. **Second part answered (integration, ticket 16):** yes. Against Oracle Free 23.26, one invocation lands one table through thick-mode oracledb and another through ConnectorX in the same process, inside the image under the emulator, and the whole Oracle suite runs both backends in one pytest process inside the image. |
| S7 | What is peak memory for the largest real table under ConnectorX streaming? | Smaller `batch_size`, keyset chunking, or more Lambda memory. **First input (integration, 24 Nov 2026):** the whole Postgres and MySQL scenario suite (tables of tens of rows, so this is the fixed cost of a run, not the data) peaks at **588 MiB** for the image container across the session (cgroup `memory.peak`: emulator plus the warm Python runtime, amd64 under QEMU, where jemalloc cannot return memory, so an overestimate) and **254 MiB** RSS for the host pytest process running the handler in-process. A 1 GB function has room for the fixed cost; the per-table figure still needs a run on the largest real table. The SQL Server suite (ticket 15) peaks at **318 MiB** for the image container, amd64 under Rosetta rather than QEMU, and 124 MiB RSS for the host process. |
| S8 | Is egress to packages.microsoft.com and the Oracle download host allowed from the build pipeline? | Internal mirrors through the build arguments. |
| S9 | What are the per-dialect minimum server versions? | **Answered for Postgres and MySQL (integration):** the whole integration suite passes on Postgres 11.16, 12.22 and 17.12, and on MySQL 8.0.46 and 8.4.13. Preflight minimums: Postgres 11, MySQL 8.0. Older series were not run (Postgres 10 and MySQL 5.7 are long out of vendor support). **SQL Server (integration):** the whole suite, both backends, passes on 2017 (14.0.3570.2, CU 31), 2019 (15.0.4510.2) and 2022 (16.0.4325.4, CU 28), on the host, and on 2022 inside the image as well. Preflight minimum: SQL Server 2017 (version 14); SQL Server on Linux, and so the compose stack, starts there. **Oracle (integration, ticket 16):** the whole suite passes on Oracle Free 23.26 (`23.26.0.0`), which first set the preflight minimum to Oracle 23. **Review decision (25 Nov 2026):** the minimum is lowered to **Oracle 19**, so 19c (the long-term release) and 21c are accepted, but they are **unverified**: no integration run has proven them. The older series exist only as x86_64 images (`gvenzl/oracle-xe:21-slim-faststart`, `:18-slim-faststart`), and on this arm64 host their PMON does not start under Rosetta (`ORA-00443`). **Still open:** run `DB_BRONZE_IT_ORACLE_IMAGE=gvenzl/oracle-xe:21-slim-faststart DB_BRONZE_IT_ORACLE_SERVICE=XEPDB1 mise run test:integration --inside --engines oracle` (and `:18`) on an x86_64 Docker host, and raise the minimum if they fail; the matrix leaves out the 23ai BOOLEAN and JSON below 23. 19c, the long-term release, has no public image. **Db2 (integration, ticket 17):** the whole Db2 suite (pyarrow) passes inside the image on Db2 Community 12.1.5.0 and on 11.5.5.1, the oldest Community image published (`icr.io/db2_community/db2` has 11.5.5.1 to 12.1.5.0), so the preflight minimum is **Db2 11.5**. Older series (11.1, 10.5) have no Community image and were not run. To repeat the 11.5 run: `DB_BRONZE_IT_DB2_IMAGE=icr.io/db2_community/db2:11.5.5.1 mise run test:integration --inside --engines db2`; under colima's Rosetta the RHEL 7 based 11.5 image does not create the database by itself (its su skips pam_env, so the compose fix does not reach it), so once its log says "Setup has completed" run `su - db2inst1 -c '. ~/sqllib/db2profile; db2start; db2 create db BRONZE'` in the container. |

Still not validated, as for `api_bronze`: real S3, real Lambda in a VPC
against the production databases, and Glue 6.0 reading typed bronze.

---

## 12. Bronze contract (for silver)

- Paths: `<bucket_url>/<dataset>/<name>/mode=<mode>/year=/month=/day=/<load_id>.<file_id>.parquet`. Append-only, zstd.
- Columns: the source columns with their source names and backend types, plus `_extracted_at` (`timestamp[us, UTC]`). Source timestamps without a time zone stay naive. Interpret them with the config's `naive_timezone`. A table's types never change within one bronze `name`. A backend change fails with `BackendChanged` and lands under a new `name`. A source type change fails ingestion until someone deals with it.
- **Duplicates:** with `cursor.lag`, every incremental run re-lands the rows in its lag window, so identical row versions can repeat. Silver deduplicates on (`primary_key`, cursor) and keeps the latest `_extracted_at`.
- **Completeness:**
  - `incremental` and `backfill`: a file counts only if its load id has `status = 0` in `_dlt_loads`. This is the `api_bronze` rule.
  - `full`: a load counts only if it is listed in a manifest at `_manifests/<name>/<snapshot_id>.json`. Single-invocation full runs write their own manifest. Paged snapshots get theirs from `finalize`. A full load without a manifest is an incomplete snapshot and must be ignored.
- Merging into silver uses `primary_key`, from the config or the documented table metadata. Silver itself is out of scope for v1.

---

## 13. Implementation order

1. Config, validation and URL building, with unit tests.
2. Query builder (stream, keyset, paging) with offline compile tests for every dialect.
3. Resource and backends; end-to-end on SQLite for both backends; modes and state.
4. Preflight and `validate`; the backend-change guard; error types and handler unwrapping.
5. `plan`, `finalize` and manifests; the example ASL.
6. `Dockerfile.db`, lock and mise tasks; the image smoke test.
7. The docker-compose integration suite; run spikes S1–S9 and fold the results back into this document.
8. `README-db.md` (operator spec), `docs/KERBEROS.md`, `CLAUDE.md` update.
