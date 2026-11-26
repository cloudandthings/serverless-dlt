# Engine and driver facts (`db_bronze`)

What ConnectorX, dlt and each traditional driver do, per engine: the research and
measurements the db_bronze design rests on. Evidence labels (*researched*, *prototyped*,
*integration*) are defined in [design.md](design.md#evidence-labels); spike numbers
(S1–S9) refer to [design.md](design.md#spikes). The resulting operator rules are in
[configuration.md](configuration.md), [preflight.md](preflight.md) and
[types.md](types.md).

## ConnectorX 0.4.6

*Researched* 24 Nov 2026, unless labelled otherwise.

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
| Type gaps | **Oracle:** RAW, ROWID, INTERVAL, TIMESTAMP WITH LOCAL TIME ZONE, NCLOB, BFILE, XMLTYPE. *Integration (settled 24 Nov 2026, Postgres and MySQL):* **Postgres:** interval, money, timetz, macaddr, macaddr8, xml, cidr, varbit, the geometric types (point, line, lseg, box, path, polygon, circle), tsvector, tsquery, `"char"`, pg_lsn, composite types, and arrays of any element type except smallint, integer, bigint, boolean, real, double precision, numeric, varchar and text (those arrays are read as `large_list`; a numeric element is `decimal128(38,10)`, so the decimal rule applies to it; a value with more dimensions than one fails the read). Postgres inet, `bit(n)`, ranges, enums, `name` and `oid` are read. **MySQL:** the geometry types (geometry, point, linestring, polygon and their multi- and collection forms); every other MySQL type is read. *Integration (settled 24 Nov 2026, SQL Server):* **MSSQL:** sql_variant, xml and the CLR types (hierarchyid, geography, geometry) panic ("not yet implemented"). Two more are read **wrongly, without an error**: `money` arrives as a double, too narrow for its 19 digits (`922337203685477.5807` lands as `922337203685477.6`), and `datetimeoffset` arrives with its offset applied twice (`08:00+02:00` lands as `04:00Z`, not `06:00Z`). smallmoney (10 digits) survives as a double. *Integration (Oracle Free 23.26):* **Oracle:** every researched entry is confirmed (RAW, ROWID, INTERVAL DAY TO SECOND and YEAR TO MONTH, TIMESTAMP WITH LOCAL TIME ZONE, NCLOB, BFILE, XMLTYPE all raise), and the list was short of **TIMESTAMP WITH TIME ZONE** (a type-check error in 0.4.6) and the 23ai **BOOLEAN** and **JSON**. NUMBER, FLOAT, BINARY_FLOAT/DOUBLE, VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, BLOB, DATE and TIMESTAMP(n) are read. The preflight deny list matches these ([preflight.md](preflight.md)). |
| **Decimals** | Always produced as `Decimal128(38,10)` and limited to about 28 significant digits. **A column with scale above 10 loses digits silently, without raising an error.** *Integration:* MySQL `decimal(65,30)` loses them silently; Postgres `numeric(38,12)` fails with `RuntimeError`; and a Postgres **`numeric` without precision** loses every digit past the 10th decimal place silently (`1.123456789012345678` lands as `1.1234567890`). Oracle is the same: `NUMBER(p, s)` with s > 0 lands as `decimal128(38,10)`, `NUMBER(38,12)` fails with `RuntimeError`, and `NUMBER` without precision loses digits silently. But Oracle **`NUMBER(p, 0)`** (and `INTEGER`) is read as **int64** whatever p, so it holds no decimal to lose: a value wider than int64 fails the read loudly (`PanicException`, `ParseIntError { kind: PosOverflow }`). |
| Integer widths | *Integration:* on Postgres, `arrow_stream` widens `smallint` and `integer` to int64 and `real` to double (the plain `arrow` return type does not). On MySQL the source widths stay (int8, int16, int32, float). On SQL Server `tinyint`, `smallint` and `int` widen to int64 and `real` to double. A SQL Server `datetime` arrives as the 1/300 s it holds (`.123` as `.123333`), where pyodbc rounds it to the millisecond. |
| Session settings | *Integration (S3):* the Postgres URL takes libpq's `options`, so `-c default_transaction_isolation=...` (and any other `-c` setting) becomes the session default, on every connection including `partition_on` ones. The MySQL URL rejects unknown parameters (no `init` command), and `transaction_isolation` cannot be set with a `SET_VAR` hint. Multi-statement SQL is rejected on both. Spaces in URL parameters must be `%20`: ConnectorX reads `+` literally. The SQL Server URL **ignores** unknown parameters silently (an `isolation_level` parameter does nothing), but a plain read runs the SQL as a batch, so `SET TRANSACTION ISOLATION LEVEL ...; SELECT ...` applies the level to the query. A `partition_on` read parses the SQL and rejects anything but a single SELECT. The **Oracle** URL also accepts any query parameter and ignores it (`?isolation_level=SERIALIZABLE` changes nothing), but a leading `ALTER SESSION ...;` is rejected (`ORA-00903`), and there is no hint for the isolation level. |
| `partition_on` | Integer column, N parallel connections. Works with `arrow_stream`, but batches from different partitions interleave, so **rows do not arrive in cursor order**. *Integration:* ConnectorX puts the column name into its own range and partition SQL as given, so on Postgres an unquoted mixed-case name is folded to lower case and fails; the name is passed quoted for the engine. On **Oracle** it upper-cases the column even when it is quoted (`"CXTMPTAB_RANGE"."TRANSACTIONID"`), so only a case-insensitive column (stored upper case, written lower case in the config) can be partitioned on: the preflight says so for any other. |
| Column names | *Integration (Oracle):* ConnectorX reports the names the server gives. Oracle gives a case-insensitive column upper case (`ID`), where SQLAlchemy and the config say `id`, so ConnectorX batches are renamed by position to the statement's column names and both backends land the same names. Quoted (mixed-case) names come back exactly. |

## dlt 1.32.0

- The `sql_table` ConnectorX backend can stream: `backend_kwargs={"return_type": "arrow_stream"}`. Its docstring saying it ignores `chunk_size` is out of date. It still reflects the table through the SQLAlchemy engine, and it compiles queries with `literal_binds`.
- A resource that yields `pa.Table` or `pa.RecordBatch` is fully supported by `dlt.sources.incremental` through `ArrowIncremental`, which handles filtering and state.
- On Arrow items dlt still applies its **naming convention**, snake_casing column names, unless the convention is changed.
- *Prototyped (S5):* `direct` naming keeps source names exactly (spaces, dashes, case). A `data_type: freeze` contract fails the run with `DataValidationError` on any type change of an existing column, **including a decimal precision/scale change** (`decimal(38,10)` → `decimal(18,4)`) and int64 → int32. Naive → tz-aware on a timestamp is not caught.
- *Prototyped (S5):* naive Arrow timestamps land as `timestamp[us, tz=UTC]`, labelled but **not shifted**, unless the column carries the hint `timezone: False`.
- *Prototyped (S4):* without `lag`, boundary deduplication by `primary_key` works on Arrow items. **With `lag`, dlt disables deduplication entirely** (`compute_deduplication_disabled`: "destination must deduplicate ranges"), so every run re-lands every row in `[last_value − lag, …)`. `cursor.start_value` is already lag-adjusted. Naive cursors stay naive in state.

## Traditional drivers

| Engine | Driver | Kerberos (see [kerberos.md](kerberos.md)) |
|---|---|---|
| MSSQL | `pyodbc` + msodbcsql18 + unixODBC (OS packages, from Microsoft's RHEL 9 repo). *Integration (S1):* closing the cursor mid-result ends the query on the server at once. pyodbc reads neither `sql_variant` nor the CLR types (ODBC `SQL_SS_UDT`, -151) without an output converter; db_bronze adds one for the CLR types ([types.md](types.md)). | `Trusted_Connection=yes`. SPN `MSSQLSvc/<fqdn>:<port>`. The driver does not renew tickets. `ActiveDirectoryIntegrated` is Entra ID, not on-premises AD. |
| Postgres | `psycopg[binary]` 3.3.x | The bundled libpq is built `--with-gssapi`. `krbsrvname` defaults to `postgres`. |
| MySQL | `PyMySQL` | None. Kerberos needs MySQL Enterprise on the server plus mysql-connector-python with its `gssapi` extra. *Integration (S1):* closing PyMySQL's streaming (unbuffered) cursor early reads the rest of the result, so the close blocks until the query has run to the end. |
| Oracle | `oracledb` in **thick** mode + Instant Client | Thin mode has no Kerberos. Thick mode uses `sqlnet.ora` with `SQLNET.AUTHENTICATION_SERVICES=(KERBEROS5)`. |
| Db2 | `ibm_db` + `ibm_db_sa`. *Integration (Db2 Community 12.1.5 and 11.5.5, ibm_db 3.3.0, ibm_db_sa 0.4.4):* the dialect calls itself `ibm_db_sa`, not `db2`. Three of its defaults are wrong for bronze and are corrected in `prepare_engine` ([types.md](types.md)): it reflects tables without `schema` in the connecting user's schema even when `CurrentSchema` makes another schema current; it declares no native Decimal support, so SQLAlchemy sends every DECIMAL through a float both ways (a bound value and every value read lose digits past the 15th, silently); and it does not know DECFLOAT, BINARY or VARBINARY, and types DOUBLE as an unconstrained NUMERIC. It has no AUTOCOMMIT level. Its `server_version_info` is text, `('DB2/LINUXX8664', '12.01.0500')`. Closing ibm_db's cursor mid-result ends the query at once (S1). The server sends a result in blocks of about 64 KB, so a query's first rows can arrive only once a block is full. In the Lambda image the clidriver prints `lscpu: command not found` on every connect, which is harmless. | The bundled clidriver ships the IBMkrb5 plugin. **Wheels exist for x86_64 only**, with no aarch64. |

## Connection settings per engine

How each engine treats the connection keys of [configuration.md](configuration.md#connection), as measured.

| Key | Meaning |
|---|---|
| `host`, `port`, `database` | Discrete fields. **Oracle** uses `service_name` instead of `database`. **SQLite** uses `path`. Oracle reads the tables of the connecting user's schema unless the table sets `schema`; a case-insensitive Oracle name (stored upper case) is written lower case in the config and lands lower case, as SQLAlchemy names it. **Db2** reads the tables of CURRENT SCHEMA (the connecting user's, or `CurrentSchema` in `driver_options`) unless the table sets `schema`; case-insensitive names behave as on Oracle. |
| `driver_options` | Passed to the traditional driver, e.g. ODBC keywords for MSSQL. *Integration (Db2):* ibm_db_sa passes on only `CurrentSchema`, `Security` and the `SSL...` keywords and drops any other silently; the CLI pads a `CurrentSchema` shorter than 8 characters with blanks, which names a different schema, so a short schema is better set per table with `schema`. The code builds **both** the SQLAlchemy URL and the ConnectorX URL from the discrete fields, because the two formats differ (`+driver` and how query parameters are spelled). |
| `isolation_level` | Applied to the traditional backend through SQLAlchemy. With ConnectorX, it is honoured only where the engine allows session settings; elsewhere the combination is rejected by validation (S3). *Integration:* on **Postgres** it reaches ConnectorX as `options=-c default_transaction_isolation=<level>`, joined to any libpq `options` in `driver_options`. On **SQL Server** it goes ahead of the ConnectorX query in the same batch, as `SET TRANSACTION ISOLATION LEVEL <level>; <query>` (any of the five SQL Server levels, SNAPSHOT included), but not on a `partition` read. On **MySQL** and **Oracle** ConnectorX cannot apply it, so it stays rejected (S3). On the traditional backend Oracle applies `SERIALIZABLE` (read back from `v$transaction`). *Integration (Db2):* ibm_db_sa knows only Db2's names and takes `REPEATABLE READ` as Db2's RR, which is the standard's SERIALIZABLE; db_bronze translates the four standard levels (READ UNCOMMITTED → UR, READ COMMITTED → CS, REPEATABLE READ → RS, SERIALIZABLE → RR), and validation rejects SNAPSHOT and AUTOCOMMIT on db2. `SERIALIZABLE` runs at RR (read back from `MON_GET_ACTIVITY`). |

## Inlined literals (S2)

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
statements do, once bound decimals stop going through a float ([Traditional drivers](#traditional-drivers)).
