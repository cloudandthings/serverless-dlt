# Preflight and `validate` (`db_bronze`)

Every run (and `mode: validate`, which stops there) reflects each table about to run and
fails **before anything is extracted** with `PreflightError`, listing every offending
column. The two fixes are the same everywhere: set `"backend": "pyarrow"` on the table,
or exclude the column with `columns` (or cast it in `query`).

## Checks

| Check | Applies to | Fix |
|---|---|---|
| Server version below the minimum (table below) | every table | upgrade the server; the backend is never switched silently |
| DECIMAL/NUMERIC with scale > 10 or precision > 28, or Postgres/Oracle NUMERIC without precision (ConnectorX loses digits silently) | ConnectorX | `pyarrow`, or declare a precision or cast. Oracle `NUMBER(p, 0)` is exempt |
| Per-dialect deny list: types ConnectorX cannot read, or reads wrongly (Postgres interval, money, timetz, macaddr, cidr, varbit, tsvector, `"char"`, most arrays; SQL Server xml, hierarchyid, geography, geometry, money, datetimeoffset; Oracle raw, rowid, interval, TIMESTAMP WITH (LOCAL) TIME ZONE, nclob, bfile, xmltype, boolean, json). A type SQLAlchemy does not recognise is a finding on every server dialect | ConnectorX | as above |
| `sql_variant` (SQL Server) | every backend | exclude or cast in `query` |
| A column named `_extracted_at` | every backend | rename or exclude it |
| `partition` column missing, not an integer, or case-sensitive on Oracle | partitioned tables | partition on an integer (case-insensitive on Oracle) column, or remove `partition` |

## Minimum server versions

Set from what the integration suite passed on:

| Dialect | Minimum | Evidence |
|---|---|---|
| mssql | 14 (SQL Server 2017) | 2017, 2019, 2022 |
| postgres | 11 | 11, 12, 17 |
| mysql | 8.0 | 8.0, 8.4 |
| oracle | 19 | Oracle Free 23.26 only. **19c and 21c are accepted but unverified**: the integration suite has not run on them (their images do not start on an arm64 host). Run it on an x86_64 host before relying on them |
| db2 | 11.5 | 11.5.5.1 and 12.1.5. 11.1 and 10.5 were not run |
| sqlite | none | |

## Rule details

Why each check exists is in [engines.md](engines.md) (what ConnectorX loses or cannot
read, per engine) and [design.md](design.md#decisions) (D5: compatibility rules are
validation, not runtime fallback). The preflight uses one SQLAlchemy reflection query per
table.

- **Server version** is at or above a minimum per dialect. The minimums are set from what the integration tests prove, not guessed. A server below the minimum fails; the backend is never switched silently. Proven so far (S9): **Postgres 11**, **MySQL 8.0**, **SQL Server 2017** (14), **Oracle 19** (by decision: 19c and 21c are accepted but **unverified**, only 23 has run, see [design.md](design.md#spikes) S9), **Db2 11.5**.
- For tables on ConnectorX:
  - **DECIMAL/NUMERIC with scale > 10 or precision > 28**, and on Postgres and Oracle a **NUMERIC/NUMBER without precision**. This check is mandatory: it is the only thing standing between a table and silent precision loss ([engines.md](engines.md#connectorx-046)). Oracle `NUMBER(p, 0)` is exempt whatever p: ConnectorX reads it as int64, and a wider value fails loudly.
  - Column types on the per-dialect deny list from [engines.md](engines.md#connectorx-046) (Type gaps). A type SQLAlchemy does not recognise
    (it reflects as `NullType`) is a finding on every server dialect: on Postgres and MySQL these are
    exactly the xml, geometric, pg_lsn, composite and geometry types, none of which ConnectorX reads.
    SQLAlchemy reflects Postgres `"char"` and `name` alike (a plain `String`), so both are findings,
    although ConnectorX reads `name`. A `query` table's column types are not reflected, so a type
    ConnectorX cannot read there fails the run with `ConnectorXFailed` instead. SQL Server's CLR
    types (hierarchyid, geography, geometry) are registered with the reflection under their own names,
    so they are named in the finding; `money` and `datetimeoffset` are findings because ConnectorX
    reads them wrongly without an error ([engines.md](engines.md#connectorx-046)). Oracle XMLTYPE and INTERVAL YEAR TO MONTH
    reflect as `NullType` and are caught by the rule for unrecognised types.
- For tables with `partition`: the column exists, is an integer, and on Oracle does not have a case-sensitive (quoted, mixed-case) name, which ConnectorX upper-cases in its range query ([engines.md](engines.md#connectorx-046)). These are table findings like the others, so `validate` reports them too.
- On every backend: a type no backend lands losslessly. On SQL Server that is `sql_variant`: pyodbc
  reads only the bytes of its value, without its type. The fix is to exclude the column or cast it in
  `query`.
- Rows with a NULL cursor are **counted and logged** when `include_null_cursor` is false, so the gap is visible. That count is the run's `null_cursor_rows` and its `NullCursorRows` metric ([errors.md](errors.md#metrics-and-logs)).

