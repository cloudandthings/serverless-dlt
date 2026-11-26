# Landed types (`db_bronze`)

What each source type becomes in bronze Parquet, per backend and engine. The rules
silver relies on (source names kept, naive timestamps stay naive, types never change
within one `name`, the backend is part of the type) are in
[../bronze-contract.md](../bronze-contract.md#database-rows-db_bronze). Which columns
the preflight refuses on ConnectorX is in [preflight.md](preflight.md).

## Conversions on the `pyarrow` backend

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

### Oracle

On **Oracle** (*integration*): `NUMBER` without precision lands as its exact text
(SQLAlchemy reflects it as a float type; reflection asks for Decimals instead).
`NUMBER(19..38)` lands as `decimal128(p, 0)`, wider than int64. **TIMESTAMP WITH
TIME ZONE** is selected through `SYS_EXTRACT_UTC` and lands as `timestamp[us, UTC]`
holding the instant: the driver hands it over as the wall time with the offset
dropped, so `08:00 +02:00` would otherwise land as 08:00 UTC. TIMESTAMP WITH LOCAL
TIME ZONE lands naive, in the session time zone (UTC on Lambda). INTERVAL DAY TO
SECOND lands as Python's text (`1 day, 2:00:00`), INTERVAL YEAR TO MONTH as Oracle's
(`1-2`), BFILE as its locator `<directory>/<file name>`, and 23ai JSON compactly
with its numbers' own digits. The [type matrix](#type-matrix-integration-24-nov-2026) lists every type.

### Db2

On **Db2** (*integration*, pyarrow only): DECIMAL lands as `decimal128(p, s)` with
every digit, once the engine reads Decimals natively. DECFLOAT lands as its exact
text (no Arrow decimal holds DECFLOAT(34), NaN or Infinity), DOUBLE as double, and
BINARY, VARBINARY and CHAR/VARCHAR **FOR BIT DATA** as binary: the reflection
types FOR BIT DATA as text, so the catalog (code page 0) names those columns, and
landed as text their bytes would fail the run unless they happened to be UTF-8.
TIMESTAMP(12) is truncated to the microsecond. XML lands as the driver serializes
it, with a byte-order mark and an `encoding="UTF-16"` declaration.

### Naive timestamps

Naive source timestamps stay naive in bronze ([design.md](design.md#decisions) D2): every timestamp column the
reflection reports as without time zone gets the resource column hint
`{"timezone": false}`. Without the hint dlt labels them UTC without shifting
them, which is the wrong instant whenever `naive_timezone` is not UTC ([design.md](design.md#spikes) S5).

## Type matrix (*integration*, 24 Nov 2026)

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
([design.md](design.md#decisions) D6).
