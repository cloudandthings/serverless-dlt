# Database events, modes and chunking (`db_bronze`)

How a run is selected and what it reads. Config keys are in
[configuration.md](configuration.md); where each mode's files land, and when they
count, is in [../bronze-contract.md](../bronze-contract.md).

The Lambda event selects the config and the mode. Unknown event keys fail with
`ConfigError`, and a key that belongs to another kind of event is rejected too, so a
mistyped event fails instead of being ignored.

| Key | Used by |
|---|---|
| `source_id` | Every event (unless `config` is given): config file `${CONFIG_URI}/<source_id>.json`; a plain name, no `/`, not starting with `.`. |
| `config` | Every event: an inline config instead of `source_id`. |
| `mode` | `incremental` (default), `full`, `backfill`, `validate`, `plan`, `finalize`. |
| `tables` | Every event except `finalize`: a non-empty list of table names to run; unknown names fail. `finalize` names its one table in `table` instead, and rejects `tables`. |
| `window_start`, `window_end` | `backfill` (both required, end after start; both integers or both ISO-8601, both with an offset or neither). |
| `plan` | `plan`: `snapshot` or `backfill`. |
| `page_size` | `plan: snapshot`: positive rows per page. |
| `windows`, `window_size` | `plan: backfill`: exactly one. `windows` is how many; `window_size` is how wide (seconds, or an integer delta). |
| `page`, `snapshot_id`, `partition_date` | One page of a planned snapshot (`mode: full`): all three together, with exactly one table in `tables`. `page` is `{"offset": N, "limit": N or null}`; `partition_date` is `YYYY-MM-DD`. |
| `table`, `load_ids` | `finalize`: the table, and every page's load ids. `snapshot_id` too; `tables` is not allowed. |

| Event | Behaviour | Path / state |
|---|---|---|
| `{"source_id": "x"}` | **incremental**: `[last cursor - lag, ...)`. First run starts at `initial_value`. Snapshot tables must be left out with `tables`. | `mode=incremental/<load date>`; cursor advances |
| `{"mode": "full"}` | **full**, one invocation: `[initial_value, run start)` for cursor tables, everything for snapshot tables. Writes the manifest on success. | `mode=full/<load date>`; cursor untouched |
| `{"mode": "backfill", "window_start": ..., "window_end": ...}` | **backfill**: `[start, end)`, ISO-8601 or integer to match the cursor. Stateless. May use `partition`. | `mode=backfill/<window start date>`; cursor untouched |
| `{"mode": "validate"}` | Static checks plus preflight; nothing extracted or written. Returns `ok`, `server_findings` and per-table `findings`, `backend`, `null_cursor_count`. Use it when onboarding a source ([preflight.md](preflight.md)). | none |
| `{"mode": "plan", "plan": "snapshot", "page_size": N}` | `COUNT(*)` per table, then `pages: [{table, offset, limit}]` (the last open-ended), a fresh `snapshot_id` and `partition_date`. Needs `primary_key` or keyset `order_by`. | none |
| `{"mode": "plan", "plan": "backfill", "windows": N}` | The cursor's `MIN`/`MAX` split into half-open, contiguous `windows: [{table, window_start, window_end}]`, timestamps in UTC. The last window ends just past MAX (`+1`, or `+1 minute`). Cursor tables only. | none |
| `{"mode": "full", "tables": ["t"], "page": {...}, "snapshot_id": ..., "partition_date": ...}` | One page, ordered by the primary key (or keyset `order_by`). Writes no manifest. | `mode=full/<partition_date>` |
| `{"mode": "finalize", "table": "t", "snapshot_id": ..., "load_ids": [...]}` | Writes `_manifests/<t>/<snapshot_id>.json`, which makes the pages count for silver. | manifest |

Run-start checks (`ConfigError`): a snapshot table cannot run in `incremental`,
`backfill` or `plan: backfill` (leave it out with `tables`); `partition` is rejected in
`incremental` mode, because partition batches interleave and rows do not arrive in
cursor order; paging needs a `primary_key` or keyset `order_by`.

The result of a run reports `rows` per table, `load_ids`, `backends`,
`null_cursor_rows`, `snapshot_id`, `is_empty` and `time_limited`. A paged snapshot is
orchestrated by a Step Function: [examples/db_snapshot.asl.json](../../examples/db_snapshot.asl.json)
runs `plan`, a `Map` over the pages, then `finalize` with the collected `load_ids`.

### Time budget and re-invocation

An incremental run gets a budget of Lambda's remaining time minus `SAFETY_MARGIN_S`
(default 120, never under 60 s); `max_time_s` caps it further. The result says
`time_limited`; the orchestrator re-invokes while `is_empty` is false.

| Table | What happens at the budget |
|---|---|
| keyset | stops **between pages**; no query is left running |
| traditional-backend `stream` (Postgres, SQL Server, Oracle, Db2) | dlt cuts the stream; closing the cursor ends the query |
| ConnectorX `stream`, or MySQL `stream` on the traditional backend | **never cut**: a dropped ConnectorX stream leaves its query running on the server with the connection open, and closing PyMySQL's cursor blocks until the query ends. The Lambda budget skips these tables (logged), and `max_time_s` is rejected with them. A table that does not fit one invocation must use keyset |

## Chunking: stream, keyset, or offset paging

- **`stream`** (default): one query, ordered by the cursor, batches streamed. It holds a
  snapshot or locks for the query's whole duration, and cannot be split across
  invocations on ConnectorX or MySQL.
- **`keyset`**: repeated short queries, each an index range scan seeded by the last row
  of the previous page (`order_by` = cursor, then primary key). Use it for big tables,
  for time-budgeted split loading, and for Oracle tables larger than memory. It cannot
  check that the `order_by` columns are indexed: **ask the DBA to confirm an index on
  exactly those columns, in that order**, or every page is a scan.
- **Offset paging** (`plan: snapshot` + page events) is for big **snapshots** that need a
  Step Function. Caveats: OFFSET is **not consistent across invocations**, so rows that
  change while a snapshot runs can be duplicated or missed; and deep offsets get slower.
  Prefer keyset wherever a unique sortable key exists; use paging only when the table
  has no usable key, or its key is not indexed. Ask the DBA to confirm the `primary_key`
  (or `order_by`) is indexed, since every page sorts by it.

Paged snapshots and parallel backfill windows run under their own function and
`pipeline_name` (see [../deployment.md](../deployment.md#orchestration)).

### Partitioned reads

`partition: {"column": ..., "num": N}` reads with N parallel ConnectorX connections.
Opt-in, never automatic. The column must be an integer column; the config must resolve
to `connectorx` and `chunking: stream`; it is rejected in `incremental` mode. On Oracle
the column must be case-insensitive (stored upper case, written lower case in the
config), because ConnectorX upper-cases it. On SQL Server it cannot be combined with
`isolation_level`.

## Duplicates from `lag`

With `cursor.lag`, dlt deduplicates nothing, so **every incremental run re-lands every
row in its lag window** (`[last cursor - lag, ...)`). Identical row versions repeat
across loads. Silver removes them: deduplicate on (`primary_key`, cursor column) and
keep the latest `_extracted_at`. Without `lag`, dlt deduplicates at the cursor boundary
by `primary_key`.

## NULL cursors

A row whose cursor is NULL never matches the cursor filter. The preflight counts them
(within `where`), and every run reports that count as `null_cursor_rows` and the
`NullCursorRows` metric, as `validate` does as `null_cursor_count`, so the gap is
visible although those rows never land. `cursor.include_null_cursor: true` adds
`OR cursor IS NULL`, which re-reads those rows on every run; `NullCursorRows` then
counts the NULL-cursor rows the run landed.
