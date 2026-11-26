# The bronze contract

What silver (and anyone else reading bronze) can rely on, for both packages. The
per-package detail is in [api/](api/) and [db/](db/); the reasons are in
[api/design.md](api/design.md) and [db/design.md](db/design.md).

## Paths

```
<bucket_url>/<dataset_name>/<name>/mode=<mode>/year=YYYY/month=MM/day=DD/<load_id>.<file_id>.parquet
<bucket_url>/<dataset_name>/_dlt_loads/<pipeline_name>__<load_id>.jsonl          completed loads
<bucket_url>/<dataset_name>/_dlt_pipeline_state/...                              cursor state (dlt-owned)
<bucket_url>/<dataset_name>/_manifests/<name>/<snapshot_id>.json                 db: full-snapshot manifests
<quarantine_url>/_quarantine/<pipeline_name>/<endpoint>/year=/month=/day=/<ts>_page<N>.body + .meta.json   api: broken pages
```

- `<name>` is the configured resource (api) or table (db) `name`.
- The date is the **load date**, with two exceptions: a backfill (api, or db with a
  timestamp cursor) uses the **window start**, so a backfill window is self-describing
  and can be re-run on its own; a page of a planned db snapshot uses the plan's
  `partition_date`.
- A day with no data has no prefix. Treat a missing path as "nothing to do".
- Parquet is zstd-compressed by default (`parquet_compression`).
- dlt's own tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) ignore the
  layout and sit flat under the dataset.

## Completeness

| Mode | A data file counts when |
|---|---|
| api, any mode; db `incremental`, `backfill` | its `<load_id>` (the part of the file name before the second-to-last dot) has an entry in `_dlt_loads` with `status = 0` |
| db `full` | its load id is listed in a manifest `_manifests/<name>/<snapshot_id>.json` (`{"table", "snapshot_id", "load_ids", "written_at"}`). A single-invocation `full` run writes the manifest itself on success; a paged snapshot gets it from `finalize`. A `full` load without a manifest is an incomplete snapshot and must be ignored. |

Silver must skip any other file: it belongs to an in-flight or failed load and will be
picked up later.

## Append-only, at-least-once

Bronze is **append-only**. Every resource uses `write_disposition="append"`, and there
is deliberately no way to configure `replace`: a `replace` load deletes the whole table
folder, including other `mode=` partitions.

Delivery is **at-least-once**: updated records, overlapping backfill windows, inclusive
API `since` filters and db `lag` windows all re-land rows. Silver deduplicates:

- api: on `record_id` + `updated_at_utc` (latest `updated_at_utc`, then latest
  `fetched_at`, per id).
- db: on (`primary_key`, cursor column), keeping the latest `_extracted_at`. With
  `cursor.lag`, dlt deduplicates nothing, so **every incremental run re-lands every row
  in its lag window** (`[last cursor - lag, ...)`) and identical row versions repeat
  across loads. Without `lag`, dlt deduplicates at the cursor boundary by `primary_key`.
  Merging into silver uses `primary_key`, from the config or the documented table
  metadata.

## State identity

`pipeline_name`, `dataset_name` and `bucket_url` together identify a pipeline's cursor
state. Changing any of them silently restarts every cursor from its `initial_value`.
One concurrent run per pipeline name: two concurrent runs both read state, and the last
writer wins (see [deployment.md](deployment.md#orchestration)).

## API rows (`api_bronze`)

One row per API record.

| Column | Type | Meaning |
|---|---|---|
| `endpoint` | string | Request path as configured |
| `url` | string | Full URL fetched, query included |
| `status` | int32 | HTTP status |
| `content_type` | string | Response `Content-Type` |
| `fetched_at` | timestamp (UTC) | When the page was received |
| `page_seq` | int32 | 1-based page number within this run and endpoint |
| `record_index` | int32 | 0-based position in the page's records array |
| `record_id` | string | Value at `id_path` (null if not configured or bad) |
| `payload` | string | **The record's JSON text, verbatim** |
| `payload_raw` | binary | Whole response body, only when it is not valid UTF-8 (then `payload` is null) |
| `error` | string | Null for good rows; otherwise why the row is suspect |
| `updated_at_utc` | timestamp (UTC) | Parsed cursor; what dlt's incremental tracks. Null on error rows |

`(file, page_seq, record_index)` identifies a row exactly, which is how silver points
back to bronze. The same record lands again when it is updated, when backfill windows
overlap, and when the API's `since` filter is inclusive. Rows with `error` set are
described in [api/broken-responses.md](api/broken-responses.md).

Bronze is verbatim per *record*, not per *response*: top-level response fields
(`pagination`, totals) and the whitespace between records are not kept
([api/design.md](api/design.md#8-limitations-and-risks)).

## Database rows (`db_bronze`)

The source columns, with their source names (case, spaces and dashes kept) and the
backend's types, plus `_extracted_at` (`timestamp[us, UTC]`, the run start). A source
column named `_extracted_at` is rejected. The Parquet type each source type lands as,
per backend and engine, is in [db/types.md](db/types.md).

- **Naive timestamps stay naive.** A source timestamp without a time zone lands as a
  naive Parquet timestamp: no shift, no UTC label. Interpret it with the config's
  `connection.naive_timezone` (default `UTC`). `naive_timezone` converts only cursor
  bounds (`cursor.initial_value`, event window bounds, `plan` output) between UTC and
  the source's wall clock; data values are never touched. Set it to what the source
  database's naive columns mean, or incremental windows will be off by the offset.
- **Types never change within one `name`.** A new source column lands. A type change
  on an existing column (including a decimal precision or scale change) fails the run
  and lands nothing. Fix the source, or land the table under a new `name`.
- **The backend is part of the type.** The two backends produce different Arrow types
  (integer widths, `binary` vs `large_binary`, every decimal). A backend change since
  the last successful run fails with `BackendChanged`, so **switching backend means a
  new table `name`** (a new bronze prefix; silver unions old and new).
