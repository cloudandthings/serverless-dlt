# Design: raw REST API bronze with dlt on Lambda

This records why `api_bronze` works the way it does: the requirement, what dlt
does by default, the options that were compared, the chosen design, and the
evidence behind each choice. Everything marked *verified* was checked by
running code against dlt 1.32.0, pyarrow 26.0.1, Python 3.12 (November 2026).
Most of it is covered by this package's own tests; the rest was measured in a
separate evaluation workspace, and the numbers are reproduced here.

What the package accepts and does is specified in [configuration.md](configuration.md),
[runs.md](runs.md), [broken-responses.md](broken-responses.md) and
[../bronze-contract.md](../bronze-contract.md); this document is the why.

## 1. Requirement

- Ingest REST APIs on **AWS Lambda** (container images) using **dlt**.
- Land a **bronze** layer in S3, which a separate **Glue/Spark** job promotes to
  **Iceberg** silver.
- Bronze is addressed by **path**, not by a catalog:
  `<table>/mode=<full|incremental|backfill>/year=/month=/day=`.
- Bronze must keep what the vendor sent. The original worry was that dlt
  flattens and rewrites nested payloads, which makes bronze useless for audit
  and for reprocessing when parsing rules change.

## 2. What dlt does to an API payload by default (verified)

dlt never writes the raw response, in any file format. Before any file is
written, its normaliser has already:

- **Flattened** nested objects with `__`: `customer.name.first` became
  `customer__name__first`.
- **Split arrays into child tables**, even scalar arrays (`tags`). One
  2-page `orders` endpoint became **8 tables**.
- **Renamed keys** to snake_case, sometimes lossily: `Fabric-Blend %` became
  `fabric_blendx`.
- **Coerced types silently to the first type seen**: a later page's
  `subtotal: 1800` (number) was written as `"1800"`, with no warning and no
  variant column.
- **Rewritten timestamps to UTC**: `2026-11-20T09:42:10.123+02:00` became
  `2026-11-20T07:42:10.123000+00:00`. The original offset is lost.
- **Dropped nulls and empties**: `email: null`, `flags: {}` and `phones: []`
  left no trace.

`max_table_nesting=0` keeps nested values intact as JSON-text columns, but
top-level keys are still renamed and coerced, and timestamps are still
rewritten. Choosing `jsonl` instead of `parquet` changes only the encoding, not
any of the above. dlt also has **no VARIANT type**: `json` columns are plain
strings, dlt rejects Arrow's `pa.json_()`, and pyarrow 26 cannot write Parquet
VARIANT.

## 3. Why bronze keeps text, not parsed values (verified)

Every JSON parser in the path loses something silently, and each one loses
something different:

| Input | requests / simplejson | orjson (dlt's jsonl writer) | stdlib `json` | Spark 4.1 `parse_json` |
|---|---|---|---|---|
| Invalid UTF-8 byte | replaced with U+FFFD | n/a (already replaced) | error (strict decode) | n/a |
| `0.1000000000000000055511151231257827`, `1.10` | float64 `0.1`, `1.1` | | float64 | |
| Duplicate key | last wins, silently | | last wins, silently | **rejected** |
| Integer > int64 | kept | **run fails** | kept | |
| `1e400` | `inf` | **value silently dropped** | `inf` | becomes Infinity |
| Escaped lone surrogate `\ud800` | kept | **run fails** | kept | becomes `?` |
| `NaN` / `Infinity` | **rejected** | | accepted | **rejected** (verified by this package's silver test) |
| UTF-8 BOM | ok | | needs `utf-8-sig` | **rejected** |

So bronze stores the record as **text cut from the original body**. It is never
parsed and re-serialised. Each consumer decides how to parse it, and the
original stays available when a decision turns out to be wrong.

## 4. Options considered

| Option | Raw? | Verdict |
|---|---|---|
| A. dlt standard `rest_api_source`, jsonl or parquet, any nesting setting | No | Rejected: §2. orjson also fails the whole run, permanently, on a big int or lone surrogate. |
| B. Raw `.json.gz` page files written beside dlt by a response hook, plus normal dlt bronze | Full bytes | Rejected as the default: two writers, raw files outside dlt's load markers and state, and silver must explode pages. Kept as an add-on for audit-critical vendors (§8). |
| C. Page-grain envelope: one bronze row per response, the body as a string | Full bytes | Rejected: §5.5 shows it loses records silently in backfills. A page is also the smallest error unit, and silver must explode pages. |
| D. Record-grain envelope in a **JSONL** container | Per record | Rejected: same code as E, but worse in every measured way (§5.8). |
| E. Native JSONL, one vendor record per line | Close, but not verbatim | Rejected: dlt cannot write it (orjson re-serialises), so it needs a second writer. Metadata has nowhere to go, pretty-printed records must be compacted, and it still needs the splitter. |
| **F. Record-grain envelope in Parquet (chosen)** | **Per record** | Raw per record. dlt keeps pagination, state, layout and load markers. Correct per-record incremental filtering. |

The extra code F needs beyond the simplest raw option (C) is the splitter, about
110 lines. The envelope itself and the broken-response policy are needed by any
raw-plus-dlt-state option. The policy is needed even on dlt's standard path,
because of the empty-page behaviour in §5.4.

## 5. The design

### 5.1 Arrow items bypass the normaliser

The resource yields `pyarrow.Table`s with a fixed schema ([../bronze-contract.md](../bronze-contract.md#api-rows-api_bronze)).
dlt writes Arrow items to Parquet as given: no flattening, renaming, coercion
or timestamp rewriting. It still applies incremental filtering on the cursor
column, writes the layout, and records `_dlt_loads` and state. The REST client
is used directly (`RESTClient.paginate`) rather than through the declarative
`rest_api_source`. Paginator and auth configs still use dlt's own factories, so
config documents keep dlt's vocabulary.

### 5.2 Verbatim record splitting

`json.JSONDecoder.raw_decode(text, i)` parses one value starting at `i` and
returns where it ended. The splitter walks to the records array, then calls
`raw_decode` once per element, and stores `text[start:end]`: the element's
exact text, including whitespace, key order, escapes and number literals. The
parsed value is a by-product, used for `record_id` and the cursor, so each
record is parsed once.

- *Verified:* records containing every case in §3's table come out
  byte-identical.
- A **duplicated records key** (`{"data": [...], "data": [...]}`) raises an
  error instead of guessing. `json.loads` would silently take the last one.
- The body is decoded with strict UTF-8. If it is not valid UTF-8, the page
  cannot be sliced as text, so it lands as one row with the bytes in
  `payload_raw`.

### 5.3 One parser for pagination and splitting

RESTClient's data selector and every paginator call `response.json()`, which is
requests' simplejson. simplejson rejects `NaN`, while the splitter's stdlib
`json` accepts it. Before the fix, **one `NaN` in one record made the whole page
"not JSON"**: the page was quarantined and the run failed, and it failed again
on every run until the vendor changed the data (verified). The response hook now
replaces `response.json` with a stdlib parse of the same bytes. With one parser,
a page is valid for both pagination and splitting, or for neither. That parse
only drives pagination; the stored text is still sliced from
`response.content`.

### 5.4 Broken-response policy

The rule: **stop when the page is not data; continue when one record is bad.**

- **Broken page** (non-JSON, empty, truncated, records key missing, `null`, not
  a list, or duplicated): the raw bytes go to `_quarantine/` with metadata, and
  the run fails. Nothing from the run lands, including earlier good pages,
  because dlt commits a load atomically. The cursor holds, so the next run
  retries. *Verified.*
  - Failing is deliberate: skipping a page would advance the cursor past
    records that were never seen.
  - dlt's RESTClient turns a missing or `null` records key into an **empty
    page with only a WARNING**. Without this policy, a 200 `{"error": "rate
    limited"}` looks like a quiet day.
- **Bad record** (not an object, id or cursor missing or unparseable): it lands
  verbatim with `error` set and a null cursor. Only parseable cursors advance
  state. *Verified.*
- **Multiple endpoints in one config** share one run: a broken page in any of
  them stops all of them from landing. *Verified.*

### 5.5 Record grain fixes a silent backfill loss (verified)

With one row per page and `cursor = max(updatedAt on the page)`, dlt's
`end_value` filter drops a **whole page** whose maximum lies past the window
end. That silently loses the in-window records on the page that straddles the
boundary, when the API does not receive the upper bound. At record grain, dlt
filters each record. The same scenario kept exactly the in-window records
(tested: `test_backfill_without_end_param_keeps_exactly_the_window`).

### 5.6 Cursor rules (verified)

- The cursor column is the **parsed, UTC** timestamp. Comparing ISO strings
  with mixed offsets picks the wrong maximum: `23:59+02:00` sorts after
  `22:30Z` as text, but it is 21:59Z. ISO values without an offset are taken
  as UTC.
- `dlt.sources.incremental(...)` must be the resource argument's **default
  value**. Building it in the function body, the usual fix for ruff B008,
  silently disables state and filtering, and every run then re-fetches
  everything.
- `primary_key=()` turns off dlt's boundary dedup, so a record equal to the
  last cursor re-lands when the API's `since` is inclusive. That is intended:
  silver dedups. With an exclusive `since`, a record that later appears with
  exactly the last timestamp is missed. Prefer inclusive.
- Split loading (`add_limit(max_time)`) stops between pages. It is enabled only
  for APIs declared `sorted_ascending`, because otherwise later pages could
  hold older records that the advanced cursor would then skip.

### 5.7 Modes, layout and append-only (verified)

- Layout: `{table_name}/mode={mode}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}`.
  dlt allows only `{schema_name}` before `{table_name}`, so `mode` cannot be
  placed above the table.
- **`replace` deletes sibling partitions.** A `replace` load with `mode=full`
  deleted the whole `mode=incremental` partition, because dlt truncates
  everything under the table prefix. Every mode therefore appends, and
  `replace` cannot be configured.
- `full` and `backfill` use `end_value`. That makes dlt's incremental
  stateless: the cursor is neither read nor advanced, and windows are
  half-open `[start, end)`. Backfills date their partition by the window start
  (`current_datetime`), so a window is self-describing.
- dlt's own tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) ignore
  the layout and sit flat under the dataset. That keeps state restore a single
  listing.

### 5.8 Container: Parquet + zstd, not JSONL (measured)

Same resource; only `loader_file_format` differs. Both containers keep payloads
verbatim and restore state. JSONL **double-escapes** the payload
(`"payload":"{\"id\": ...`), so it is no more readable. It turns timestamps into
strings, **base64-encodes** `payload_raw` silently, and cannot prune columns.

100,000 records of about 1 KB (134 MB of JSON), local disk:

| Container | Run | On disk | Read needed to filter on `record_id`/`error`/cursor |
|---|---|---|---|
| Parquet, snappy (dlt default) | 4.1 s | 11.1 MB | 0.70 MB |
| **Parquet, zstd (chosen)** | 4.2 s | **2.4 MB** | **0.24 MB** |
| JSONL, gzip (dlt default) | 6.1 s | 3.7 MB | 3.73 MB (all of it) |

The synthetic data is repetitive, so real files will be larger. Compare the rows
with each other, not with real-world sizes.

### 5.9 State on Lambda

- State lives in bronze (`_dlt_pipeline_state`), keyed by **pipeline name +
  bucket + dataset**. Changing any one of them silently starts from scratch,
  so all three are marked STABLE in the config.
- Every invocation **drops the local working directory** and restores from S3.
  A warm container then behaves exactly like a cold one, and a half-finished
  package left in `/tmp` by a failed run is never replayed in place of a fresh
  extract.
- Restore takes the newest state attached to a **completed** load, so a failed
  run cannot advance the cursor. *Verified across cold starts, server errors
  and broken pages.*
- **One concurrent run per pipeline name.** Two concurrent runs both read the
  same state and the last writer wins. Backfills do write state objects (the
  cursor is unchanged in them), so parallel backfills need their own pipeline
  name.

### 5.10 Silver

- Glue 6.0 (Spark 4.1): `try_parse_json(payload)`, then `try_variant_get` each
  field into a typed column. VARIANT exists only in memory.
- Iceberg gets typed columns in **v2**. VARIANT would need Iceberg v3, and
  Athena cannot read Iceberg v3 tables written by Glue 6.0 (checked ad hoc).
  Glue 5.x (Spark 3.5) has no VARIANT at all. Plain Parquet *can* carry VARIANT
  (DuckDB 2.x writes it and Spark 4.1.1 reads it), but dlt cannot write it, so
  bronze stays text.
- **Completeness** comes from `_dlt_loads`: only files whose load id is listed
  with `status = 0` are read.
- Bronze rows with `error`, and payloads Spark cannot parse, go to a quarantine
  table with a reason.
- Fields present but failing their cast go to `_rescued_data`. Top-level keys
  not covered by the spec are listed in `_unknown_keys`.
- Every row carries a `_bronze` lineage struct. Dedup: latest
  `updated_at_utc`, then latest `fetched_at`, per id. Then MERGE with `>=` on
  the cursor, so a replay never overwrites newer data with older.

## 6. Cost (measured, Apple M2, single thread)

| Page | stdlib parse (RESTClient's, per page) | Verbatim split, incl. its parse | Per record |
|---|---|---|---|
| 100 records / 0.13 MB | 0.5 ms | 1.1 ms | ~11 µs |
| 1,000 / 1.3 MB | 5.5 ms | 10.9 ms | ~11 µs |
| 10,000 / 13 MB | 67 ms | 182 ms | ~18 µs |

End to end, 100k records to local Parquet: **2.7 s** for the record envelope
vs **7.1 s** for dlt's standard normaliser (4 tables). The split costs about two
parses, but skipping normalisation more than pays for it.
`scripts/bench_split.py` reproduces the split numbers on any machine.

## 7. dlt gotchas found along the way

- The incremental must be the argument's **default value** (§5.6).
- `RESTClient` turns a missing or `null` records key into an **empty page**,
  with only a warning (§5.4).
- Passing `hooks` to `RESTClient.paginate` **replaces** the default
  `raise_for_status`. The hook must call it itself.
- requests raises its own `JSONDecodeError` (simplejson-based), which is **not**
  a `json.JSONDecodeError`. Catch both.
- `replace` truncates **all** partitions under the table prefix (§5.7).
- `end_value` windows are exact only at record grain, or when the API also
  receives the upper bound (§5.5).
- `dlt.pipeline(restore_from_destination=False)` as a keyword argument is
  silently ignored (dlt issue 4584). It is not used here: restore must stay on.
- dlt's default Parquet codec is snappy. zstd is set through
  `DATA_WRITER__COMPRESSION`.
- dlt wraps exceptions from a resource in `PipelineStepFailed`. The handler
  re-raises `BrokenPageError` itself, so the Lambda `errorType` is meaningful.

## 8. Limitations and risks

- **Bronze is verbatim per record, not per response.** Top-level response
  fields (pagination, totals, request ids) and the whitespace between records
  are not stored. If an audit ever needs "the exact bytes the vendor sent",
  add a raw zone for those vendors only: a response hook that writes
  `response.content` to `raw/<source>/<endpoint>/year=/month=/day=/...json.gz`
  (option B). It needs no second API call. A cheaper middle ground is a
  per-row `response_sha256`, which proves which response a record came from
  but cannot rebuild it.
- **dlt is pinned at 1.32.0.** The envelope depends on RESTClient behaviour
  (hook semantics, `response.json()` call sites) that is not a public contract.
- **Memory:** a page is held several times at once (bytes, decoded text, parsed
  objects, Arrow table). Not yet measured on Lambda with large pages.
- **Exclusive-`since` APIs** can miss records that tie on the boundary timestamp
  (§5.6).
- **A quiet run leaves no trace in bronze** (no rows, no load marker). Monitor
  runs through Lambda metrics, not bronze.
- Untested so far: real S3, a real Lambda, Glue 6.0, the Iceberg MERGE.

## 9. What the tests prove

| Test file | Covers |
|---|---|
| `test_split.py` | Byte-identical slices for every §3 edge case, BOM, root arrays, dotted paths, duplicate records key, unusable shapes |
| `test_policy.py` | Each broken-page shape → quarantine (byte-identical), run fails, nothing lands, cursor held. Page 2 broken discards page 1, and the rerun recovers. 5xx. Bad records inline. NaN / Infinity. Invalid UTF-8. Quiet run. A broken resource blocks its siblings |
| `test_modes.py` | Cold-start restore, mixed offsets, updates re-land, layout path, zstd, row metadata, backfill stateless + bounded + window-dated, backfill without an upper bound keeps exactly the window, full mode, snapshot endpoints, split loading converges, time limit ignored when unsorted, epoch cursors, auth from a secret ref |
| `test_handler_and_config.py` | Config from `CONFIG_URI`, inline backfill, `BrokenPageError` surfaced as itself, unsafe `source_id`s, strict config and event validation |
| `test_silver.py` | Completeness filter (incomplete load excluded), typed fields, offset-aware timestamps, rescued cast failures, unknown keys, Spark's NaN rejection → quarantine, bronze errors → quarantine, latest-per-id, missing prefix |
| `scripts/smoke_image.py` | The built Lambda image via the emulator: incremental, quiet rerun with state from bronze, broken page → `BrokenPageError` + quarantine |
