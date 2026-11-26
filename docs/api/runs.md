# API run modes and events (`api_bronze`)

The Lambda event selects the config and the run mode. Config keys are in
[configuration.md](configuration.md); where each mode's files land is in
[../bronze-contract.md](../bronze-contract.md#paths).

| Event | Mode | Window | Cursor state | Path |
|---|---|---|---|---|
| `{"source_id": "x"}` | incremental | `[last cursor, ...)`; `initial_value` on the very first run | Read and advanced | `mode=incremental/<load date>` |
| `{"source_id": "x", "mode": "full"}` | full | `[initial_value, run start)` | Untouched | `mode=full/<load date>` |
| `{"source_id": "x", "mode": "backfill", "window_start": "...", "window_end": "..."}` | backfill | `[window_start, window_end)` | Untouched | `mode=backfill/<window start date>` |
| `{"config": {...}, ...}` | any | | | Inline config instead of `source_id`, for tests and ad hoc runs |

Sample events: `examples/events.json`.

- Endpoints without `incremental` can only run as `mode=full`.
- **Upper bounds.** When the API has an `end_param`, the window is sent to it. Without
  one, the API returns everything after the start and records at or past the end are
  dropped client-side. That is correct (filtering is per record) but fetches more.
- **Cursor ties.** If the API's `since` is **exclusive**, a record with exactly the last
  landed timestamp that arrives later (or sits on the next page when a split-loading run
  is cut) is missed. Prefer inclusive `since`; the re-landed duplicate is removed by
  silver.

## Split loading

If every resource is `sorted_ascending`, an incremental run stops extracting when the
time budget runs out (the Lambda's remaining time minus `SAFETY_MARGIN_S`, or
`max_time_s` if smaller). It stops between pages, and the cursor advances to what
landed. The orchestrator re-invokes while `is_empty` is false
([../deployment.md](../deployment.md#orchestration)). For unsorted APIs there is no
limit: a run must finish inside the Lambda timeout, because stopping early would skip
older records on later pages.

## Result and failures

The handler returns:

```json
{"pipeline_name": "vendor_orders", "mode": "incremental", "window_start": null, "window_end": null,
 "load_ids": ["1795437864.2080688"], "rows": {"orders_raw": 4}, "is_empty": false,
 "time_limited": false, "started_at": "2026-11-23T12:44:24.174+00:00"}
```

Failures raise, so the invocation is marked failed and the Lambda `Errors` metric fires.
A broken page surfaces as `errorType: "BrokenPageError"` with the quarantine path in the
message ([broken-responses.md](broken-responses.md)).
