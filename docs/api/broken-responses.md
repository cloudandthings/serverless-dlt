# Broken responses and API operations (`api_bronze`)

The rule: **stop when the page is not data; continue when one record is bad.** Why, and
the evidence, is in [design.md](design.md#54-broken-response-policy).

## Policy

| What the API returned | What happens |
|---|---|
| 2xx, but not JSON, empty, truncated, records key missing / `null` / not an array, or the records key duplicated | Raw body + metadata written to `_quarantine/`. The run **fails** with `BrokenPageError`, **nothing** from the run lands (not even earlier good pages), and the cursor does not move, so the next run retries. A page that stays broken fails every run: alarm on it. |
| A record that isn't an object, or whose id or cursor is missing / unparseable | Lands verbatim with `error` set and a null cursor. The run continues. Good records advance the cursor. |
| `NaN` / `Infinity` inside a record | Lands verbatim like any record (`error` null). Spark cannot parse it, so silver quarantines it. |
| Body is valid JSON but not valid UTF-8 | One row for the whole page: bytes in `payload_raw`, `error` set. The run continues. |
| `"data": []` | Normal: no rows. A quiet run writes no files and no load marker. |
| HTTP 4xx / 5xx | dlt retries per its runtime config, then the run fails. Nothing is quarantined. |

Quarantined pages land at
`<quarantine_url>/_quarantine/<pipeline_name>/<endpoint>/year=/month=/day=/<ts>_page<N>.body`
plus `.meta.json` ([../bronze-contract.md](../bronze-contract.md#paths)).

## Operations

- **`BrokenPageError` alarm:** open the quarantined `.body` and `.meta.json`. Common
  causes are an HTML maintenance page, a 200 carrying an error object, or a truncated
  body. The function fails every run until the API recovers; nothing is lost, because
  the cursor has not moved. If the vendor changed the response shape, fix `records_path`
  in the config.
- **Rows with `error`** are expected occasionally. Silver routes them to its quarantine
  table. A sudden rise usually means the vendor changed a field name (`missing id`,
  `bad <cursor>`).
- **A quiet run leaves no trace in bronze** (no rows, no load marker). Monitor runs
  through Lambda metrics, not bronze.
- **Adding a source** needs no code change, unless the API needs a paginator or auth
  that dlt does not have.
