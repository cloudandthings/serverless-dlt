# Errors, metrics and logs (`db_bronze`)

## Errors

The handler unwraps these from dlt's `PipelineStepFailed`, so the Lambda `errorType` is
the class name. Nothing lands and cursor state holds for every one of them (state only
persists with a successful load).

| `errorType` | When | Fix |
|---|---|---|
| `ConfigError` | Static validation (including `backend: sqlalchemy`, not built yet), or a bad event (unknown key, bad mode, a snapshot table in a cursor mode, partition in `incremental`, paging without a key, unknown table in `tables`). The message says how to fix it | Fix the config or event |
| `PreflightError` | Reflection checks failed ([preflight.md](preflight.md)); also a bad `partition` column. Metric `PreflightFailed` | `"backend": "pyarrow"` on the table, exclude the column with `columns`, cast it in `query`, or upgrade the server |
| `ConnectorXFailed` | ConnectorX raised at any point, including mid-stream and for a `query` table's unreadable types. Metric `ConnectorXFailed` | `"backend": "pyarrow"` on that table. This changes bronze types, so the next run fails with `BackendChanged` unless the table lands under a new `name` |
| `BackendChanged` | The table's resolved backend differs from the last successful run's. Fails before extraction. Metric `BackendChanged` | Land the table under a new `name` (silver unions old and new), or set `backend` back to the previous one |
| `NotImplementedError` | `auth.type: kerberos` | Use `password` (see [kerberos.md](kerberos.md)) |
| `PipelineStepFailed` | dlt wrapped an error that is not one of ours. A source column type change on an existing bronze column surfaces as this, with `DataValidationError` as its cause | Restore the column's type, or land the table under a new `name` |
| `ValueError` | The event has neither an inline `config` nor a plain `source_id` | Fix the event |
| `KeyError` | A secret `{"env": ...}` reference names an unset environment variable, or `CONFIG_URI` is unset | Set the variable |

## Metrics and logs

EMF metrics (namespace `db_bronze`, dimensions `pipeline` and `table`): `RowsExtracted`
and `NullCursorRows` after each successful run, and one of `PreflightFailed`,
`ConnectorXFailed`, `BackendChanged` (value 1) when that error ends a run. Logs are
JSON: one `run start`, one `run done` carrying rows and the backend per table.
