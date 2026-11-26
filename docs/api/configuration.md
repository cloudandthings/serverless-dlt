# API config reference (`api_bronze`)

One JSON document per source = one dlt pipeline = one Lambda function. Unknown keys are
rejected, so typos fail loudly. Configs live at `${CONFIG_URI}/${source_id}.json` (any
fsspec URL) or inline in the event as `config` ([runs.md](runs.md)). Examples:
`examples/vendor_orders.json`, `examples/vendor_countries.json`. JSON Schema:
`schemas/api_bronze.config.schema.json` (see [../testing.md](../testing.md#config-schemas)).

```jsonc
{
  "pipeline_name": "vendor_orders",   // STABLE: part of the state identity
  "dataset_name": "vendor",           // STABLE: part of the state identity; folder under bucket_url
  "bucket_url": "s3://bronze/bronze", // STABLE: changing any of these three silently starts from scratch
  "quarantine_url": null,             // default: bucket_url
  "parquet_compression": "zstd",      // zstd | snappy | gzip | none
  "max_time_s": null,                 // optional cap per run for split loading (see runs.md)
  "client": {
    "base_url": "https://api.vendor.example/v2/",
    "paginator": {"type": "json_link", "next_url_path": "pagination.next"},
    "auth": {"type": "bearer", "token": {"secret_id": "ingest/vendor", "key": "api_token"}},
    "headers": {"Accept": "application/json"}
  },
  "resources": [{
    "name": "orders_raw",             // bronze table name: [a-z][a-z0-9_]*
    "path": "orders",                 // relative to base_url
    "params": {"limit": 500},         // static query params
    "records_path": "data",           // dotted path to the records array; "$" if the body is the array
    "id_path": "orderId",             // dotted path to the record id (optional)
    "paginator": null,                // optional per-endpoint override of client.paginator
    "incremental": {                  // optional; without it the endpoint is a snapshot (mode=full only)
      "cursor_path": "updatedAt",     // dotted path to the cursor in each record
      "cursor_type": "iso8601",       // iso8601 | epoch_s | epoch_ms  (ISO without offset = UTC)
      "initial_value": "2020-01-01T00:00:00Z",
      "start_param": "updated_since", // query param that gets the lower bound
      "end_param": "updated_before",  // query param for the upper bound, if the API has one
      "param_format": "iso8601",      // iso8601 (+00:00) | iso8601_z (Z) | epoch_s | epoch_ms
      "sorted_ascending": false       // true only if the API returns records in ascending cursor order
    }
  }]
}
```

- **State identity.** `pipeline_name`, `dataset_name` and `bucket_url` must stay stable
  ([../bronze-contract.md](../bronze-contract.md#state-identity)).
- **Paginators and auth** use dlt's `rest_api` config vocabulary unchanged
  (`json_link`, `header_link`, `offset`, `page_number`, `cursor`, `header_cursor`,
  `single_page`; auth `bearer`, `api_key`, `http_basic`, `oauth2_client_credentials`).
  See the dlt 1.32 REST API source docs for each type's options. `auto` detection is
  rejected: it can guess wrong silently.
- **Secret references.** Any value under `client.auth` or `client.headers` may be
  `{"env": "VAR"}`, `{"secret_id": "id"}`, `{"secret_id": "id", "key": "json_key"}`,
  optionally with `"format": "Bearer {}"`. Secrets Manager values are cached per
  container; rotation applies from the next cold start.
- **Several resources in one config** share one client and one run. A broken page in
  any of them fails the whole run, so nothing lands for any of them. Give endpoints that
  should fail independently their own config.
- **`sorted_ascending`** enables split loading; set it only when the API really returns
  records in ascending cursor order ([runs.md](runs.md#split-loading)).
