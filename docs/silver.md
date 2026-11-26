# Silver (Spark 4.1 / Glue 6.0)

`silver/silver_transform.py` is a reference for **API** bronze. Each source defines its
fields:

```python
from silver_transform import Field, latest_per_id, read_bronze, transform

FIELDS = [
    Field("status", "$.status", "string"),
    Field("amount", "$.detail.amount.value", "decimal(18,2)"),
    Field("updated_at", "$.updatedAt", "timestamp"),
]
bronze = read_bronze(spark, "s3://bronze/bronze/vendor", "orders_raw", "incremental", dates)
if bronze is not None:
    silver, quarantine = transform(bronze, FIELDS, known_keys=["orderId"])
    current = latest_per_id(silver)  # then MERGE into Iceberg v2 (sketch: merge_into_iceberg)
```

- `transform` parses `payload` with `try_parse_json` and extracts each field with
  `try_variant_get`. VARIANT exists only in memory; Iceberg gets typed columns (v2).
- Rows with a bronze `error`, or a payload Spark cannot parse (e.g. `NaN`), go to
  `quarantine` with a `reason`.
- `_rescued_data` is a JSON object of fields that are present but fail their cast.
  `_unknown_keys` lists top-level keys no field covers. Every row carries `_bronze`:
  file, load id, url, page, index and fetch time.
- Set `spark.sql.session.timeZone=UTC`.

There is no database silver transform: database bronze is already typed, and
[bronze-contract.md](bronze-contract.md) is what a silver job must follow.

Bronze rules a silver job must follow (completeness, manifests, deduplication, backend
changes) are in [bronze-contract.md](bronze-contract.md). Why silver parses into typed
Iceberg v2 columns rather than VARIANT is in
[api/design.md](api/design.md#510-silver).
