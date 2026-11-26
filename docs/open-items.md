# Open items

What has not been validated yet, across both packages.

1. Run against **real S3** and a **real Lambda** (db: in a VPC against production
   databases): IAM minimal set, s3fs credentials from the function role, timing,
   cold-start import time. The Terraform has not been applied to a real account.
2. **Lambda memory**: api with the largest realistic page, db with peak RSS on the
   largest real table; set the function memory from the measurement.
3. **Glue 6.0** run of the silver transform (tested locally on pyspark 4.1.1 only), the
   **Iceberg v2 MERGE** (untested sketch), and Glue reading typed db bronze.
4. **Audit requirement (api):** bronze is verbatim per *record*, not per *response*.
   Top-level response fields (`pagination`, totals) and the whitespace between records
   are not kept. If exact response bytes are ever needed for a dispute, add a separate
   raw zone for those vendors only (see [api/design.md](api/design.md#8-limitations-and-risks)).
5. Silver: decide the Iceberg table layout, MERGE cadence, and the quarantine table's
   retention. There is no database silver transform yet.
6. S3 lifecycle rules for `_dlt_loads`/`_dlt_pipeline_state`: one small object each per
   run.
7. Database versions accepted but not run: **Oracle 19c/21c** (images do not start on
   arm64; run the suite on x86_64, see [db/design.md](db/design.md#spikes) S9) and **Db2 11.1/10.5**.
8. **Kerberos** database auth is a placeholder ([db/kerberos.md](db/kerberos.md)).
