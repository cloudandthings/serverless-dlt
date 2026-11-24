"""Bronze -> silver reference transform for Spark 4.1 (AWS Glue 6.0).

STATUS
  read_bronze, completed_load_ids, transform, latest_per_id:
      TESTED locally with pyspark 4.1.1 against bronze written by api_bronze
      (tests/test_silver.py). Not yet run on Glue 6.0 itself.
  merge_into_iceberg, main:
      UNTESTED SKETCH. No Iceberg catalog was available. Review before use.

What it does
  1. Reads one bronze table for a mode and a list of dates (paths, no catalog).
  2. Keeps only files whose load id is in `_dlt_loads` (status 0): files from an
     in-flight or failed load are skipped and picked up by a later run.
  3. Parses `payload` with try_parse_json (VARIANT, in memory only) and pulls
     the configured fields out as typed columns with try_variant_get.
  4. Splits rows:
       silver      parsed, no bronze `error`
       quarantine  bronze `error` set (bad record, invalid UTF-8 page) or Spark
                   could not parse the payload (e.g. NaN/Infinity tokens)
  5. Adds lineage: `_bronze` struct (file, load id, url, page, index, fetched_at)
     on every row, so any silver or quarantine row leads back to its exact bytes.
  6. `_rescued_data`: JSON object of configured fields that are present but fail
     their cast, as the original JSON value. `_unknown_keys`: top-level keys
     not covered by the field spec (their values stay in bronze).
  7. latest_per_id: one row per id (highest cursor, then latest fetch), because
     bronze is at-least-once: updates, backfill overlap and inclusive `since`
     APIs all re-land records.

No VARIANT column is written to Iceberg: VARIANT needs Iceberg v3, and Athena
cannot read v3 tables written by Glue 6.0. Typed columns in Iceberg v2 instead.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOAD_ID_FROM_FILE = r"/([0-9]+\.[0-9]+)\.[^/]+\.parquet$"  # {load_id}.{file_id}.parquet


@dataclass(frozen=True)
class Field:
    name: str  # silver column name
    path: str  # JSON path into the record, e.g. "$.detail.amount.value"
    type: str  # Spark SQL type, e.g. "string", "decimal(18,2)", "timestamp"

    @property
    def top_key(self) -> str:
        return self.path.removeprefix("$.").split(".")[0].split("[")[0]


def _exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def bronze_paths(dataset_root: str, table: str, mode: str, dates: Iterable[date]) -> list[str]:
    root = dataset_root.rstrip("/")
    return [f"{root}/{table}/mode={mode}/year={d:%Y}/month={d:%m}/day={d:%d}" for d in dates]


def completed_load_ids(spark: SparkSession, dataset_root: str) -> DataFrame:
    """Load ids dlt marked complete. Written by dlt only when a load finishes."""
    path = f"{dataset_root.rstrip('/')}/_dlt_loads"
    if not _exists(spark, path):
        return spark.createDataFrame([], "load_id string")
    return spark.read.json(path).where(F.col("status") == 0).select("load_id").distinct()


def read_bronze(
    spark: SparkSession, dataset_root: str, table: str, mode: str, dates: Sequence[date]
) -> DataFrame | None:
    """Bronze rows from complete loads only, with `_file` and `_load_id`. None if nothing to do."""
    paths = [p for p in bronze_paths(dataset_root, table, mode, dates) if _exists(spark, p)]
    if not paths:
        return None  # a day with no data has no prefix: nothing to do, not an error
    df = (
        spark.read.option("pathGlobFilter", "*.parquet")
        .parquet(*paths)
        .withColumn("_file", F.col("_metadata.file_path"))
        .withColumn("_load_id", F.regexp_extract("_file", LOAD_ID_FROM_FILE, 1))
    )
    done = completed_load_ids(spark, dataset_root).withColumnRenamed("load_id", "_load_id")
    return df.join(F.broadcast(done), "_load_id", "left_semi")


def _bronze_ref() -> Column:
    return F.struct(
        F.col("_file").alias("file"),
        F.col("_load_id").alias("load_id"),
        "url",
        "page_seq",
        "record_index",
        "fetched_at",
    )


def transform(
    bronze: DataFrame, fields: Sequence[Field], known_keys: Sequence[str] = ()
) -> tuple[DataFrame, DataFrame]:
    """(silver, quarantine). Silver has the typed fields plus id, cursor, lineage, rescue columns.

    `known_keys`: top-level record keys used without a Field, e.g. the id key that
    bronze already extracted into `record_id`. They are not reported as unknown.
    """
    v = F.try_parse_json("payload")
    df = bronze.withColumn("_v", v).withColumn("_bronze", _bronze_ref())

    quarantine = df.where(F.col("error").isNotNull() | F.col("_v").isNull()).select(
        F.coalesce(F.col("error"), F.lit("silver: payload is not valid JSON for Spark")).alias("reason"),
        "record_id",
        F.col("updated_at_utc").alias("_cursor"),
        "payload",
        "payload_raw",
        "_bronze",
    )

    good = df.where(F.col("error").isNull() & F.col("_v").isNotNull())
    typed = [F.try_variant_get("_v", f.path, f.type).alias(f.name) for f in fields]
    raw = [F.variant_get("_v", f.path, "variant") for f in fields]
    failed = [
        F.when(
            r.isNotNull() & ~F.is_variant_null(r) & F.try_variant_get("_v", f.path, f.type).isNull(),
            F.struct(F.lit(f.name).alias("k"), F.to_json(r).alias("v")),
        )
        for f, r in zip(fields, raw, strict=True)
    ]
    rescued = F.filter(F.array(*failed), lambda x: x.isNotNull())
    known = sorted({f.top_key for f in fields} | set(known_keys))
    unknown = F.array_except(
        F.coalesce(F.json_object_keys("payload"), F.array().cast("array<string>")), F.array(*[F.lit(k) for k in known])
    )
    silver = good.select(
        F.col("record_id").alias("id"),
        F.col("updated_at_utc").alias("_cursor"),
        *typed,
        F.when(F.size(rescued) > 0, F.to_json(F.map_from_entries(rescued))).alias("_rescued_data"),
        F.when(F.size(unknown) > 0, unknown).alias("_unknown_keys"),
        "_bronze",
    )
    return silver, quarantine


def latest_per_id(silver: DataFrame) -> DataFrame:
    """One row per id: highest cursor wins, ties go to the latest fetch."""
    w = Window.partitionBy("id").orderBy(F.col("_cursor").desc_nulls_last(), F.col("_bronze.fetched_at").desc())
    return silver.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ----------------------------------------------------------------- UNTESTED below this line
def merge_into_iceberg(spark: SparkSession, silver: DataFrame, table: str) -> None:
    """UNTESTED SKETCH. Upsert into an Iceberg v2 table keyed on `id`.

    `>=` lets an identical replay overwrite itself harmlessly and never lets an
    older version (backfill, late page) overwrite a newer one.
    """
    silver.createOrReplaceTempView("_silver_batch")
    spark.sql(f"""
        MERGE INTO {table} t
        USING _silver_batch s
        ON t.id = s.id
        WHEN MATCHED AND (t._cursor IS NULL OR s._cursor >= t._cursor) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main(argv: Sequence[str] | None = None) -> None:  # pragma: no cover
    """UNTESTED SKETCH of a Glue 6.0 entry point.

    --dataset_root s3://bucket/bronze/vendor --table events_raw --mode incremental
    --dates 2026-11-22,2026-11-23 --silver_table glue_catalog.silver.events
    --quarantine_table glue_catalog.silver.events_quarantine
    Fields are project-specific: define FIELDS for the source (see README).
    """
    import argparse

    ap = argparse.ArgumentParser()
    for a in ("dataset_root", "table", "mode", "dates", "silver_table", "quarantine_table"):
        ap.add_argument(f"--{a}", required=True)
    args, _ = ap.parse_known_args(argv)  # Glue adds its own --JOB_ID etc.
    spark = SparkSession.builder.getOrCreate()
    dates = [date.fromisoformat(d) for d in args.dates.split(",")]
    bronze = read_bronze(spark, args.dataset_root, args.table, args.mode, dates)
    if bronze is None:
        return
    silver, quarantine = transform(bronze, FIELDS, KNOWN_KEYS)
    quarantine.writeTo(args.quarantine_table).append()
    merge_into_iceberg(spark, latest_per_id(silver), args.silver_table)


FIELDS: list[Field] = []  # per source, e.g. [Field("status", "$.status", "string"), ...]
KNOWN_KEYS: list[str] = ["id"]  # per source: top-level keys used without a Field (the id key)


if __name__ == "__main__":  # pragma: no cover
    main()
