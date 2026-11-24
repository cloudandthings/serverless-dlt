"""Silver reference transform against real bronze. Needs: uv sync --group silver, Java 17+."""

import shutil
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import data_files, make_config

from api_bronze import RunRequest, SourceConfig, run_pipeline

pyspark = pytest.importorskip("pyspark", reason="uv sync --group silver")
pytestmark = pytest.mark.spark
sys.path.insert(0, str(Path(__file__).parents[1] / "silver"))
from pyspark.sql import functions as F  # noqa: E402
from silver_transform import Field, latest_per_id, read_bronze, transform  # noqa: E402

FIELDS = [
    Field("status", "$.status", "string"),
    Field("amount", "$.detail.amount.value", "decimal(18,2)"),
    Field("currency", "$.detail.amount.currency", "string"),
    Field("updated_at", "$.updatedAt", "timestamp"),
]


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("silver-test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def rec(i, ts, amount='"10.00"', extra=""):
    return (
        f'{{"id": "EV-{i}", "updatedAt": "{ts}", "status": "open", '
        f'"detail": {{"amount": {{"value": {amount}, "currency": "ZAR"}}}}{extra}}}'
    ).encode()


def body(*records: bytes) -> bytes:
    return b'{"data": [' + b", ".join(records) + b'], "pagination": {"next": null}}'


def test_bronze_to_silver(spark, bucket, api, pipelines_dir):
    cfg = SourceConfig.from_dict(make_config(bucket, api.base_url))
    api.override[1] = body(
        rec(1, "2026-11-20T10:00:00+02:00"),  # good, offset
        rec(2, "2026-11-20T09:00:00Z", amount='"not money"'),  # cast fails -> rescued
        rec(3, "2026-11-20T09:30:00Z", extra=', "newField": {"x": 1}'),  # unknown key
        rec(4, "2026-11-20T09:45:00Z", amount="NaN"),  # Spark can't parse NaN
        b'{"id": "EV-5"}',  # bronze error: no cursor
    )
    run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
    api.override[1] = body(rec(1, "2026-11-21T10:00:00Z", amount='"20.00"'))  # newer version of EV-1
    run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)

    # a file from a load that never completed: same layout, load id absent from _dlt_loads
    src = data_files(bucket)[0]
    shutil.copy(src, src.with_name("1999999999.123.deadbeef00.parquet"))

    today = datetime.now(UTC).date()
    bronze = read_bronze(spark, (bucket / "vendor").as_uri(), "events_raw", "incremental", [today, date(2000, 1, 1)])
    assert bronze.count() == 6  # 5 + 1; the incomplete load's rows are excluded

    silver, quarantine = transform(bronze, FIELDS, known_keys=["id"])
    # collect() renders timestamps in the host timezone; compare in UTC inside Spark
    utc_text = F.date_format("updated_at", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("updated_at_text")
    by_id = {}
    for r in silver.select("*", utc_text).collect():
        by_id.setdefault(r["id"], []).append(r)

    ev1 = sorted(by_id["EV-1"], key=lambda r: r["_cursor"])
    assert [r["amount"] for r in ev1] == [Decimal("10.00"), Decimal("20.00")]
    assert ev1[0]["updated_at_text"] == "2026-11-20T08:00:00Z"  # +02:00 offset honoured
    assert ev1[0]["_rescued_data"] is None and ev1[0]["_unknown_keys"] is None
    assert ev1[0]["_bronze"]["record_index"] == 0 and ev1[0]["_bronze"]["load_id"]

    [ev2] = by_id["EV-2"]
    assert ev2["amount"] is None and ev2["_rescued_data"] == '{"amount":"\\"not money\\""}'
    [ev3] = by_id["EV-3"]
    assert ev3["_unknown_keys"] == ["newField"]

    reasons = {r["record_id"]: r["reason"] for r in quarantine.collect()}
    assert reasons["EV-5"] == "bad updatedAt: None"
    assert reasons["EV-4"] == "silver: payload is not valid JSON for Spark"
    assert set(by_id) == {"EV-1", "EV-2", "EV-3"}

    latest = {r["id"]: r for r in latest_per_id(silver).collect()}
    assert latest["EV-1"]["amount"] == Decimal("20.00") and len(latest) == 3


def test_missing_prefix_is_nothing_to_do(spark, bucket):
    assert read_bronze(spark, (bucket / "vendor").as_uri(), "events_raw", "incremental", [date(2001, 1, 1)]) is None
