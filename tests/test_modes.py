"""Run modes, cursor state across cold starts, layout, codec, cursor/param formats, auth."""

import json
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest
from conftest import bronze_rows, cursor_state, data_files, event, make_config, serve

from api_bronze import ConfigError, RunRequest, SourceConfig, run_pipeline


def cfg_for(bucket, api, **overrides) -> SourceConfig:
    return SourceConfig.from_dict(make_config(bucket, api.base_url, **overrides))


def ids(bucket, mode=None) -> list[str]:
    return sorted(r["record_id"] for r in bronze_rows(bucket) if mode is None or f"mode={mode}" in str(r["_file"]))


def test_incremental_restores_cursor_from_bronze_every_run(bucket, api, pipelines_dir):
    cfg = cfg_for(bucket, api)
    # mixed offsets: as strings "23:59+02:00" sorts after "22:30Z", but it is 21:59Z
    api.upsert(
        event(1, "2026-11-20T23:59:00+02:00"), event(2, "2026-11-20T22:30:00Z"), event(3, "2026-11-20T08:00:00")
    )  # no offset: taken as UTC
    r1 = run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
    assert r1["rows"] == {"events_raw": 3} and not r1["is_empty"]
    assert cursor_state(bucket, pipelines_dir) == datetime(2026, 11, 20, 22, 30, tzinfo=UTC)

    r2 = run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)  # cold start, nothing new
    assert r2["is_empty"]
    assert api.queries[-1]["updated_since"] == "2026-11-20T22:30:00+00:00"  # state came back from bronze

    api.upsert(event(4, "2026-11-21T09:00:00Z"), event(2, "2026-11-21T10:00:00Z", status="paid"))
    run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
    assert ids(bucket) == ["EV-001", "EV-002", "EV-002", "EV-003", "EV-004"]  # updates re-land; silver dedups


def test_layout_codec_and_row_metadata(bucket, api, pipelines_dir):
    api.upsert(event(1, "2026-11-20T08:00:00Z"))
    run_pipeline(cfg_for(bucket, api), RunRequest(), pipelines_dir=pipelines_dir)
    [f] = data_files(bucket)
    today = datetime.now(UTC)
    assert f.parent.relative_to(bucket).as_posix() == (
        f"vendor/events_raw/mode=incremental/year={today:%Y}/month={today:%m}/day={today:%d}"
    )
    assert pq.ParquetFile(f).metadata.row_group(0).column(0).compression == "ZSTD"
    [row] = pq.read_table(f).to_pylist()
    assert row["endpoint"] == "events" and row["status"] == 200 and row["page_seq"] == 1
    assert row["record_index"] == 0 and row["content_type"] == "application/json"
    assert "updated_since=" in row["url"] and row["payload"] in api.served[0].decode()
    loads = list((bucket / "vendor" / "_dlt_loads").glob("*"))
    assert any(f.name.startswith(load) for load in [p.name.split("__")[1].split(".jsonl")[0] for p in loads])


def test_backfill_is_stateless_bounded_and_partitioned_by_window(bucket, api, pipelines_dir):
    cfg = cfg_for(bucket, api)
    api.upsert(
        event(1, "2025-02-28T23:00:00Z"),
        event(2, "2025-03-01T00:00:00Z"),
        event(3, "2025-03-15T00:00:00Z"),
        event(4, "2025-04-01T00:00:00Z"),
    )
    req = RunRequest.from_event(
        {"mode": "backfill", "window_start": "2025-03-01T00:00:00Z", "window_end": "2025-04-01T00:00:00Z"}
    )
    run_pipeline(cfg, req, pipelines_dir=pipelines_dir)
    assert api.queries[-1] == {
        "updated_since": "2025-03-01T00:00:00+00:00",
        "updated_before": "2025-04-01T00:00:00+00:00",
    }
    # API since is exclusive, so EV-002 at exactly window_start is the API's call, not ours
    assert ids(bucket, "backfill") == ["EV-003"]
    assert all("mode=backfill/year=2025/month=03/day=01" in str(f) for f in data_files(bucket))
    assert cursor_state(bucket, pipelines_dir) is None  # backfill never writes the cursor


def test_backfill_without_end_param_keeps_exactly_the_window(bucket, api, pipelines_dir):
    # the trap that page-grain bronze fell into: a page straddling end_value lost in-window records
    cfg = cfg_for(
        bucket,
        api,
        resource={
            "incremental": {
                "cursor_path": "updatedAt",
                "initial_value": "2026-01-01T00:00:00Z",
                "start_param": "updated_since",
            }
        },
    )
    api.upsert(*(event(i, f"2025-03-{i:02}T00:00:00Z") for i in range(2, 9)))  # 3 per page
    req = RunRequest.from_event(
        {"mode": "backfill", "window_start": "2025-03-01T00:00:00Z", "window_end": "2025-03-06T00:00:00Z"}
    )
    run_pipeline(cfg, req, pipelines_dir=pipelines_dir)
    assert "updated_before" not in api.queries[-1]
    assert ids(bucket) == ["EV-002", "EV-003", "EV-004", "EV-005"]


def test_full_mode_refetches_everything_without_touching_state(bucket, api, pipelines_dir):
    cfg = cfg_for(bucket, api)
    api.upsert(event(1, "2026-11-20T08:00:00Z"), event(2, "2026-11-20T09:00:00Z"))
    run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
    state = cursor_state(bucket, pipelines_dir)
    run_pipeline(cfg, RunRequest.from_event({"mode": "full"}), pipelines_dir=pipelines_dir)
    assert api.queries[-1]["updated_since"] == "2026-01-01T00:00:00+00:00"
    assert ids(bucket, "full") == ["EV-001", "EV-002"]
    assert cursor_state(bucket, pipelines_dir) == state


def test_snapshot_endpoint_without_cursor(bucket, pipelines_dir):
    routes = {"countries": lambda q, h, b: (200, b'[{"code": "ZA"}, {"code": "NA"}]')}
    with serve(routes) as base_url:
        cfg = SourceConfig.from_dict(
            {
                "pipeline_name": "ref",
                "dataset_name": "vendor",
                "bucket_url": bucket.as_uri(),
                "client": {"base_url": base_url, "paginator": {"type": "single_page"}},
                "resources": [{"name": "countries_raw", "path": "countries", "records_path": "$", "id_path": "code"}],
            }
        )
        with pytest.raises(ConfigError, match="mode=full"):
            run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
        result = run_pipeline(cfg, RunRequest("full"), pipelines_dir=pipelines_dir)
    assert result["rows"] == {"countries_raw": 2}
    assert [r["payload"] for r in bronze_rows(bucket, "countries_raw")] == ['{"code": "ZA"}', '{"code": "NA"}']


def test_split_loading_with_time_limit_eventually_lands_everything(bucket, api, pipelines_dir):
    cfg = cfg_for(
        bucket,
        api,
        resource={
            "incremental": {
                "cursor_path": "updatedAt",
                "initial_value": "2026-01-01T00:00:00Z",
                "start_param": "updated_since",
                "sorted_ascending": True,
            }
        },
    )
    api.upsert(*(event(i, f"2026-11-{i:02}T00:00:00Z") for i in range(1, 11)))  # 4 pages of 3
    invocations = []
    while True:
        result = run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir, max_time_s=0.000001)
        invocations.append(result["rows"].get("events_raw", 0))
        if result["is_empty"]:
            break
        assert len(invocations) < 20
    assert result["time_limited"]
    assert len(invocations) > 2  # really was cut short
    assert ids(bucket) == [f"EV-{i:03}" for i in range(1, 11)]


def test_time_limit_ignored_unless_sorted(bucket, api, pipelines_dir):
    api.upsert(*(event(i, f"2026-11-{i:02}T00:00:00Z") for i in range(1, 8)))
    result = run_pipeline(cfg_for(bucket, api), RunRequest(), pipelines_dir=pipelines_dir, max_time_s=0.000001)
    assert not result["time_limited"] and result["rows"] == {"events_raw": 7}


def test_epoch_cursor_and_param_format(bucket, pipelines_dir):
    seen = []

    def route(q, headers, base_url):
        seen.append(q["since"][0])
        return 200, b'{"items": [{"id": 1, "ts": 1763625600123}, {"id": 2, "ts": "1763625700000"}]}'

    with serve({"things": route}) as base_url:
        cfg = SourceConfig.from_dict(
            {
                "pipeline_name": "epochs",
                "dataset_name": "vendor",
                "bucket_url": bucket.as_uri(),
                "client": {"base_url": base_url, "paginator": {"type": "single_page"}},
                "resources": [
                    {
                        "name": "things_raw",
                        "path": "things",
                        "records_path": "items",
                        "id_path": "id",
                        "incremental": {
                            "cursor_path": "ts",
                            "cursor_type": "epoch_ms",
                            "param_format": "epoch_ms",
                            "start_param": "since",
                            "initial_value": "2025-01-01T00:00:00Z",
                        },
                    }
                ],
            }
        )
        run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
        run_pipeline(cfg, RunRequest(), pipelines_dir=pipelines_dir)
    assert seen == ["1735689600000", "1763625700000"]
    rows = bronze_rows(bucket, "things_raw")
    assert rows[0]["updated_at_utc"] == datetime(2025, 11, 20, 8, 0, 0, 123000, tzinfo=UTC)
    assert rows[0]["record_id"] == "1" and rows[0]["error"] is None


def test_auth_header_from_secret_ref(bucket, api, pipelines_dir, monkeypatch):
    monkeypatch.setenv("VENDOR_TOKEN", "s3cr3t")
    api.require_auth = "Bearer s3cr3t"
    api.upsert(event(1, "2026-11-20T08:00:00Z"))
    cfg = make_config(bucket, api.base_url)
    cfg["client"]["auth"] = {"type": "bearer", "token": {"env": "VENDOR_TOKEN"}}
    run_pipeline(SourceConfig.from_dict(cfg), RunRequest(), pipelines_dir=pipelines_dir)
    assert ids(bucket) == ["EV-001"]
    assert "s3cr3t" not in json.dumps(cfg)  # the config document itself holds only the reference
