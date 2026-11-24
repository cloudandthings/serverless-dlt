"""Lambda handler wiring and config validation."""

import json

import pytest
from conftest import bronze_rows, event, make_config

from api_bronze import BrokenPageError, ConfigError, RunRequest, SourceConfig
from api_bronze.handler import handler


class FakeContext:
    def __init__(self, remaining_ms: int = 900_000) -> None:
        self.remaining_ms = remaining_ms

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_ms


def test_handler_loads_config_from_config_uri(bucket, api, tmp_path, monkeypatch):
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "vendor_events.json").write_text(json.dumps(make_config(bucket, api.base_url)))
    monkeypatch.setenv("CONFIG_URI", configs.as_uri())
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    api.upsert(event(1, "2026-11-20T08:00:00Z"))

    result = handler({"source_id": "vendor_events"}, FakeContext())
    assert result["rows"] == {"events_raw": 1} and result["mode"] == "incremental"
    assert result["load_ids"] and not result["is_empty"]
    assert handler({"source_id": "vendor_events"}, FakeContext())["is_empty"]
    assert [r["record_id"] for r in bronze_rows(bucket)] == ["EV-001"]


def test_handler_inline_config_backfill(bucket, api, tmp_path, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    api.upsert(event(1, "2025-03-02T00:00:00Z"))
    result = handler(
        {
            "config": make_config(bucket, api.base_url),
            "mode": "backfill",
            "window_start": "2025-03-01T00:00:00Z",
            "window_end": "2025-04-01T00:00:00Z",
        },
        None,
    )
    assert result["window_start"] == "2025-03-01T00:00:00+00:00" and result["rows"] == {"events_raw": 1}


def test_handler_raises_broken_page_error_itself(bucket, api, tmp_path, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    api.override[1] = b"<html>maintenance</html>"
    with pytest.raises(BrokenPageError, match="quarantined"):
        handler({"config": make_config(bucket, api.base_url)}, FakeContext())


@pytest.mark.parametrize("source_id", ["../etc/passwd", "a/b", "", None])
def test_handler_rejects_unsafe_source_ids(source_id, monkeypatch):
    monkeypatch.setenv("CONFIG_URI", "file:///nonexistent")
    with pytest.raises(ValueError, match="source_id"):
        handler({"source_id": source_id}, None)


def base_cfg() -> dict:
    return make_config(__import__("pathlib").Path("/tmp/x"), "http://api")


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda c: c.update(pipline_name="typo"), "unknown keys"),
        (lambda c: c["resources"][0].update(recordsPath="data"), "unknown keys"),
        (lambda c: c["resources"][0]["incremental"].update(cursor_type="unix"), "cursor_type"),
        (lambda c: c["resources"][0]["incremental"].update(initial_value="yesterday"), "initial_value"),
        (lambda c: c["resources"][0].update(records_path="data[0]"), "dotted"),
        (lambda c: c["resources"][0].update(name="Events-Raw"), "must match"),
        (lambda c: c["client"].update(paginator={"type": "auto"}), "auto-detection"),
        (lambda c: c.update(resources=[]), "at least one"),
        (lambda c: c.update(parquet_compression="lz4"), "parquet_compression"),
        (lambda c: c["resources"].append(dict(c["resources"][0])), "unique"),
    ],
)
def test_config_validation(mutate, match):
    cfg = base_cfg()
    mutate(cfg)
    with pytest.raises(ConfigError, match=match):
        SourceConfig.from_dict(cfg)


@pytest.mark.parametrize(
    "ev,match",
    [
        ({"mode": "replace"}, "mode"),
        ({"mode": "backfill", "window_start": "2025-03-01T00:00:00Z"}, "window_end"),
        ({"mode": "backfill", "window_start": "2025-03-02", "window_end": "2025-03-01"}, "after"),
        ({"mode": "incremental", "window_start": "2025-03-01"}, "only valid"),
    ],
)
def test_run_request_validation(ev, match):
    with pytest.raises(ConfigError, match=match):
        RunRequest.from_event(ev)
