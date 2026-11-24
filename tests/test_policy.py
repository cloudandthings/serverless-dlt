"""Broken-response policy, end to end through run_pipeline against the fake API."""

import json

import pytest
from conftest import (
    EventsAPI,
    bronze_rows,
    cursor_state,
    data_files,
    event,
    make_config,
    quarantined,
    root_cause,
    serve,
)
from dlt.pipeline.exceptions import PipelineStepFailed

from api_bronze import RunRequest, SourceConfig, run_pipeline

INC = RunRequest("incremental")


def run(bucket, api, pipelines_dir, **cfg_overrides):
    cfg = SourceConfig.from_dict(make_config(bucket, api.base_url, **cfg_overrides))
    return run_pipeline(cfg, INC, pipelines_dir=pipelines_dir)


def page(*records: bytes, nxt: bool = False) -> bytes:
    link = b'"{next}"' if nxt else b"null"
    return b'{"data": [' + b", ".join(records) + b'], "pagination": {"next": ' + link + b"}}"


@pytest.fixture
def seeded(bucket, api, pipelines_dir):
    """One good load first, so 'cursor unchanged' is a real assertion."""
    api.upsert(event(1, "2026-11-20T08:00:00Z"))
    run(bucket, api, pipelines_dir)
    before = cursor_state(bucket, pipelines_dir)
    assert before is not None
    return before


BROKEN_PAGES = {
    "records_key_missing": b'{"error": "rate limited", "retryAfter": 30}',
    "records_null": b'{"data": null, "pagination": {"next": null}}',
    "records_object": b'{"data": {"id": "EV-9"}, "pagination": {"next": null}}',
    "html_200": b"<html><body>Service Unavailable</body></html>",
    "empty_body": b"",
    "truncated": b'{"data": [{"id": "EV-4", "updatedAt": "2026-11-21T09:00:00Z"}, {"id": "EV-5", "upd',
    "duplicate_records_key": b'{"data": [], "data": [{"id": "EV-6", "updatedAt": "2026-11-21T09:00:00Z"}]}',
}


@pytest.mark.parametrize("name", BROKEN_PAGES)
def test_broken_page_is_quarantined_and_fails_the_run(name, seeded, bucket, api, pipelines_dir):
    files_before = data_files(bucket)
    api.override[1] = BROKEN_PAGES[name]
    with pytest.raises(PipelineStepFailed) as exc:
        run(bucket, api, pipelines_dir)
    assert type(root_cause(exc.value)).__name__ == "BrokenPageError"
    [body] = quarantined(bucket)
    assert body.read_bytes() == BROKEN_PAGES[name]  # byte-identical
    meta = json.loads(body.with_name(body.name.replace(".body", ".meta.json")).read_text())
    assert meta["pipeline"] == "test_events" and meta["status"] == 200
    assert data_files(bucket) == files_before  # nothing landed
    assert cursor_state(bucket, pipelines_dir) == seeded  # cursor held


def test_broken_page_2_discards_good_page_1_then_rerun_recovers(seeded, bucket, api, pipelines_dir):
    api.upsert(
        event(2, "2026-11-21T08:00:00Z"),
        event(3, "2026-11-21T09:00:00Z"),
        event(4, "2026-11-21T10:00:00Z"),
        event(5, "2026-11-21T11:00:00Z"),
    )
    api.override[2] = b"<html>maintenance</html>"
    with pytest.raises(PipelineStepFailed) as exc:
        run(bucket, api, pipelines_dir)
    assert type(root_cause(exc.value)).__name__ == "BrokenPageError"
    assert {r["record_id"] for r in bronze_rows(bucket)} == {"EV-001"}  # page 1 did not land either
    assert cursor_state(bucket, pipelines_dir) == seeded

    del api.override[2]
    run(bucket, api, pipelines_dir)
    assert {r["record_id"] for r in bronze_rows(bucket)} == {f"EV-{i:03}" for i in range(1, 6)}


def test_server_error_fails_without_moving_cursor(seeded, bucket, api, pipelines_dir, monkeypatch):
    monkeypatch.setenv("RUNTIME__REQUEST_MAX_ATTEMPTS", "1")
    api.upsert(*(event(i, f"2026-11-21T0{i}:00:00Z") for i in range(2, 6)))
    api.fail_on_page = 2
    with pytest.raises(PipelineStepFailed, match="500"):
        run(bucket, api, pipelines_dir)
    assert cursor_state(bucket, pipelines_dir) == seeded
    assert quarantined(bucket) == []  # HTTP errors are not quarantined: there is no data to keep


def test_bad_records_land_inline_with_error(seeded, bucket, api, pipelines_dir):
    files_before = set(data_files(bucket))
    api.override[1] = page(
        b'{"id": "EV-6", "updatedAt": "2026-11-22T08:00:00Z"}',
        b'{"id": "EV-7"}',
        b'{"id": "EV-8", "updatedAt": "yesterday"}',
        b'{"updatedAt": "2026-11-22T09:00:00Z"}',
        b'{"id": {"nested": 1}, "updatedAt": "2026-11-22T09:30:00Z"}',
        b'"oops"',
    )
    run(bucket, api, pipelines_dir)
    rows = {r["record_index"]: r for r in bronze_rows(bucket) if r["_file"] not in files_before}
    assert len(rows) == 6
    assert rows[0]["error"] is None
    assert rows[1]["error"] == "bad updatedAt: None" and rows[1]["updated_at_utc"] is None
    assert rows[2]["error"] == "bad updatedAt: 'yesterday'"
    assert rows[3]["error"] == "missing id" and rows[3]["updated_at_utc"] is not None
    assert rows[4]["error"] == "id is not a scalar"
    assert rows[5]["error"] == "record is str, not an object" and rows[5]["payload"] == '"oops"'
    served = api.served[-1].decode()
    assert all(r["payload"] in served for r in rows.values())  # all verbatim
    # only parseable cursors move state: the max is row 4 at 09:30 (bad id, good cursor)
    assert str(cursor_state(bucket, pipelines_dir)).startswith("2026-11-22 09:30")


def test_nan_and_infinity_records_land_verbatim(seeded, bucket, api, pipelines_dir):
    rec_a = b'{"id": "EV-11", "updatedAt": "2026-11-22T10:00:00Z", "score": NaN}'
    rec_b = b'{"id": "EV-12", "updatedAt": "2026-11-22T11:00:00Z", "range": [-Infinity, Infinity]}'
    api.override[1] = page(rec_a, rec_b)
    run(bucket, api, pipelines_dir)
    rows = {r["record_id"]: r for r in bronze_rows(bucket)}
    assert rows["EV-11"]["payload"].encode() == rec_a and rows["EV-11"]["error"] is None
    assert rows["EV-12"]["payload"].encode() == rec_b
    assert quarantined(bucket) == []


def test_invalid_utf8_lands_one_raw_row(seeded, bucket, api, pipelines_dir):
    body = page(b'{"id": "EV-10", "updatedAt": "2026-11-23T08:00:00Z", "name": "caf\xe9"}')
    api.override[1] = body
    run(bucket, api, pipelines_dir)
    [row] = [r for r in bronze_rows(bucket) if r["payload_raw"] is not None]
    assert row["payload_raw"] == body and row["payload"] is None
    assert row["error"].startswith("invalid UTF-8") and row["updated_at_utc"] is None
    assert cursor_state(bucket, pipelines_dir) == seeded


def test_empty_records_array_is_a_quiet_run(seeded, bucket, api, pipelines_dir):
    files_before = data_files(bucket)
    result = run(bucket, api, pipelines_dir)  # nothing newer than the cursor
    assert result["is_empty"] and data_files(bucket) == files_before
    assert cursor_state(bucket, pipelines_dir) == seeded


def test_broken_page_in_one_resource_blocks_all_resources_in_the_run(bucket, pipelines_dir):
    good = EventsAPI()
    good.upsert(event(1, "2026-11-20T08:00:00Z"))
    broken = lambda q, h, base: (200, b"<html>maintenance</html>")  # noqa: E731
    with serve({"events": good, "broken": broken}) as base_url:
        cfg = make_config(bucket, base_url)
        cfg["resources"].append(dict(cfg["resources"][0], name="broken_raw", path="broken"))
        with pytest.raises(PipelineStepFailed):
            run_pipeline(SourceConfig.from_dict(cfg), INC, pipelines_dir=pipelines_dir)
        assert good.served, "the good endpoint was fetched"
    assert data_files(bucket) == [] and data_files(bucket, "broken_raw") == []  # good one did not land
