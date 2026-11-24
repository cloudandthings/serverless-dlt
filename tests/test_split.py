"""The verbatim splitter: every record is a byte-identical slice of the body."""

import json

import pytest

from api_bronze.split import SplitError, path_keys, split_records

# record bodies a JSON parser would silently change or reject
EDGE_RECORDS = {
    "unicode": '{"v": "café 😀 中文 שלום é a\\u200bb"}',
    "lone_surrogate_escape": '{"v": "bad \\ud800 surrogate"}',
    "nul_and_controls": '{"v": "a\\u0000b\\u001bc\\td\\ne"}',
    "nan_infinity": '{"v": [NaN, Infinity, -Infinity]}',
    "big_int": '{"v": 123456789012345678901234567890}',
    "precise_number": '{"v": [0.1000000000000000055511151231257827, 1.10, 1e400, -0.0]}',
    "duplicate_inner_keys": '{"amount": "10.00", "amount": 10}',
    "pretty_printed": '{\n    "v" :  1 ,\n\t"w": [ 1 , 2 ]\n  }',
    "not_an_object": '"just a string"',
}


@pytest.mark.parametrize("name", EDGE_RECORDS)
def test_record_is_cut_byte_identical(name):
    rec = EDGE_RECORDS[name]
    body = '{"data": [ ' + rec + " ,\n " + rec + '], "pagination": {"next": null}}'
    assert split_records(body, "data") == [rec, rec]


def test_bom_is_skipped():
    assert split_records('﻿{"data": [{"a": 1}]}', "data") == ['{"a": 1}']


def test_root_array_and_dotted_paths():
    assert split_records('[{"a": 1}, {"a": 2}]', "$") == ['{"a": 1}', '{"a": 2}']
    assert split_records(" [ ] ", "") == []
    body = '{"result": {"meta": {}, "items": [{"a": 1}]}, "items": "decoy"}'
    assert split_records(body, "result.items") == ['{"a": 1}']
    assert split_records(body, "$.result.items") == ['{"a": 1}']


def test_duplicate_records_key_raises():
    # json.loads would silently take the last "data"; the splitter refuses to guess
    with pytest.raises(SplitError, match="duplicate key 'data'"):
        split_records('{"data": [{"id": 1}], "data": [{"id": 2}]}', "data")


@pytest.mark.parametrize(
    "body,match",
    [
        ('{"error": "rate limited"}', "not found"),
        ('{"data": null}', "not an array"),
        ('{"data": {"id": 1}}', "not an array"),
        ("[1, 2]", "expected an object"),
    ],
)
def test_unusable_shapes_raise(body, match):
    with pytest.raises(SplitError, match=match):
        split_records(body, "data")


def test_truncated_body_raises():
    with pytest.raises(json.JSONDecodeError):
        split_records('{"data": [{"id": 1}, {"id": 2, "x": "unterm', "data")


def test_unsupported_paths_rejected():
    for bad in ("data[0]", "data..x", "data.*", "a."):
        with pytest.raises(ValueError):
            path_keys(bad)
