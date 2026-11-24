"""Cut each record out of a JSON response body VERBATIM, without re-serialising it.

`json.JSONDecoder.raw_decode(text, idx)` parses one value starting at `idx` and
returns where it ended, so `text[start:end]` is the record exactly as the vendor
sent it: same whitespace, key order, escapes and number literals. Parsing only
finds the boundaries. The stored text is a slice of the original.

    split_records('{"data": [ {"a": 1.10}, {"a": 2} ], "next": null}', "data")
    -> ['{"a": 1.10}', '{"a": 2}']

`path` is a dotted key path to the records array (`"data"`, `"result.items"`),
or `"$"` / `""` when the body itself is the array.
"""

from __future__ import annotations

import json
import re

_DEC = json.JSONDecoder()  # stdlib: accepts NaN/Infinity, big ints, keeps number text via slicing
_WS = re.compile(r"[ \t\n\r]*")
_BOM = "﻿"


class SplitError(ValueError):
    """The body is JSON, but the records array can't be located unambiguously."""


def _ws(text: str, i: int) -> int:
    return _WS.match(text, i).end()


def _value_at_key(text: str, i: int, key: str) -> int:
    """`i` points at an object; return the index where `key`'s value starts.

    Scans the whole object. A duplicated key raises, because `json.loads` would
    take the LAST one while a naive scanner takes the first.
    """
    if text[i : i + 1] != "{":
        raise SplitError(f"expected an object before key {key!r} at offset {i}")
    i = _ws(text, i + 1)
    found = None
    while text[i : i + 1] != "}":
        k, i = _DEC.raw_decode(text, i)
        i = _ws(text, i)
        if text[i : i + 1] != ":":
            raise SplitError(f"expected ':' at offset {i}")
        i = _ws(text, i + 1)
        if k == key:
            if found is not None:
                raise SplitError(f"duplicate key {key!r}")
            found = i
        _, i = _DEC.raw_decode(text, i)  # skip the value
        i = _ws(text, i)
        if text[i : i + 1] == ",":
            i = _ws(text, i + 1)
        elif text[i : i + 1] != "}":
            raise SplitError(f"expected ',' or '}}' at offset {i}")
    if found is None:
        raise SplitError(f"key {key!r} not found")
    return found


def path_keys(path: str) -> list[str]:
    """`"data"` -> ["data"], `"$.a.b"` -> ["a", "b"], `"$"` / `""` -> [] (root array)."""
    path = path.strip()
    if path in ("", "$"):
        return []
    if path.startswith("$."):
        path = path[2:]
    keys = path.split(".")
    if any(not k or any(c in k for c in "[]*") for k in keys):
        raise ValueError(f"records_path {path!r}: only dotted object keys are supported")
    return keys


def split_records_parsed(text: str, path: str) -> list[tuple[str, object]]:
    """Verbatim text of every element of the array at `path`, plus its parsed value.

    The parsed value is the one `raw_decode` produced while finding the boundary,
    so each record is parsed once.
    """
    i = _ws(text, 1 if text.startswith(_BOM) else 0)
    for key in path_keys(path):
        i = _value_at_key(text, i, key)
    if text[i : i + 1] != "[":
        raise SplitError(f"{path!r} is not an array")
    out: list[tuple[str, object]] = []
    i = _ws(text, i + 1)
    if text[i : i + 1] == "]":
        return out
    while True:
        obj, end = _DEC.raw_decode(text, i)
        out.append((text[i:end], obj))
        i = _ws(text, end)
        if text[i : i + 1] == ",":
            i = _ws(text, i + 1)
        elif text[i : i + 1] == "]":
            return out
        else:
            raise SplitError(f"expected ',' or ']' at offset {i}")


def split_records(text: str, path: str) -> list[str]:
    """Verbatim JSON text of every element of the array at `path`."""
    return [t for t, _ in split_records_parsed(text, path)]
