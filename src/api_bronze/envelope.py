"""Record-grain bronze rows for one paginated endpoint, with the broken-response policy.

Each API record becomes one bronze row. `payload` is the record's JSON text cut
verbatim from the response (split.py). The resource adds `record_id` and the
cursor column, and changes nothing inside `payload`. Rows are yielded as Arrow
tables, which bypass dlt's normaliser: no flattening, renaming, type coercion
or timestamp rewriting.

Broken-response policy:

  Page is not data       non-JSON / empty / truncated body, records key missing,
  (can't trust it or     null or not an array, on a 2xx response
  paginate past it)      -> raw body + metadata written to <quarantine_url>/_quarantine/...
                         -> BrokenPageError: the run fails, nothing from it lands,
                            cursor state doesn't move, and the next run retries.
                            A page that stays broken fails every run: alarm on it.

  Page is data, can't    invalid UTF-8
  be cut verbatim        -> ONE fallback row: whole body in `payload_raw`,
                            `error` set, cursor null. The run continues.

  One bad record         not an object, id or cursor missing, cursor unparseable
                         -> the row lands verbatim with `error` set, cursor null.
                            The run continues; good records advance the cursor.

  NaN / Infinity token   not RFC 8259, but accepted: pagination and splitting both
  in a record            use stdlib `json`, so the record lands verbatim like any
                         other. Silver's parse decides.

  `[]`                   normal empty page: no rows.

  HTTP 4xx/5xx           RESTClient raises (after dlt's retries).
"""

from __future__ import annotations

import json
import logging
import posixpath
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import fsspec
import pyarrow as pa
from requests import Response
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError

from .split import SplitError, split_records_parsed

log = logging.getLogger(__name__)

TS = pa.timestamp("us", tz="UTC")
CURSOR_COLUMN = "updated_at_utc"
RECORD_SCHEMA = pa.schema(
    [
        ("endpoint", pa.string()),  # request path as configured
        ("url", pa.string()),  # full URL actually fetched, query included
        ("status", pa.int32()),
        ("content_type", pa.string()),
        ("fetched_at", TS),
        ("page_seq", pa.int32()),  # 1-based page number within this run and endpoint
        ("record_index", pa.int32()),  # 0-based position in the page's records array
        ("record_id", pa.string()),
        ("payload", pa.string()),  # the record's JSON text, verbatim
        ("payload_raw", pa.binary()),  # whole body, only when it isn't valid UTF-8
        ("error", pa.string()),  # null for good rows
        (CURSOR_COLUMN, TS),  # parsed cursor (UTC), what dlt's incremental tracks
    ]
)

CURSOR_TYPES = ("iso8601", "epoch_s", "epoch_ms")


class BrokenPageError(RuntimeError):
    """A 2xx response that isn't usable data. Its raw body is already quarantined."""


def lookup(record: dict, path: str) -> Any:
    """Dotted-path lookup; None when any step is missing."""
    value: Any = record
    for key in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def parse_cursor(value: Any, cursor_type: str) -> datetime:
    """Cursor value -> aware UTC datetime. Raises TypeError/ValueError when unusable.

    iso8601 strings without an offset are taken as UTC. Comparing ISO strings
    with mixed offsets as text gives the wrong order, so always parse first.
    """
    if cursor_type == "iso8601":
        if not isinstance(value, str):
            raise TypeError(f"expected an ISO-8601 string, got {type(value).__name__}")
        dt = datetime.fromisoformat(value)
        return (dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt).astimezone(UTC)
    if isinstance(value, bool):
        raise TypeError("expected an epoch number, got bool")
    if isinstance(value, str):
        value = float(value)
    if not isinstance(value, int | float):
        raise TypeError(f"expected an epoch number, got {type(value).__name__}")
    seconds = value / 1000 if cursor_type == "epoch_ms" else value
    return datetime.fromtimestamp(seconds, tz=UTC)


def quarantine(
    quarantine_url: str, pipeline_name: str, endpoint: str, page_seq: int, response: Response, reason: str
) -> str:
    """Write the exact response bytes plus metadata outside dlt (works for file:// and s3://)."""
    fs, root = fsspec.core.url_to_fs(quarantine_url)
    now = datetime.now(UTC)
    folder = posixpath.join(
        root,
        "_quarantine",
        pipeline_name,
        endpoint.strip("/").replace("/", "_") or "root",
        f"year={now:%Y}",
        f"month={now:%m}",
        f"day={now:%d}",
    )
    fs.makedirs(folder, exist_ok=True)
    stem = posixpath.join(folder, f"{now:%Y%m%dT%H%M%S%f}_page{page_seq}")
    fs.pipe(stem + ".body", response.content)
    fs.pipe(
        stem + ".meta.json",
        json.dumps(
            {
                "pipeline": pipeline_name,
                "url": response.url,
                "status": response.status_code,
                "content_type": response.headers.get("Content-Type"),
                "fetched_at": now.isoformat(),
                "page_seq": page_seq,
                "error": reason,
            },
            indent=1,
        ).encode(),
    )
    return stem + ".body"


def _stdlib_json(response: Response):
    """`response.json` replacement: parse with stdlib `json`, the parser the splitter uses.

    RESTClient's data selector and every paginator call `response.json()`, which
    is requests' simplejson. simplejson rejects `NaN`/`Infinity` and stdlib
    accepts them, so one record with `NaN` would make the whole page "not JSON",
    and it would be quarantined and fail every run. With one parser, a page is
    either valid for both pagination and splitting, or for neither. Decoding
    mirrors requests (bad UTF-8 -> U+FFFD, BOM skipped). Only pagination reads
    this parse; the stored text is cut from `response.content`.
    """

    def loads(**kwargs):
        return json.loads(response.content.decode("utf-8-sig", errors="replace"), **kwargs)

    return loads


def _record_row(
    meta: dict, idx: int, text: str, rec: object, id_path: str | None, cursor_path: str | None, cursor_type: str
) -> dict:
    row = {**meta, "record_index": idx, "payload": text, "record_id": None, "error": None, CURSOR_COLUMN: None}
    if not isinstance(rec, dict):
        row["error"] = f"record is {type(rec).__name__}, not an object"
        return row
    errors = []
    if id_path:
        rid = lookup(rec, id_path)
        if rid is None or isinstance(rid, dict | list):
            errors.append(f"missing {id_path}" if rid is None else f"{id_path} is not a scalar")
        else:
            row["record_id"] = json.dumps(rid) if isinstance(rid, bool | float) else str(rid)
    if cursor_path:
        raw_cursor = lookup(rec, cursor_path)
        try:
            row[CURSOR_COLUMN] = parse_cursor(raw_cursor, cursor_type)
        except (TypeError, ValueError, OverflowError, OSError):
            errors.append(f"bad {cursor_path}: {raw_cursor!r}"[:200])
    row["error"] = "; ".join(errors) or None
    return row


def envelope_records(
    client,
    path: str,
    *,
    pipeline_name: str,
    params: dict | None,
    records_path: str,
    id_path: str | None,
    cursor_path: str | None,
    cursor_type: str = "iso8601",
    quarantine_url: str,
    paginator=None,
) -> Iterator[pa.Table]:
    """Yield one Arrow table of record rows per page. Raises BrokenPageError per the policy."""
    if cursor_type not in CURSOR_TYPES:
        raise ValueError(f"cursor_type {cursor_type!r} not in {CURSOR_TYPES}")
    last: dict[str, Response] = {}

    def keep_last(response: Response, *args, **kwargs) -> Response:
        last["response"] = response
        response.raise_for_status()  # passing hooks replaces RESTClient's default raise
        response.json = _stdlib_json(response)  # one parser for pagination and splitting
        return response

    endpoint = path.strip("/")
    seq = 0
    pages = client.paginate(
        path,
        params=params,
        paginator=paginator,
        data_selector=records_path if records_path.strip() not in ("", "$") else "$",
        hooks={"response": [keep_last]},
    )
    while True:
        try:
            page = next(pages)
        except StopIteration:
            return
        # RESTClient couldn't parse the body at all. keep_last swaps in stdlib json, but a
        # response that skipped the hook would raise requests' simplejson-based
        # JSONDecodeError, which is NOT a subclass of json.JSONDecodeError. Catch both.
        except (RequestsJSONDecodeError, json.JSONDecodeError) as e:
            resp = last["response"]
            where = quarantine(quarantine_url, pipeline_name, path, seq + 1, resp, f"not JSON: {e}")
            raise BrokenPageError(f"page {seq + 1} of {path}: not JSON ({e}); quarantined {where}") from e
        seq += 1
        resp = page.response
        meta = {
            "endpoint": endpoint,
            "url": resp.url,
            "status": resp.status_code,
            "content_type": resp.headers.get("Content-Type"),
            "fetched_at": datetime.now(UTC),
            "page_seq": seq,
        }
        try:
            text = resp.content.decode("utf-8")
        except UnicodeDecodeError as e:
            # still data (RESTClient parsed it), just not cuttable: keep the exact bytes
            log.warning("page %s of %s is not valid UTF-8; landing one payload_raw row", seq, path)
            yield pa.Table.from_pylist(
                [{**meta, "payload_raw": resp.content, "error": f"invalid UTF-8: {e}"}], schema=RECORD_SCHEMA
            )
            continue
        try:
            records = split_records_parsed(text, records_path)
        except (SplitError, json.JSONDecodeError) as e:
            # RESTClient turns a missing/null records key into [] with only a warning: not here
            where = quarantine(quarantine_url, pipeline_name, path, seq, resp, f"{type(e).__name__}: {e}")
            raise BrokenPageError(f"page {seq} of {path}: {e}; quarantined {where}") from e
        rows = [_record_row(meta, i, t, r, id_path, cursor_path, cursor_type) for i, (t, r) in enumerate(records)]
        if rows:
            yield pa.Table.from_pylist(rows, schema=RECORD_SCHEMA)
