"""What both Lambda handlers do around their `run_pipeline`: load the config, set the time budget, unwrap errors."""

from __future__ import annotations

import os
import posixpath

import fsspec

MIN_BUDGET_S = 60.0  # never less than this, whatever Lambda has left


def load_config[C](event: dict, config_cls: type[C]) -> C:
    """`config_cls` (a SourceConfig) from the event's inline `config`, or from `${CONFIG_URI}/${source_id}.json`.

    CONFIG_URI is any fsspec URL (`s3://config-bucket/sources`, `file:///opt/config`).
    """
    if "config" in event:
        return config_cls.from_dict(event["config"])
    source_id = event.get("source_id")
    if not source_id or "/" in source_id or source_id.startswith("."):
        raise ValueError(f"event needs a plain source_id or an inline config, got {source_id!r}")
    uri = posixpath.join(os.environ["CONFIG_URI"].rstrip("/"), f"{source_id}.json")
    with fsspec.open(uri, "rb") as f:
        return config_cls.from_json(f.read())


def lambda_budget(context) -> float | None:
    """Seconds a run may spend: Lambda's remaining time less SAFETY_MARGIN_S (default 120), at least MIN_BUDGET_S.

    None without a Lambda context (local runs, tests): no limit.
    """
    if context is None:
        return None
    margin = float(os.environ.get("SAFETY_MARGIN_S", "120"))
    return max(MIN_BUDGET_S, context.get_remaining_time_in_millis() / 1000 - margin)


def find_in_chain[E: BaseException](exc: BaseException | None, kind: type[E]) -> E | None:
    """The first exception of type `kind` in `exc`'s cause/context chain (dlt wraps ours in PipelineStepFailed)."""
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, kind):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return None
