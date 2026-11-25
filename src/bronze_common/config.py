"""Strict config building: plain dataclasses, unknown keys rejected (both packages' `config`)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields
from typing import Any


def strict_builder(error: type[Exception]) -> Callable[..., Any]:
    """`build(cls, data, where, **overrides)`: `cls(**data)`, failing with `error` on anything but a known-keys object.

    `error` is the package's own ConfigError, so callers and the Lambda errorType see that.
    `overrides` replace keys of `data` (nested objects the caller has already built).
    """

    def build(cls, data: Any, where: str, **overrides):
        if not isinstance(data, dict):
            raise error(f"{where}: expected an object, got {type(data).__name__}")
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(data) - known)
        if unknown:
            raise error(f"{where}: unknown keys {unknown}; allowed {sorted(known)}")
        try:
            return cls(**{**data, **overrides})
        except TypeError as e:
            raise error(f"{where}: {e}") from e

    return build
