"""Resolve secret references inside the client config at run time.

Any value in `client.auth` or `client.headers` may be a reference instead of a
literal, so config documents never hold credentials:

    {"env": "VENDOR_TOKEN"}                              environment variable
    {"secret_id": "vendor/api"}                          whole SecretString
    {"secret_id": "vendor/api", "key": "token"}          one key of a JSON SecretString
    {"secret_id": "...", "key": "token", "format": "Bearer {}"}   wrap the value

AWS Secrets Manager values are cached for the container's lifetime. Rotation is
picked up on the next cold start, or call `clear_cache()`.
"""

from __future__ import annotations

import json
import os
from functools import cache
from typing import Any

_REF_KEYS = ({"env"}, {"secret_id"}, {"secret_id", "key"})


def _is_ref(value: Any) -> bool:
    return isinstance(value, dict) and set(value) - {"format"} in _REF_KEYS


@cache
def _secret_string(secret_id: str) -> str:
    import boto3  # lazy: only needed when a config actually uses Secrets Manager

    return boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]


def clear_cache() -> None:
    _secret_string.cache_clear()


def _resolve_ref(ref: dict) -> str:
    if "env" in ref:
        try:
            value = os.environ[ref["env"]]
        except KeyError:
            raise KeyError(f"secret ref: environment variable {ref['env']!r} is not set") from None
    else:
        value = _secret_string(ref["secret_id"])
        if "key" in ref:
            value = json.loads(value)[ref["key"]]
    return ref.get("format", "{}").format(value)


def resolve(value: Any) -> Any:
    """Return a copy of `value` with every secret reference replaced by its value."""
    if _is_ref(value):
        return _resolve_ref(value)
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v) for v in value]
    return value
