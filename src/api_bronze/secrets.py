"""Resolve secret references inside the client config at run time.

Any value in `client.auth` or `client.headers` may be a reference instead of a
literal, so config documents never hold credentials:

    {"env": "VENDOR_TOKEN"}                              environment variable
    {"secret_id": "vendor/api"}                          whole SecretString
    {"secret_id": "vendor/api", "key": "token"}          one key of a JSON SecretString
    {"secret_id": "...", "key": "token", "format": "Bearer {}"}   wrap the value

The lookup and its Secrets Manager cache are `bronze_common.secrets`, shared with
`db_bronze`. Values are cached for the container's lifetime. Rotation is picked up
on the next cold start, or call `clear_cache()`.
"""

from __future__ import annotations

from typing import Any

from bronze_common.secrets import REF_KEYS, clear_cache, lookup

__all__ = ["clear_cache", "resolve"]


def _is_ref(value: Any) -> bool:
    return isinstance(value, dict) and set(value) - {"format"} in REF_KEYS


def _resolve_ref(ref: dict) -> str:
    return ref.get("format", "{}").format(lookup(ref))


def resolve(value: Any) -> Any:
    """Return a copy of `value` with every secret reference replaced by its value."""
    if _is_ref(value):
        return _resolve_ref(value)
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v) for v in value]
    return value
