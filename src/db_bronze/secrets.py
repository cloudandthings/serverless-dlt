"""Resolve secret references in connection fields at run time (docs/db/configuration.md).

Any connection field may be a reference instead of a literal, so config documents
never hold credentials:

    {"env": "CORE_DB_PASSWORD"}                          environment variable
    {"secret_id": "ingest/core_banking"}                 whole SecretString
    {"secret_id": "ingest/core_banking", "key": "password"}   one key of a JSON SecretString

The lookup and its Secrets Manager cache are `bronze_common.secrets`, shared with
`api_bronze` (D1). Values are cached for the container's lifetime; rotation is
picked up on the next cold start, or call `clear_cache()`.
"""

from __future__ import annotations

from typing import Any

from bronze_common.secrets import REF_KEYS, clear_cache, lookup

__all__ = ["clear_cache", "is_ref", "resolve"]


def is_ref(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) in REF_KEYS
        and all(isinstance(v, str) and v for v in value.values())
    )


def resolve(value: Any) -> Any:
    """`value` itself, or the value a secret reference points at."""
    return lookup(value) if is_ref(value) else value
