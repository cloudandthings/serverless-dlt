"""Resolve secret references in connection fields at run time (DESIGN-db.md section 5.1).

Any connection field may be a reference instead of a literal, so config documents
never hold credentials:

    {"env": "CORE_DB_PASSWORD"}                          environment variable
    {"secret_id": "ingest/core_banking"}                 whole SecretString
    {"secret_id": "ingest/core_banking", "key": "password"}   one key of a JSON SecretString

Copied from `api_bronze.secrets` (D1: no shared module yet). AWS Secrets Manager
values are cached for the container's lifetime; rotation is picked up on the next
cold start, or call `clear_cache()`.
"""

from __future__ import annotations

import json
import os
from functools import cache
from typing import Any

_REF_KEYS = ({"env"}, {"secret_id"}, {"secret_id", "key"})


def is_ref(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) in _REF_KEYS
        and all(isinstance(v, str) and v for v in value.values())
    )


@cache
def _secret_string(secret_id: str) -> str:
    import boto3  # lazy: only needed when a config actually uses Secrets Manager

    return boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]


def clear_cache() -> None:
    _secret_string.cache_clear()


def resolve(value: Any) -> Any:
    """`value` itself, or the value a secret reference points at."""
    if not is_ref(value):
        return value
    if "env" in value:
        try:
            return os.environ[value["env"]]
        except KeyError:
            raise KeyError(f"secret ref: environment variable {value['env']!r} is not set") from None
    secret = _secret_string(value["secret_id"])
    if "key" in value:
        return json.loads(secret)[value["key"]]
    return secret
