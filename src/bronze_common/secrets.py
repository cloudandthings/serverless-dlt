"""Secret references, resolved at run time, so config documents never hold credentials.

    {"env": "NAME"}                         environment variable
    {"secret_id": "id"}                     whole SecretString
    {"secret_id": "id", "key": "k"}         one key of a JSON SecretString

Each package decides where references may appear and what else a reference may
carry (`api_bronze` adds `format`). AWS Secrets Manager values are cached for
the container's lifetime; rotation is picked up on the next cold start, or call
`clear_cache()`.
"""

from __future__ import annotations

import json
import os
from functools import cache

REF_KEYS = ({"env"}, {"secret_id"}, {"secret_id", "key"})


@cache
def _secret_string(secret_id: str) -> str:
    import boto3  # lazy: only needed when a config actually uses Secrets Manager

    return boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]


def clear_cache() -> None:
    _secret_string.cache_clear()


def lookup(ref: dict) -> str:
    """The value a reference (keys as in REF_KEYS, extra keys ignored) points at."""
    if "env" in ref:
        try:
            return os.environ[ref["env"]]
        except KeyError:
            raise KeyError(f"secret ref: environment variable {ref['env']!r} is not set") from None
    secret = _secret_string(ref["secret_id"])
    if "key" in ref:
        return json.loads(secret)[ref["key"]]
    return secret
