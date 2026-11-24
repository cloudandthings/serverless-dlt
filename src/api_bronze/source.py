"""Build dlt resources (one per configured endpoint) for one run mode."""

from __future__ import annotations

import logging
from datetime import datetime

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.rest_api.config_setup import create_auth, create_paginator

from . import secrets
from .config import ResourceConfig, RunRequest, SourceConfig, format_param
from .envelope import CURSOR_COLUMN, envelope_records

log = logging.getLogger(__name__)


def make_client(cfg: SourceConfig) -> RESTClient:
    auth = create_auth(secrets.resolve(cfg.client.auth)) if cfg.client.auth else None
    return RESTClient(base_url=cfg.client.base_url, headers=secrets.resolve(cfg.client.headers) or None, auth=auth)


def _window(rc: ResourceConfig, req: RunRequest, run_started: datetime) -> tuple[datetime, datetime | None]:
    """(initial_value, end_value) for dlt's incremental.

    incremental  state-driven; initial_value only applies to the very first run
    full         [initial_value, run start): end_value makes it stateless
    backfill     [window_start, window_end): stateless, parallel-safe
    """
    inc = rc.incremental
    if req.mode == "backfill":
        return req.window_start, req.window_end
    if req.mode == "full":
        return inc.initial, run_started
    return inc.initial, None


def make_resource(cfg: SourceConfig, rc: ResourceConfig, req: RunRequest, run_started: datetime):
    client = make_client(cfg)
    quarantine_url = cfg.quarantine_url or cfg.bucket_url
    common = dict(
        pipeline_name=cfg.pipeline_name, records_path=rc.records_path, id_path=rc.id_path, quarantine_url=quarantine_url
    )

    def paginator():
        # a fresh paginator per run: paginators are stateful
        return create_paginator(rc.paginator or cfg.client.paginator)

    inc = rc.incremental
    if inc is None:

        @dlt.resource(name=rc.name, write_disposition="append", primary_key=())
        def snapshot():
            yield from envelope_records(
                client, rc.path, params=dict(rc.params), cursor_path=None, paginator=paginator(), **common
            )

        return snapshot

    initial, end = _window(rc, req, run_started)
    if end is not None and not inc.end_param:
        log.warning(
            "%s: API has no end_param, so the %s window [%s, %s) is filtered client-side. "
            "Safe at record grain, but the API returns everything after the start.",
            rc.name,
            req.mode,
            initial.isoformat(),
            end.isoformat(),
        )

    @dlt.resource(name=rc.name, write_disposition="append", primary_key=())
    def records(
        # The incremental MUST be the argument's default value: dlt binds state and
        # filtering only to an incremental found in the signature. Building it in
        # the body (the usual fix for ruff B008) silently disables both, and every
        # run re-fetches from initial_value.
        cursor=dlt.sources.incremental(  # noqa: B008
            CURSOR_COLUMN,
            initial_value=initial,
            end_value=end,
            on_cursor_value_missing="include",  # error rows have a null cursor; keep them
            row_order="asc" if inc.sorted_ascending else None,
        ),
    ):
        params = dict(rc.params)
        params[inc.start_param] = format_param(cursor.start_value, inc.param_format)
        if cursor.end_value is not None and inc.end_param:
            params[inc.end_param] = format_param(cursor.end_value, inc.param_format)
        yield from envelope_records(
            client,
            rc.path,
            params=params,
            cursor_path=inc.cursor_path,
            cursor_type=inc.cursor_type,
            paginator=paginator(),
            **common,
        )

    return records
