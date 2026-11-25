"""db_bronze error types (DESIGN-db.md section 7.6).

Every one derives from `DbBronzeError`, so the handler can unwrap it from dlt's
`PipelineStepFailed` and report its class name as the Lambda `errorType`.
Types added later are unwrapped too, as long as they derive from the base.
"""

from __future__ import annotations


class DbBronzeError(Exception):
    """Base of every db_bronze failure the handler surfaces by type."""

    metric: str | None = None  # EMF metric the handler emits when this error ends a run
    table: str | None = None


class PreflightError(DbBronzeError):
    """Reflection checks failed at the start of a run, before anything was extracted.

    Built from `findings` (table name -> what is wrong with it), or from one message.
    """

    metric = "PreflightFailed"

    def __init__(self, findings: dict[str, list[str]] | str):
        if isinstance(findings, str):
            self.findings: dict[str, list[str]] = {}
            super().__init__(findings)
            return
        self.findings = findings
        if len(findings) == 1:
            self.table = next(iter(findings))
        lines = [f"preflight failed for {len(findings)} table(s); nothing was extracted:"]
        for name, items in findings.items():
            lines.append(f"  table {name}:")
            lines.extend(f"    - {item}" for item in items)
        lines.append(
            'Fix each table by setting "backend": "pyarrow" on it, or by excluding the offending column '
            'with "columns" (or casting it in "query").'
        )
        super().__init__("\n".join(lines))


class ConnectorXFailed(DbBronzeError):
    metric = "ConnectorXFailed"

    def __init__(self, table: str, reason: str) -> None:
        self.table = table
        self.reason = reason
        super().__init__(
            f'table {table}: ConnectorX failed: {reason}. Fix: set "backend": "pyarrow" on this table. '
            "Note that switching backend changes the bronze column types, so the next run fails with "
            "BackendChanged unless the table lands under a new name."
        )


class BackendChanged(DbBronzeError):
    metric = "BackendChanged"

    def __init__(self, table: str, previous: str, current: str) -> None:
        self.table = table
        super().__init__(
            f'table {table}: backend changed from "{previous}" to "{current}" since the last successful run. '
            "The backends produce different Arrow types, so the bronze column types would change. "
            f'Fix: land the table under a new table "name" (a new bronze prefix; silver unions old and new), '
            f'or set "backend": "{previous}" again.'
        )
