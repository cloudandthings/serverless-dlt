"""CloudWatch Embedded Metric Format output: one JSON line on stdout per table and metric set."""

from __future__ import annotations

import json
import time

NAMESPACE = "db_bronze"


def emit(pipeline: str, table: str, **values: int) -> None:
    """Print one EMF record carrying `values` (metric name -> count), dimensioned by pipeline and table."""
    record = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": NAMESPACE,
                    "Dimensions": [["pipeline", "table"]],
                    "Metrics": [{"Name": name, "Unit": "Count"} for name in values],
                }
            ],
        },
        "pipeline": pipeline,
        "table": table,
        **values,
    }
    print(json.dumps(record), flush=True)
