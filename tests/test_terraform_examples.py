"""terraform/: the files the Terraform reads stay in step with the code, offline (no terraform binary)."""

from __future__ import annotations

import json
from pathlib import Path

from db_bronze.config import SourceConfig as DbSourceConfig

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "terraform/modules/db_bronze_source/templates/db_snapshot.asl.json.tftpl"
EXAMPLE_ASL = ROOT / "examples/db_snapshot.asl.json"
EXAMPLE_ARN = "arn:aws:lambda:eu-west-1:123456789012:function:db-bronze-core-banking-snapshot"


def test_snapshot_template_matches_example_asl():
    """The module's state machine is examples/db_snapshot.asl.json with two variables.

    tests/test_db_asl.py drives the example against the real handler, so keeping the
    template identical keeps the deployed state machine covered by that test.
    """
    rendered = TEMPLATE.read_text().replace("${function_arn}", EXAMPLE_ARN).replace("${max_concurrency}", "4")
    assert json.loads(rendered) == json.loads(EXAMPLE_ASL.read_text())


def test_template_has_no_other_interpolations():
    text = TEMPLATE.read_text()
    for var in ("${function_arn}", "${max_concurrency}"):
        text = text.replace(var, "")
    assert "${" not in text and "%{" not in text


def test_db_example_config_is_valid():
    """The Terraform db example uploads this config; it must pass db_bronze's strict validation."""
    data = json.loads((ROOT / "examples/core_banking.json").read_text())
    config = DbSourceConfig.from_dict(data)
    assert config.pipeline_name == "core_banking"
