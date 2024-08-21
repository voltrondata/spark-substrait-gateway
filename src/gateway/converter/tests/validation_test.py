# SPDX-License-Identifier: Apache-2.0
"""Validation for the Spark to Substrait plan conversion routines."""

from pathlib import Path

import pytest
import substrait_validator
from google.protobuf import text_format
from substrait.gen.proto import plan_pb2

test_case_directory = Path(__file__).resolve().parent / "data"

test_case_paths = [f for f in test_case_directory.iterdir() if f.suffix == ".splan"]

test_case_names = [p.stem for p in test_case_paths]


# pylint: disable=E1101,fixme
@pytest.mark.parametrize(
    "path",
    test_case_paths,
    ids=test_case_names,
)
def test_validate_substrait_plan(path):
    """Uses substrait-validator to check the plan for issues."""
    with open(path.with_suffix(".splan"), "rb") as file:
        splan_prototext = file.read()
    substrait_plan = text_format.Parse(splan_prototext, plan_pb2.Plan())
    diagnostics = substrait_validator.plan_to_diagnostics(substrait_plan.SerializeToString())
    issues = []
    for issue in diagnostics:
        if issue.adjusted_level >= substrait_validator.Diagnostic.LEVEL_ERROR:
            issues.append(issue.msg)
    assert issues == []  # pylint: disable=use-implicit-booleaness-not-comparison
