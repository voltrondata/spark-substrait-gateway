# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait plan conversion routines."""
from pathlib import Path

from google.protobuf import json_format, text_format
import pytest
from hamcrest import assert_that, equal_to
from substrait.gen.proto import plan_pb2

from gateway.converter.tools.duckdb_substrait_to_arrow import simplify_casts

test_case_directory = Path(__file__).resolve().parent / 'data'

test_case_paths = [f for f in test_case_directory.iterdir() if f.suffix == '.json']

test_case_names = [p.stem for p in test_case_paths]


# pylint: disable=E1101
@pytest.mark.parametrize(
    'path',
    test_case_paths,
    ids=test_case_names,
)
def test_simplify_casts(request, path):
    """Test cast simplification in a Substrait plan."""
    # Read the Spark plan to convert.
    with open(path, "rb") as file:
        plan_prototext = file.read()
    source_plan = json_format.Parse(plan_prototext, plan_pb2.Plan())

    # The expected result is in the corresponding Substrait plan.
    with open(path.with_suffix('.golden'), "rb") as file:
        splan_prototext = file.read()
    expected_plan = json_format.Parse(splan_prototext, plan_pb2.Plan())

    arrow_plan = simplify_casts(source_plan)

    if request.config.getoption('rebuild_goldens'):
        if arrow_plan != expected_plan:
            with open(path.with_suffix('.golden'), "wt", encoding='utf-8') as file:
                file.write(json_format.MessageToJson(arrow_plan))
        return

    arrow_plan_text = text_format.MessageToString(arrow_plan)
    expected_plan_text = text_format.MessageToString(expected_plan)
    assert_that(arrow_plan_text, equal_to(expected_plan_text))
