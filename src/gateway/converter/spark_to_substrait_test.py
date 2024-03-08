# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait plan conversion routines."""
import os
from pathlib import Path

from google.protobuf import text_format
import pytest
from substrait.gen.proto import plan_pb2

from gateway.converter.conversion_options import duck_db
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from spark.connect import base_pb2 as spark_base_pb2


test_case_directory = Path(os.path.dirname(os.path.realpath(__file__))) / 'data'

test_case_paths = [f for f in test_case_directory.iterdir() if f.name.endswith('.spark')]

test_case_names = [os.path.basename(p).removesuffix('.spark') for p in test_case_paths]


# pylint: disable=E1101
@pytest.mark.parametrize(
    'path',
    test_case_paths,
    ids=test_case_names,
)
def test_plan_conversion(request, path):
    """Test the conversion of a Spark plan to a Substrait plan."""
    # Read the Spark plan to convert.
    with open(path, "rb") as file:
        plan_prototext = file.read()
    spark_plan = text_format.Parse(plan_prototext, spark_base_pb2.Plan())

    # The expected result is in the corresponding Substrait plan.
    with open(path.with_suffix('.splan'), "rb") as file:
        splan_prototext = file.read()
    substrait_plan = text_format.Parse(splan_prototext, plan_pb2.Plan())

    convert = SparkSubstraitConverter(duck_db())
    substrait = convert.convert_plan(spark_plan)

    if request.config.getoption('rebuild_goldens'):
        if substrait != substrait_plan:
            with open(path.with_suffix('.splan'), "wt", encoding='utf-8') as file:
                file.write(text_format.MessageToString(substrait))
        return

    assert substrait == substrait_plan
