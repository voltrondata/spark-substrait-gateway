import os
import pytest
from pathlib import Path

from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from google.protobuf import text_format
import spark.connect.base_pb2 as base_pb2


test_case_directory = Path(os.path.dirname(os.path.realpath(__file__))) / 'data'

test_case_paths = [f for f in test_case_directory.iterdir() if f.name.endswith('.spark')]

test_case_names = [os.path.basename(p).removesuffix('.spark') for p in test_case_paths]


@pytest.mark.parametrize(
    'path',
    test_case_paths,
    ids=test_case_names,
)
def test_plan_conversion(path):
    with open(path, "rb") as f:
        plan_protobuf = f.read()
    spark_plan = text_format.Parse(plan_protobuf, base_pb2.Plan())

    # TODO -- Read from the expected results from the corresponding .splan file.

    convert = SparkSubstraitConverter()
    substrait = convert.convert_plan(spark_plan)
    assert substrait
