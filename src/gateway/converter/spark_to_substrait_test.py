import os
import pytest
from pathlib import Path

from gateway.converter.spark_to_substrait import SparkSubstraitConverter


test_case_directory = Path(os.path.dirname(os.path.realpath(__file__))) / 'data'

test_case_paths = [f for f in test_case_directory.iterdir() if f.name.endswith('.spark')]

test_case_names = [os.path.basename(p).removesuffix('.spark') for p in test_case_paths]


@pytest.fixture(
    params=test_case_paths,
    ids=test_case_names,
)
def test_func(request: pytest.FixtureRequest):
    spark_plan = request.param
    convert = SparkSubstraitConverter.SparkSubstraitConverter()
    substrait = convert.convert_plan(spark_plan)
    assertTrue(substrait)
