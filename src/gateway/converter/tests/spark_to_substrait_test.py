# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait plan conversion routines."""
from pathlib import Path

import pytest
from google.protobuf import text_format
from pyspark.sql.connect.proto import base_pb2 as spark_base_pb2
from substrait.gen.proto import plan_pb2

from backends.backend_selector import find_backend
from gateway.converter.conversion_options import duck_db
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from gateway.demo.mystream_database import create_mystream_database, delete_mystream_database
from gateway.tests.conftest import find_tpch, prepare_tpch_parquet_data

test_case_directory = Path(__file__).resolve().parent / 'data'

substrait_test_case_paths = [f for f in test_case_directory.iterdir() if f.suffix == '.spark']

substrait_test_case_names = [p.stem for p in substrait_test_case_paths]

sql_test_case_paths = [f for f in test_case_directory.iterdir() if f.suffix == '.sql']

sql_test_case_names = [p.stem for p in sql_test_case_paths]


# pylint: disable=E1101
@pytest.mark.parametrize(
    'path',
    substrait_test_case_paths,
    ids=substrait_test_case_names,
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

    options = duck_db()
    backend = find_backend(options.backend)
    options.implement_show_string = False
    convert = SparkSubstraitConverter(options)
    convert.set_backends(backend, backend)
    substrait = convert.convert_plan(spark_plan)

    if request.config.getoption('rebuild_goldens'):
        if substrait != substrait_plan:
            with open(path.with_suffix('.splan'), "w", encoding='utf-8') as file:
                file.write(text_format.MessageToString(substrait))
        return

    assert substrait == substrait_plan


@pytest.fixture(autouse=True)
def manage_database(prepare_tpch_parquet_data) -> None:
    """Creates the mystream database for use throughout all the tests."""
    create_mystream_database()
    yield
    delete_mystream_database()


# pylint: disable=E1101
@pytest.mark.parametrize(
    'path',
    sql_test_case_paths,
    ids=sql_test_case_names,
)
def test_sql_conversion(request, path):
    """Test the conversion of SQL to a Substrait plan."""
    # Read the SQL to run.
    with open(path, "rb") as file:
        sql_bytes = file.read()
    sql = sql_bytes.decode('utf-8')

    # The expected result is in the corresponding Substrait plan.
    with open(path.with_suffix('.sql-splan'), "rb") as file:
        splan_prototext = file.read()
    substrait_plan = text_format.Parse(splan_prototext, plan_pb2.Plan())

    options = duck_db()
    backend = find_backend(options.backend)
    backend.register_table('customer', find_tpch() / 'customer.parquet')
    substrait = backend.convert_sql(str(sql))

    if request.config.getoption('rebuild_goldens'):
        if substrait != substrait_plan:
            with open(path.with_suffix('.sql-splan'), "w", encoding='utf-8') as file:
                file.write(text_format.MessageToString(substrait))
        return

    assert substrait == substrait_plan
