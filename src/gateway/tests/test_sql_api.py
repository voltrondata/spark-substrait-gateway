# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.testing import assertDataFrameEqual

test_case_directory = Path(__file__).resolve().parent / 'data'

sql_test_case_paths = [f for f in sorted(test_case_directory.iterdir()) if f.suffix == '.sql']

sql_test_case_names = [p.stem for p in sql_test_case_paths]


@pytest.fixture(autouse=True)
def mark_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue('source')
    originalname = request.keywords.node.originalname
    if source == 'gateway-over-duckdb' and originalname == 'test_tpch':
        path = request.getfixturevalue('path')
        if path.stem in ['02', '04', '15', '16', '17', '18', '20', '21', '22']:
            request.node.add_marker(pytest.mark.xfail(reason='DuckDB needs Delim join'))


# pylint: disable=missing-function-docstring
# ruff: noqa: E712
class TestSqlAPI:
    """Tests of the SQL side of SparkConnect."""

    def test_count(self, spark_session_with_tpch_dataset):
        outcome = spark_session_with_tpch_dataset.sql(
            'SELECT COUNT(*) FROM customer').collect()
        assert_that(outcome[0][0], equal_to(149999))

    def test_limit(self, spark_session_with_tpch_dataset):
        expected = [
            Row(c_custkey=2, c_phone='23-768-687-3665', c_mktsegment='AUTOMOBILE'),
            Row(c_custkey=3, c_phone='11-719-748-3364', c_mktsegment='AUTOMOBILE'),
            Row(c_custkey=4, c_phone='14-128-190-5944', c_mktsegment='MACHINERY'),
            Row(c_custkey=5, c_phone='13-750-942-6364', c_mktsegment='HOUSEHOLD'),
            Row(c_custkey=6, c_phone='30-114-968-4951', c_mktsegment='AUTOMOBILE'),
        ]
        outcome = spark_session_with_tpch_dataset.sql(
            'SELECT c_custkey, c_phone, c_mktsegment FROM customer LIMIT 5').collect()
        assertDataFrameEqual(outcome, expected)

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize(
        'path',
        sql_test_case_paths,
        ids=sql_test_case_names,
    )
    def test_tpch(self, spark_session_with_tpch_dataset, path):
        """Test the TPC-H queries."""
        # Read the SQL to run.
        with open(path, "rb") as file:
            sql_bytes = file.read()
        sql = sql_bytes.decode('utf-8')
        spark_session_with_tpch_dataset.sql(sql).collect()
