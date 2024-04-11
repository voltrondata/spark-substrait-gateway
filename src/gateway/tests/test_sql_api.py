# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
import pytest
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.sql.session import SparkSession
from pyspark.testing import assertDataFrameEqual

from gateway.converter.sql_to_substrait import find_tpch


@pytest.fixture(scope='function')
def spark_session_with_customer_database(spark_session: SparkSession, source: str) -> SparkSession:
    """Creates a temporary view of the customer database."""
    if source == 'spark':
        customer_location = find_tpch() / 'customer'
        spark_session.sql(
            'CREATE OR REPLACE TEMPORARY VIEW customer USING org.apache.spark.sql.parquet '
            f'OPTIONS ( path "{customer_location}" )')
    return spark_session


# pylint: disable=missing-function-docstring
# ruff: noqa: E712
class TestSqlAPI:
    """Tests of the SQL side of SparkConnect."""

    def test_count(self, spark_session_with_customer_database):
        outcome = spark_session_with_customer_database.sql(
            'SELECT COUNT(*) FROM customer').collect()
        assert_that(outcome[0][0], equal_to(149999))

    def test_limit(self, spark_session_with_customer_database):
        expected = [
            Row(c_custkey=2, c_phone='23-768-687-3665', c_mktsegment='AUTOMOBILE'),
            Row(c_custkey=3, c_phone='11-719-748-3364', c_mktsegment='AUTOMOBILE'),
            Row(c_custkey=4, c_phone='14-128-190-5944', c_mktsegment='MACHINERY'),
            Row(c_custkey=5, c_phone='13-750-942-6364', c_mktsegment='HOUSEHOLD'),
            Row(c_custkey=6, c_phone='30-114-968-4951', c_mktsegment='AUTOMOBILE'),
        ]
        outcome = spark_session_with_customer_database.sql(
            'SELECT c_custkey, c_phone, c_mktsegment FROM customer LIMIT 5').collect()
        assertDataFrameEqual(outcome, expected)
