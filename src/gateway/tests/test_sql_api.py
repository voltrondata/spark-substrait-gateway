# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from pathlib import Path

import pytest
from gateway.backends.backend import Backend
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.sql.session import SparkSession
from pyspark.testing import assertDataFrameEqual

test_case_directory = Path(__file__).resolve().parent / 'data'

sql_test_case_paths = [f for f in sorted(test_case_directory.iterdir()) if f.suffix == '.sql']

sql_test_case_names = [p.stem for p in sql_test_case_paths]


def _register_table(spark_session: SparkSession, name: str) -> None:
    location = Backend.find_tpch() / name
    spark_session.sql(
        f'CREATE OR REPLACE TEMPORARY VIEW {name} USING org.apache.spark.sql.parquet '
        f'OPTIONS ( path "{location}" )')


@pytest.fixture(scope='function')
def spark_session_with_customer_database(spark_session: SparkSession, source: str) -> SparkSession:
    """Creates a temporary view of the customer database."""
    if source == 'spark':
        _register_table(spark_session, 'customer')
        _register_table(spark_session, 'lineitem')
        _register_table(spark_session, 'nation')
        _register_table(spark_session, 'orders')
        _register_table(spark_session, 'part')
        _register_table(spark_session, 'partsupp')
        _register_table(spark_session, 'region')
        _register_table(spark_session, 'supplier')
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

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize(
        'path',
        sql_test_case_paths,
        ids=sql_test_case_names,
    )
    def test_tpch(self, spark_session_with_customer_database, path):
        """Test the TPC-H queries."""
        # Read the SQL to run.
        with open(path, "rb") as file:
            sql_bytes = file.read()
        sql = sql_bytes.decode('utf-8')
        spark_session_with_customer_database.sql(sql).collect()
