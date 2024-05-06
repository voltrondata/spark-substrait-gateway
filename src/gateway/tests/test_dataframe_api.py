# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
import pytest
from gateway.backends.backend import Backend
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.sql.functions import col, substring
from pyspark.testing import assertDataFrameEqual


@pytest.fixture(autouse=True)
def mark_dataframe_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue('source')
    originalname = request.keywords.node.originalname
    if source == 'gateway-over-duckdb':
        if originalname == 'test_with_column' or originalname == 'test_cast':
            request.node.add_marker(pytest.mark.xfail(reason='DuckDB column binding error'))
        elif originalname in [
            'test_create_or_replace_temp_view', 'test_create_or_replace_multiple_temp_views']:
            request.node.add_marker(pytest.mark.xfail(reason='ADBC DuckDB from_substrait error'))
    elif source == 'gateway-over-datafusion':
        if originalname in [
            'test_data_source_schema', 'test_data_source_filter', 'test_table', 'test_table_schema',
            'test_table_filter', 'test_create_or_replace_temp_view',
            'test_create_or_replace_multiple_temp_views',]:
            request.node.add_marker(pytest.mark.xfail(reason='Gateway internal iterating error'))
        else:
            pytest.importorskip("datafusion.substrait")


# pylint: disable=missing-function-docstring
# ruff: noqa: E712
class TestDataFrameAPI:
    """Tests of the dataframe side of SparkConnect."""

    def test_collect(self, users_dataframe):
        outcome = users_dataframe.collect()
        assert len(outcome) == 100

    # pylint: disable=singleton-comparison
    def test_filter(self, users_dataframe):
        outcome = users_dataframe.filter(col('paid_for_service') == True).collect()
        assert len(outcome) == 29

    # pylint: disable=singleton-comparison
    def test_filter_with_show(self, users_dataframe, capsys):
        expected = '''+-------------+---------------+----------------+
|      user_id|           name|paid_for_service|
+-------------+---------------+----------------+
|user669344115|   Joshua Brown|            true|
|user282427709|Michele Carroll|            true|
+-------------+---------------+----------------+

'''
        users_dataframe.filter(col('paid_for_service') == True).limit(2).show()
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    # pylint: disable=singleton-comparison
    def test_filter_with_show_with_limit(self, users_dataframe, capsys):
        expected = '''+-------------+------------+----------------+
|      user_id|        name|paid_for_service|
+-------------+------------+----------------+
|user669344115|Joshua Brown|            true|
+-------------+------------+----------------+
only showing top 1 row

'''
        users_dataframe.filter(col('paid_for_service') == True).show(1)
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    # pylint: disable=singleton-comparison
    def test_filter_with_show_and_truncate(self, users_dataframe, capsys):
        expected = '''+----------+----------+----------------+
|   user_id|      name|paid_for_service|
+----------+----------+----------------+
|user669...|Joshua ...|            true|
+----------+----------+----------------+

'''
        users_dataframe.filter(col('paid_for_service') == True).limit(1).show(truncate=10)
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    def test_count(self, users_dataframe):
        outcome = users_dataframe.count()
        assert outcome == 100

    def test_limit(self, users_dataframe):
        expected = [
            Row(user_id='user849118289', name='Brooke Jones', paid_for_service=False),
            Row(user_id='user954079192', name='Collin Frank', paid_for_service=False),
        ]
        outcome = users_dataframe.limit(2).collect()
        assertDataFrameEqual(outcome, expected)

    def test_with_column(self, users_dataframe):
        expected = [
            Row(user_id='user849118289', name='Brooke Jones', paid_for_service=False),
        ]
        outcome = users_dataframe.withColumn(
            'user_id', col('user_id')).limit(1).collect()
        assertDataFrameEqual(outcome, expected)

    def test_cast(self, users_dataframe):
        expected = [
            Row(user_id=849, name='Brooke Jones', paid_for_service=False),
        ]
        outcome = users_dataframe.withColumn(
            'user_id',
            substring(col('user_id'), 5, 3).cast('integer')).limit(1).collect()
        assertDataFrameEqual(outcome, expected)

    def test_data_source_schema(self, spark_session):
        location_customer = str(Backend.find_tpch() / 'customer')
        schema = spark_session.read.parquet(location_customer).schema
        assert len(schema) == 8

    def test_data_source_filter(self, spark_session):
        location_customer = str(Backend.find_tpch() / 'customer')
        customer_dataframe = spark_session.read.parquet(location_customer)
        outcome = customer_dataframe.filter(col('c_mktsegment') == 'FURNITURE').collect()
        assert len(outcome) == 29968

    def test_table(self, spark_session_with_customer_dataset):
        outcome = spark_session_with_customer_dataset.table('customer').collect()
        assert len(outcome) == 149999

    def test_table_schema(self, spark_session_with_customer_dataset):
        schema = spark_session_with_customer_dataset.table('customer').schema
        assert len(schema) == 8

    def test_table_filter(self, spark_session_with_customer_dataset):
        customer_dataframe = spark_session_with_customer_dataset.table('customer')
        outcome = customer_dataframe.filter(col('c_mktsegment') == 'FURNITURE').collect()
        assert len(outcome) == 29968

    def test_create_or_replace_temp_view(self, spark_session):
        location_customer = str(Backend.find_tpch() / 'customer')
        df_customer = spark_session.read.parquet(location_customer)
        df_customer.createOrReplaceTempView("mytempview")
        outcome = spark_session.table('mytempview').collect()
        assert len(outcome) == 149999

    def test_create_or_replace_multiple_temp_views(self, spark_session):
        location_customer = str(Backend.find_tpch() / 'customer')
        df_customer = spark_session.read.parquet(location_customer)
        df_customer.createOrReplaceTempView("mytempview1")
        df_customer.createOrReplaceTempView("mytempview2")
        outcome1 = spark_session.table('mytempview1').collect()
        outcome2 = spark_session.table('mytempview2').collect()
        assert len(outcome1) == len(outcome2) == 149999
