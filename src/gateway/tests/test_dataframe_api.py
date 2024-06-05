# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
import pytest
from gateway.tests.conftest import find_tpch
from gateway.tests.plan_validator import utilizes_valid_plans
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.sql.functions import col, substring
from pyspark.testing import assertDataFrameEqual


@pytest.fixture(autouse=True)
def mark_dataframe_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue('source')
    if source == 'gateway-over-datafusion':
        pytest.importorskip("datafusion.substrait")


# pylint: disable=missing-function-docstring
# ruff: noqa: E712
class TestDataFrameAPI:
    """Tests of the dataframe side of SparkConnect."""

    def test_collect(self, users_dataframe):
        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.collect()

        assert len(outcome) == 100

    # pylint: disable=singleton-comparison
    def test_filter(self, users_dataframe):
        with utilizes_valid_plans(users_dataframe):
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
        with utilizes_valid_plans(users_dataframe):
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
        with utilizes_valid_plans(users_dataframe):
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
        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.count()

        assert outcome == 100

    def test_limit(self, users_dataframe):
        expected = [
            Row(user_id='user849118289', name='Brooke Jones', paid_for_service=False),
            Row(user_id='user954079192', name='Collin Frank', paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.limit(2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_with_column(self, users_dataframe):
        expected = [
            Row(user_id='user849118289', name='Brooke Jones', paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.withColumn('user_id', col('user_id')).limit(1).collect()

        assertDataFrameEqual(outcome, expected)

    def test_cast(self, users_dataframe):
        expected = [
            Row(user_id=849, name='Brooke Jones', paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.withColumn(
                'user_id',
                substring(col('user_id'), 5, 3).cast('integer')).limit(1).collect()

        assertDataFrameEqual(outcome, expected)

    def test_getattr(self, users_dataframe):
        expected = [
            Row(user_id='user669344115'),
            Row(user_id='user849118289'),
            Row(user_id='user954079192'),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(users_dataframe.user_id).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_getitem(self, users_dataframe):
        expected = [
            Row(user_id='user669344115'),
            Row(user_id='user849118289'),
            Row(user_id='user954079192'),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(users_dataframe['user_id']).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_column_getfield(self, spark_session):
        expected = [
            Row(answer='b'),
        ]
        expected2 = [
            Row(answer=1),
        ]

        df = spark_session.createDataFrame([Row(r=Row(a=1, b="b"))])
        with utilizes_valid_plans(df):
            outcome = df.select(df.r.getField("b")).collect()
            outcome2 = df.select(df.r.a).collect()

        assertDataFrameEqual(outcome, expected)
        assertDataFrameEqual(outcome2, expected2)

    def test_column_getitem(self, spark_session):
        expected = [
            Row(answer=1, answer2='value'),
        ]

        df = spark_session.createDataFrame([([1, 2], {"key": "value"})], ["l", "d"])
        with utilizes_valid_plans(df):
            outcome = df.select(df.l.getItem(0), df.d.getItem("key")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_join(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=5, n_name='ETHIOPIA', n_regionkey=0,
                n_comment='ven packages wake quickly. regu', s_suppkey=2,
                s_name='Supplier#000000002', s_address='89eJ5ksX3ImxJQBvxObC,', s_nationkey=5,
                s_phone='15-679-861-2259', s_acctbal=4032.68,
                s_comment=' slyly bold instructions. idle dependen'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            nat = nation.join(supplier, col('n_nationkey') == col('s_nationkey'))
            outcome = nat.filter(col('s_suppkey') == 2).limit(1).collect()

        assertDataFrameEqual(outcome, expected)

    def test_data_source_schema(self, spark_session):
        location_customer = str(find_tpch() / 'customer')
        schema = spark_session.read.parquet(location_customer).schema
        assert len(schema) == 8

    def test_data_source_filter(self, spark_session):
        location_customer = str(find_tpch() / 'customer')
        customer_dataframe = spark_session.read.parquet(location_customer)

        with utilizes_valid_plans(spark_session):
            outcome = customer_dataframe.filter(col('c_mktsegment') == 'FURNITURE').collect()

        assert len(outcome) == 29968

    def test_table(self, spark_session_with_tpch_dataset):
        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            outcome = spark_session_with_tpch_dataset.table('customer').collect()

        assert len(outcome) == 149999

    def test_table_schema(self, spark_session_with_tpch_dataset):
        schema = spark_session_with_tpch_dataset.table('customer').schema
        assert len(schema) == 8

    def test_table_filter(self, spark_session_with_tpch_dataset):
        customer_dataframe = spark_session_with_tpch_dataset.table('customer')

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            outcome = customer_dataframe.filter(col('c_mktsegment') == 'FURNITURE').collect()

        assert len(outcome) == 29968

    def test_create_or_replace_temp_view(self, spark_session):
        location_customer = str(find_tpch() / 'customer')
        df_customer = spark_session.read.parquet(location_customer)
        df_customer.createOrReplaceTempView("mytempview")

        with utilizes_valid_plans(spark_session):
            outcome = spark_session.table('mytempview').collect()

        assert len(outcome) == 149999

    def test_create_or_replace_multiple_temp_views(self, spark_session):
        location_customer = str(find_tpch() / 'customer')
        df_customer = spark_session.read.parquet(location_customer)
        df_customer.createOrReplaceTempView("mytempview1")
        df_customer.createOrReplaceTempView("mytempview2")

        with utilizes_valid_plans(spark_session):
            outcome1 = spark_session.table('mytempview1').collect()
            outcome2 = spark_session.table('mytempview2').collect()

        assert len(outcome1) == len(outcome2) == 149999
