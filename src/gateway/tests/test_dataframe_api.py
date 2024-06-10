# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
import pyarrow as pa
import pytest
from gateway.tests.conftest import find_tpch
from gateway.tests.plan_validator import utilizes_valid_plans
from hamcrest import assert_that, equal_to
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark import Row
from pyspark.sql.functions import col, substring
from pyspark.testing import assertDataFrameEqual


@pytest.fixture(autouse=True)
def mark_dataframe_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue('source')
    originalname = request.keywords.node.originalname
    if source == 'gateway-over-datafusion':
        pytest.importorskip("datafusion.substrait")
        if originalname in ['test_column_getfield', 'test_column_getitem']:
            request.node.add_marker(pytest.mark.xfail(reason='structs not handled'))
    elif originalname == 'test_column_getitem':
        request.node.add_marker(pytest.mark.xfail(reason='maps and lists not handled'))
    elif source == 'spark' and originalname == 'test_subquery_alias':
        pytest.xfail('Spark supports subquery_alias but everyone else does not')


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

    def test_alias(self, users_dataframe):
        expected = [
            Row(foo='user849118289'),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(col('user_id').alias('foo')).limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == ['foo']

    def test_subquery_alias(self, users_dataframe):
        with pytest.raises(Exception) as exc_info:
            users_dataframe.select(col('user_id')).alias('foo').limit(1).collect()

        assert exc_info.match('Subquery alias relations are not yet implemented')

    def test_name(self, users_dataframe):
        expected = [
            Row(foo='user849118289'),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(col('user_id').name('foo')).limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == ['foo']

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

    def test_column_getfield(self, spark_session, caplog):
        expected = [
            Row(answer='b', answer2=1),
        ]

        struct_type = pa.struct([('a', pa.int64()), ('b', pa.string())])
        data = [
            {'a': 1, 'b': 'b'}
        ]
        struct_array = pa.array(data, type=struct_type)
        table = pa.Table.from_arrays([struct_array], names=['r'])

        pq.write_table(table, 'test_table.parquet')
        table_df = spark_session.read.parquet('test_table.parquet')
        table_df.createOrReplaceTempView('mytesttable')
        df = spark_session.table('mytesttable')

        with utilizes_valid_plans(df):
            outcome = df.select(df.r.getField("b"), df.r.a).collect()

        assertDataFrameEqual(outcome, expected)

    def test_column_getitem(self, spark_session):
        expected = [
            Row(answer=1, answer2='value'),
        ]

        list_array = pa.array([[1, 2]], type=pa.list_(pa.int64()))
        map_array = pa.array([{"key": "value"}],
                             type=pa.map_(pa.string(), pa.string(), False))
        table = pa.Table.from_arrays([list_array, map_array], names=['l', 'd'])

        pq.write_table(table, 'test_table.parquet')
        table_df = spark_session.read.parquet('test_table.parquet')
        table_df.createOrReplaceTempView('mytesttable')
        df = spark_session.table('mytesttable')

        with utilizes_valid_plans(df):
            outcome = df.select(df.l.getItem(0), df.d.getItem("key")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_astype(self, users_dataframe):
        expected = [
            Row(user_id=849, name='Brooke Jones', paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.withColumn(
                'user_id',
                substring(col('user_id'), 5, 3).astype('integer')).limit(1).collect()

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

    def test_crossjoin(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=1, n_name='ARGENTINA', s_name='Supplier#000000002'),
            Row(n_nationkey=2, n_name='BRAZIL', s_name='Supplier#000000002'),
            Row(n_nationkey=3, n_name='CANADA', s_name='Supplier#000000002'),
            Row(n_nationkey=4, n_name='EGYPT', s_name='Supplier#000000002'),
            Row(n_nationkey=5, n_name='ETHIOPIA', s_name='Supplier#000000002'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            nat = nation.crossJoin(supplier).filter(col('s_suppkey') == 2)
            outcome = nat.select('n_nationkey', 'n_name', 's_name').limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_union(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=23, n_name='UNITED KINGDOM', n_regionkey=3,
                n_comment='eans boost carefully special requests. accounts are. carefull'),
            Row(n_nationkey=23, n_name='UNITED KINGDOM', n_regionkey=3,
                n_comment='eans boost carefully special requests. accounts are. carefull'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation')

            outcome = nation.union(nation).filter(col('n_nationkey') == 23).collect()

        assertDataFrameEqual(outcome, expected)

    def test_union_distinct(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=23, n_name='UNITED KINGDOM', n_regionkey=3,
                n_comment='eans boost carefully special requests. accounts are. carefull'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation')

            outcome = nation.union(nation).distinct().filter(col('n_nationkey') == 23).collect()

        assertDataFrameEqual(outcome, expected)

    def test_unionall(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=23, n_name='UNITED KINGDOM', n_regionkey=3,
                n_comment='eans boost carefully special requests. accounts are. carefull'),
            Row(n_nationkey=23, n_name='UNITED KINGDOM', n_regionkey=3,
                n_comment='eans boost carefully special requests. accounts are. carefull'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation')

            outcome = nation.unionAll(nation).filter(col('n_nationkey') == 23).collect()

        assertDataFrameEqual(outcome, expected)

    def test_exceptall(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=21, n_name='VIETNAM', n_regionkey=2,
                n_comment='hely enticingly express accounts. even, final '),
            Row(n_nationkey=21, n_name='VIETNAM', n_regionkey=2,
                n_comment='hely enticingly express accounts. even, final '),
            Row(n_nationkey=22, n_name='RUSSIA', n_regionkey=3,
                n_comment=' requests against the platelets use never according to the '
                          'quickly regular pint'),
            Row(n_nationkey=22, n_name='RUSSIA', n_regionkey=3,
                n_comment=' requests against the platelets use never according to the '
                          'quickly regular pint'),
            Row(n_nationkey=23, n_name='UNITED KINGDOM', n_regionkey=3,
                n_comment='eans boost carefully special requests. accounts are. carefull'),
            Row(n_nationkey=24, n_name='UNITED STATES', n_regionkey=1,
                n_comment='y final packages. slow foxes cajole quickly. quickly silent platelets '
                          'breach ironic accounts. unusual pinto be'),
            Row(n_nationkey=24, n_name='UNITED STATES', n_regionkey=1,
                n_comment='y final packages. slow foxes cajole quickly. quickly silent platelets '
                          'breach ironic accounts. unusual pinto be'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation').filter(col('n_nationkey') > 20)
            nation1 = nation.union(nation)
            nation2 = nation.filter(col('n_nationkey') == 23)

            outcome = nation1.exceptAll(nation2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_subtract(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_nationkey=21, n_name='VIETNAM', n_regionkey=2,
                n_comment='hely enticingly express accounts. even, final '),
            Row(n_nationkey=22, n_name='RUSSIA', n_regionkey=3,
                n_comment=' requests against the platelets use never according to the '
                          'quickly regular pint'),
            Row(n_nationkey=24, n_name='UNITED STATES', n_regionkey=1,
                n_comment='y final packages. slow foxes cajole quickly. quickly silent platelets '
                          'breach ironic accounts. unusual pinto be'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation').filter(col('n_nationkey') > 20)
            nation1 = nation.union(nation)
            nation2 = nation.filter(col('n_nationkey') == 23)

            outcome = nation1.subtract(nation2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_unionbyname(self, spark_session):
        expected = [
            Row(a=1, b=2, c=3, d=None),
            Row(a=None, b=4, c=5, d=6),
        ]

        int1_array = pa.array([1], type=pa.int32())
        int2_array = pa.array([2], type=pa.int32())
        int3_array = pa.array([3], type=pa.int32())
        table = pa.Table.from_arrays([int1_array, int2_array, int3_array], names=['a', 'b', 'c'])
        int4_array = pa.array([4], type=pa.int32())
        int5_array = pa.array([5], type=pa.int32())
        int6_array = pa.array([6], type=pa.int32())
        table2 = pa.Table.from_arrays([int4_array, int5_array, int6_array], names=['b', 'c', 'd'])

        pq.write_table(table, 'test_table1.parquet')
        table_df = spark_session.read.parquet('test_table1.parquet')
        table_df.createOrReplaceTempView('mytesttable1')
        df = spark_session.table('mytesttable1')

        pq.write_table(table2, 'test_table2.parquet')
        table_df2 = spark_session.read.parquet('test_table2.parquet')
        table_df2.createOrReplaceTempView('mytesttable2')
        df2 = spark_session.table('mytesttable2')

        with utilizes_valid_plans(df):
            outcome = df.unionByName(df2, allowMissingColumns=True).collect()
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
