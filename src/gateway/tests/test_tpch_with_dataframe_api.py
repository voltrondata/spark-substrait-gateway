# SPDX-License-Identifier: Apache-2.0
"""TPC-H Dataframe tests for the Spark to Substrait Gateway server."""
import datetime

import pyspark
import pytest
from gateway.tests.compare_dataframes import assert_dataframes_equal
from gateway.tests.plan_validator import utilizes_valid_plans
from pyspark import Row
from pyspark.sql.functions import avg, col, count, countDistinct, desc, try_sum, when


@pytest.fixture(autouse=True)
def mark_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue('source')
    originalname = request.keywords.node.originalname
    if source == 'gateway-over-duckdb':
        if originalname in ['test_query_15']:
            request.node.add_marker(pytest.mark.xfail(reason='No results (float vs decimal)'))
        elif originalname == 'test_query_16':
            request.node.add_marker(pytest.mark.xfail(reason='distinct not supported'))
    elif source == 'gateway-over-datafusion':
        pytest.importorskip("datafusion.substrait")
        if originalname in ['test_query_01', 'test_query_16', 'test_query_18']:
            request.node.add_marker(pytest.mark.xfail(reason='Results mismatch'))
        elif originalname in ['test_query_03', 'test_query_10', 'test_query_20']:
            request.node.add_marker(pytest.mark.xfail(reason='Schema mismatch'))
        elif originalname in ['test_query_07']:
            request.node.add_marker(pytest.mark.xfail(reason='Duplicate Expression names'))
        elif originalname in ['test_query_09']:
            request.node.add_marker(pytest.mark.xfail(
                reason='Cannot create filter with non-boolean predicate - substr function'))
        elif originalname in ['test_query_11']:
            request.node.add_marker(pytest.mark.xfail(reason='Duplicate field in schema'))
        elif originalname in ['test_query_08', 'test_query_14']:
            request.node.add_marker(pytest.mark.xfail(reason='Sum not implemented'))
        elif originalname in ['test_query_15']:
            request.node.add_marker(pytest.mark.xfail(reason='No results (float vs decimal)'))
        elif originalname in ['test_query_17']:
            request.node.add_marker(pytest.mark.xfail(reason='Avg not implemented'))


class TestTpchWithDataFrameAPI:
    """Runs the TPC-H standard test suite against the dataframe side of SparkConnect."""

    # pylint: disable=singleton-comparison
    def test_query_01(self, spark_session_with_tpch_dataset):
        expected = [
            Row(l_returnflag='A', l_linestatus='F', sum_qty=37734107.00,
                sum_base_price=56586554400.73, sum_disc_price=53758257134.87,
                sum_charge=55909065222.83, avg_qty=25.52,
                avg_price=38273.13, avg_disc=0.05, count_order=1478493),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            outcome = lineitem.filter(col('l_shipdate') <= '1998-09-02').groupBy(
                'l_returnflag', 'l_linestatus').agg(
                try_sum('l_quantity').alias('sum_qty'),
                try_sum('l_extendedprice').alias('sum_base_price'),
                try_sum(col('l_extendedprice') * (1 - col('l_discount'))).alias('sum_disc_price'),
                try_sum(
                    col('l_extendedprice') * (1 - col('l_discount')) * (1 + col('l_tax'))).alias(
                    'sum_charge'),
                avg('l_quantity').alias('avg_qty'),
                avg('l_extendedprice').alias('avg_price'),
                avg('l_discount').alias('avg_disc'),
                count('*').alias('count_order'))

            sorted_outcome = outcome.sort('l_returnflag', 'l_linestatus').limit(1).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_02(self, spark_session_with_tpch_dataset):
        expected = [
            Row(s_acctbal=9938.53, s_name='Supplier#000005359', n_name='UNITED KINGDOM',
                p_partkey=185358, p_mfgr='Manufacturer#4', s_address='QKuHYh,vZGiwu2FWEJoLDx04',
                s_phone='33-429-790-6131',
                s_comment='uriously regular requests hag'),
            Row(s_acctbal=9937.84, s_name='Supplier#000005969', n_name='ROMANIA',
                p_partkey=108438, p_mfgr='Manufacturer#1',
                s_address='ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa', s_phone='29-520-692-3537',
                s_comment='efully express instructions. regular requests against the slyly fin'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            part = spark_session_with_tpch_dataset.table('part')
            supplier = spark_session_with_tpch_dataset.table('supplier')
            partsupp = spark_session_with_tpch_dataset.table('partsupp')
            nation = spark_session_with_tpch_dataset.table('nation')
            region = spark_session_with_tpch_dataset.table('region')

            europe = region.filter(col('r_name') == 'EUROPE').join(
                nation, col('r_regionkey') == col('n_regionkey')).join(
                supplier, col('n_nationkey') == col('s_nationkey')).join(
                partsupp, col('s_suppkey') == col('ps_suppkey'))

            brass = part.filter((col('p_size') == 15) & (col('p_type').endswith('BRASS'))).join(
                europe, col('ps_partkey') == col('p_partkey'))

            minCost = brass.groupBy(col('ps_partkey')).agg(
                pyspark.sql.functions.min('ps_supplycost').alias('min'))

            outcome = brass.join(minCost, brass.ps_partkey == minCost.ps_partkey).filter(
                col('ps_supplycost') == col('min')).select('s_acctbal', 's_name', 'n_name',
                                                           'p_partkey',
                                                           'p_mfgr', 's_address', 's_phone',
                                                           's_comment')

            sorted_outcome = outcome.sort(
                desc('s_acctbal'), 'n_name', 's_name', 'p_partkey').limit(2).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_03(self, spark_session_with_tpch_dataset):
        expected = [
            Row(l_orderkey=2456423, revenue=406181.01, o_orderdate=datetime.date(1995, 3, 5),
                o_shippriority=0),
            Row(l_orderkey=3459808, revenue=405838.70, o_orderdate=datetime.date(1995, 3, 4),
                o_shippriority=0),
            Row(l_orderkey=492164, revenue=390324.06, o_orderdate=datetime.date(1995, 2, 19),
                o_shippriority=0),
            Row(l_orderkey=1188320, revenue=384537.94, o_orderdate=datetime.date(1995, 3, 9),
                o_shippriority=0),
            Row(l_orderkey=2435712, revenue=378673.06, o_orderdate=datetime.date(1995, 2, 26),
                o_shippriority=0),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            orders = spark_session_with_tpch_dataset.table('orders')

            fcust = customer.filter(col('c_mktsegment') == 'BUILDING')
            forders = orders.filter(col('o_orderdate') < '1995-03-15')
            flineitems = lineitem.filter(lineitem.l_shipdate > '1995-03-15')

            outcome = fcust.join(forders, col('c_custkey') == forders.o_custkey).select(
                'o_orderkey', 'o_orderdate', 'o_shippriority').join(
                flineitems, col('o_orderkey') == flineitems.l_orderkey).select(
                'l_orderkey',
                (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume'),
                'o_orderdate',
                'o_shippriority').groupBy('l_orderkey', 'o_orderdate', 'o_shippriority').agg(
                try_sum('volume').alias('revenue')).select(
                'l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority')

            sorted_outcome = outcome.sort(desc('revenue'), 'o_orderdate').limit(5).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_04(self, spark_session_with_tpch_dataset):
        expected = [
            Row(o_orderpriority='1-URGENT', order_count=10594),
            Row(o_orderpriority='2-HIGH', order_count=10476),
            Row(o_orderpriority='3-MEDIUM', order_count=10410),
            Row(o_orderpriority='4-NOT SPECIFIED', order_count=10556),
            Row(o_orderpriority='5-LOW', order_count=10487),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            orders = spark_session_with_tpch_dataset.table('orders')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')

            forders = orders.filter(
                (col('o_orderdate') >= '1993-07-01') & (col('o_orderdate') < '1993-10-01'))
            flineitems = lineitem.filter(col('l_commitdate') < col('l_receiptdate')).select(
                'l_orderkey').distinct()

            outcome = flineitems.join(
                forders,
                col('l_orderkey') == col('o_orderkey')).groupBy('o_orderpriority').agg(
                count('o_orderpriority').alias('order_count'))

            sorted_outcome = outcome.sort('o_orderpriority').collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_05(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_name='INDONESIA', revenue=55502041.17),
            Row(n_name='VIETNAM', revenue=55295087.00),
            Row(n_name='CHINA', revenue=53724494.26),
            Row(n_name='INDIA', revenue=52035512.00),
            Row(n_name='JAPAN', revenue=45410175.70),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            orders = spark_session_with_tpch_dataset.table('orders')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            nation = spark_session_with_tpch_dataset.table('nation')
            region = spark_session_with_tpch_dataset.table('region')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            forders = orders.filter(col('o_orderdate') >= '1994-01-01').filter(
                col('o_orderdate') < '1995-01-01')

            outcome = region.filter(col('r_name') == 'ASIA').join(  # r_name = 'ASIA'
                nation, col('r_regionkey') == col('n_regionkey')).join(
                supplier, col('n_nationkey') == col('s_nationkey')).join(
                lineitem, col('s_suppkey') == col('l_suppkey')).select(
                'n_name', 'l_extendedprice', 'l_discount', 'l_quantity', 'l_orderkey',
                's_nationkey').join(forders, col('l_orderkey') == forders.o_orderkey).join(
                customer, (col('o_custkey') == col('c_custkey')) & (
                        col('s_nationkey') == col('c_nationkey'))).select(
                'n_name',
                (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume')).groupBy(
                'n_name').agg(try_sum('volume').alias('revenue'))

            sorted_outcome = outcome.sort('revenue').collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_06(self, spark_session_with_tpch_dataset):
        expected = [
            Row(revenue=123141078.23),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')

            outcome = lineitem.filter((col('l_shipdate') >= '1994-01-01') &
                                      (col('l_shipdate') < '1995-01-01') &
                                      (col('l_discount') >= 0.05) &
                                      (col('l_discount') <= 0.07) &
                                      (col('l_quantity') < 24)).agg(
                try_sum(col('l_extendedprice') * col('l_discount'))).alias('revenue').collect()

        assert_dataframes_equal(outcome, expected)

    def test_query_07(self, spark_session_with_tpch_dataset):
        expected = [
            Row(supp_nation='FRANCE', cust_nation='GERMANY', l_year='1995', revenue=54639732.73),
            Row(supp_nation='FRANCE', cust_nation='GERMANY', l_year='1996', revenue=54633083.31),
            Row(supp_nation='GERMANY', cust_nation='FRANCE', l_year='1995', revenue=52531746.67),
            Row(supp_nation='GERMANY', cust_nation='FRANCE', l_year='1996', revenue=52520549.02),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            orders = spark_session_with_tpch_dataset.table('orders')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            supplier = spark_session_with_tpch_dataset.table('supplier')
            nation = spark_session_with_tpch_dataset.table('nation')

            fnation = nation.filter((nation.n_name == 'FRANCE') | (nation.n_name == 'GERMANY'))
            fline = lineitem.filter(
                (col('l_shipdate') >= '1995-01-01') & (col('l_shipdate') <= '1996-12-31'))

            suppNation = fnation.join(supplier, col('n_nationkey') == col('s_nationkey')).join(
                fline, col('s_suppkey') == col('l_suppkey')).select(
                col('n_name').alias('supp_nation'), 'l_orderkey', 'l_extendedprice', 'l_discount',
                'l_shipdate')

            outcome = fnation.join(customer, col('n_nationkey') == col('c_nationkey')).join(
                orders, col('c_custkey') == col('o_custkey')).select(
                col('n_name').alias('cust_nation'), 'o_orderkey').join(
                suppNation, col('o_orderkey') == suppNation.l_orderkey).filter(
                (col('supp_nation') == 'FRANCE') & (col('cust_nation') == 'GERMANY') | (
                        col('supp_nation') == 'GERMANY') & (col('cust_nation') == 'FRANCE')).select(
                'supp_nation', 'cust_nation', col('l_shipdate').substr(1, 4).alias('l_year'),
                (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume')).groupBy(
                'supp_nation', 'cust_nation', 'l_year').agg(
                try_sum('volume').alias('revenue'))

            sorted_outcome = outcome.sort('supp_nation', 'cust_nation', 'l_year').collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_08(self, spark_session_with_tpch_dataset):
        expected = [
            Row(o_year='1995', mkt_share=0.03),
            Row(o_year='1996', mkt_share=0.04),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            orders = spark_session_with_tpch_dataset.table('orders')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            nation = spark_session_with_tpch_dataset.table('nation')
            part = spark_session_with_tpch_dataset.table('part')
            region = spark_session_with_tpch_dataset.table('region')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            fregion = region.filter(col('r_name') == 'AMERICA')
            forder = orders.filter((col('o_orderdate') >= '1995-01-01') & (
                    col('o_orderdate') <= '1996-12-31'))
            fpart = part.filter(col('p_type') == 'ECONOMY ANODIZED STEEL')

            nat = nation.join(supplier, col('n_nationkey') == col('s_nationkey'))

            line = lineitem.select(
                'l_partkey', 'l_suppkey', 'l_orderkey',
                (col('l_extendedprice') * (1 - col('l_discount'))).alias(
                    'volume')).join(
                fpart, col('l_partkey') == fpart.p_partkey).join(
                nat, col('l_suppkey') == nat.s_suppkey)

            outcome = nation.join(fregion, col('n_regionkey') == fregion.r_regionkey).select(
                'n_nationkey', 'n_name').join(customer,
                                              col('n_nationkey') == col('c_nationkey')).select(
                'c_custkey').join(forder, col('c_custkey') == col('o_custkey')).select(
                'o_orderkey', 'o_orderdate').join(line,
                                                  col('o_orderkey') == line.l_orderkey).select(
                col('n_name'), col('o_orderdate').substr(1, 4).alias('o_year'),
                col('volume')).withColumn('case_volume',
                                          when(col('n_name') == 'BRAZIL', col('volume')).otherwise(
                                              0)).groupBy('o_year').agg(
                (try_sum('case_volume') / try_sum('volume')).alias('mkt_share'))

            sorted_outcome = outcome.sort('o_year').collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_09(self, spark_session_with_tpch_dataset):
        # TODO -- Verify the correctness of these results against another version of the dataset.
        expected = [
            Row(n_name='ARGENTINA', o_year='1998', sum_profit=28341663.78),
            Row(n_name='ARGENTINA', o_year='1997', sum_profit=47143964.12),
            Row(n_name='ARGENTINA', o_year='1996', sum_profit=45255278.60),
            Row(n_name='ARGENTINA', o_year='1995', sum_profit=45631769.21),
            Row(n_name='ARGENTINA', o_year='1994', sum_profit=48268856.35),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            orders = spark_session_with_tpch_dataset.table('orders')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            nation = spark_session_with_tpch_dataset.table('nation')
            part = spark_session_with_tpch_dataset.table('part')
            partsupp = spark_session_with_tpch_dataset.table('partsupp')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            linePart = part.filter(col('p_name').contains('green')).join(
                lineitem, col('p_partkey') == lineitem.l_partkey)
            natSup = nation.join(supplier, col('n_nationkey') == supplier.s_nationkey)

            outcome = linePart.join(natSup, col('l_suppkey') == natSup.s_suppkey).join(
                partsupp, (col('l_suppkey') == partsupp.ps_suppkey) & (
                        col('l_partkey') == partsupp.ps_partkey)).join(
                orders, col('l_orderkey') == orders.o_orderkey).select(
                'n_name', col('o_orderdate').substr(1, 4).alias('o_year'),
                (col('l_extendedprice') * (1 - col('l_discount')) - (
                        col('ps_supplycost') * col('l_quantity'))).alias('amount')).groupBy(
                'n_name', 'o_year').agg(try_sum('amount').alias('sum_profit'))

            sorted_outcome = outcome.sort('n_name', desc('o_year')).limit(5).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_10(self, spark_session_with_tpch_dataset):
        expected = [
            Row(c_custkey=57040, c_name='Customer#000057040', revenue=734235.25,
                c_acctbal=632.87, n_name='JAPAN', c_address='Eioyzjf4pp',
                c_phone='22-895-641-3466',
                c_comment='sits. slyly regular requests sleep alongside of the regular inst'),
            Row(c_custkey=143347, c_name='Customer#000143347', revenue=721002.69,
                c_acctbal=2557.47, n_name='EGYPT', c_address='1aReFYv,Kw4',
                c_phone='14-742-935-3718',
                c_comment='ggle carefully enticing requests. final deposits use bold, bold '
                          'pinto beans. ironic, idle re'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            nation = spark_session_with_tpch_dataset.table('nation')
            orders = spark_session_with_tpch_dataset.table('orders')

            flineitem = lineitem.filter(col('l_returnflag') == 'R')

            outcome = orders.filter(
                (col('o_orderdate') >= '1993-10-01') & (col('o_orderdate') < '1994-01-01')).join(
                customer, col('o_custkey') == customer.c_custkey).join(
                nation, col('c_nationkey') == nation.n_nationkey).join(
                flineitem, col('o_orderkey') == flineitem.l_orderkey).select(
                'c_custkey', 'c_name',
                (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume'),
                'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment').groupBy(
                'c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address',
                'c_comment').agg(
                try_sum('volume').alias('revenue')).select(
                'c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 'c_address', 'c_phone',
                'c_comment')

            sorted_outcome = outcome.sort(desc('revenue')).limit(2).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_11(self, spark_session_with_tpch_dataset):
        expected = [
            Row(ps_partkey=129760, part_value=17538456.86),
            Row(ps_partkey=166726, part_value=16503353.92),
            Row(ps_partkey=191287, part_value=16474801.97),
            Row(ps_partkey=161758, part_value=16101755.54),
            Row(ps_partkey=34452, part_value=15983844.72),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            nation = spark_session_with_tpch_dataset.table('nation')
            partsupp = spark_session_with_tpch_dataset.table('partsupp')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            tmp = nation.filter(col('n_name') == 'GERMANY').join(
                supplier, col('n_nationkey') == supplier.s_nationkey).select(
                's_suppkey').join(partsupp, col('s_suppkey') == partsupp.ps_suppkey).select(
                'ps_partkey', (col('ps_supplycost') * col('ps_availqty')).alias('value'))

            sumRes = tmp.agg(try_sum('value').alias('total_value'))

            outcome = tmp.groupBy('ps_partkey').agg(
                (try_sum('value')).alias('part_value')).join(
                sumRes, col('part_value') > col('total_value') * 0.0001)

            sorted_outcome = outcome.sort(desc('part_value')).limit(5).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_12(self, spark_session_with_tpch_dataset):
        expected = [
            Row(l_shipmode='MAIL', high_line_count=6202, low_line_count=9324),
            Row(l_shipmode='SHIP', high_line_count=6200, low_line_count=9262),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            orders = spark_session_with_tpch_dataset.table('orders')

            outcome = lineitem.filter(
                (col('l_shipmode') == 'MAIL') | (col('l_shipmode') == 'SHIP')).filter(
                (col('l_commitdate') < col('l_receiptdate')) &
                (col('l_shipdate') < col('l_commitdate')) &
                (col('l_receiptdate') >= '1994-01-01') & (
                        col('l_receiptdate') < '1995-01-01')).join(
                orders,
                col('l_orderkey') == orders.o_orderkey).select(
                'l_shipmode', 'o_orderpriority').groupBy('l_shipmode').agg(
                count(
                    when((col('o_orderpriority') == '1-URGENT') | (
                            col('o_orderpriority') == '2-HIGH'),
                         True)).alias('high_line_count'),
                count(
                    when((col('o_orderpriority') != '1-URGENT') & (
                            col('o_orderpriority') != '2-HIGH'),
                         True)).alias('low_line_count'))

            sorted_outcome = outcome.sort('l_shipmode').collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_13(self, spark_session_with_tpch_dataset):
        # TODO -- Verify the corretness of these results against another version of the dataset.
        expected = [
            Row(c_count=9, custdist=6641),
            Row(c_count=10, custdist=6532),
            Row(c_count=11, custdist=6014),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            orders = spark_session_with_tpch_dataset.table('orders')

            outcome = customer.join(
                orders, (col('c_custkey') == orders.o_custkey) & (
                    ~col('o_comment').rlike('.*special.*requests.*')), 'left_outer').groupBy(
                'o_custkey').agg(count('o_orderkey').alias('c_count')).groupBy(
                'c_count').agg(count('o_custkey').alias('custdist'))

            sorted_outcome = outcome.sort(desc('custdist'), desc('c_count')).limit(3).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_14(self, spark_session_with_tpch_dataset):
        expected = [
            Row(promo_revenue=16.38),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            part = spark_session_with_tpch_dataset.table('part')

            outcome = part.join(lineitem, (col('l_partkey') == col('p_partkey')) &
                                (col('l_shipdate') >= '1995-09-01') &
                                (col('l_shipdate') < '1995-10-01')).select(
                'p_type', (col('l_extendedprice') * (1 - col('l_discount'))).alias('value')).agg(
                try_sum(when(col('p_type').contains('PROMO'), col('value'))) * 100 / try_sum(
                    col('value'))
            ).alias('promo_revenue').collect()

        assert_dataframes_equal(outcome, expected)

    def test_query_15(self, spark_session_with_tpch_dataset):
        expected = [
            Row(s_suppkey=8449, s_name='Supplier#000008449', s_address='Wp34zim9qYFbVctdW',
                s_phone='20-469-856-8873', total=1772627.21),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            revenue = lineitem.filter((col('l_shipdate') >= '1996-01-01') &
                                      (col('l_shipdate') < '1996-04-01')).select(
                'l_suppkey',
                (col('l_extendedprice') * (1 - col('l_discount'))).alias('value')).groupBy(
                'l_suppkey').agg(try_sum('value').alias('total'))

            outcome = revenue.agg(pyspark.sql.functions.max(col('total')).alias('max_total')).join(
                revenue, col('max_total') == revenue.total).join(
                supplier, col('l_suppkey') == supplier.s_suppkey).select(
                's_suppkey', 's_name', 's_address', 's_phone', 'total')

            sorted_outcome = outcome.sort('s_suppkey').limit(1).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_16(self, spark_session_with_tpch_dataset):
        expected = [
            Row(p_brand='Brand#41', p_type='MEDIUM BRUSHED TIN', p_size=3, supplier_cnt=28),
            Row(p_brand='Brand#54', p_type='STANDARD BRUSHED COPPER', p_size=14, supplier_cnt=27),
            Row(p_brand='Brand#11', p_type='STANDARD BRUSHED TIN', p_size=23, supplier_cnt=24),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            part = spark_session_with_tpch_dataset.table('part')
            partsupp = spark_session_with_tpch_dataset.table('partsupp')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            fparts = part.filter((col('p_brand') != 'Brand#45') &
                                 (~col('p_type').startswith('MEDIUM POLISHED')) &
                                 (col('p_size').isin([3, 14, 23, 45, 49, 9, 19, 36]))).select(
                'p_partkey', 'p_brand', 'p_type', 'p_size')

            outcome = supplier.filter(~col('s_comment').rlike('.*Customer.*Complaints.*')).join(
                partsupp, col('s_suppkey') == partsupp.ps_suppkey).select(
                'ps_partkey', 'ps_suppkey').join(
                fparts, col('ps_partkey') == fparts.p_partkey).groupBy(
                'p_brand', 'p_type', 'p_size').agg(
                countDistinct('ps_suppkey').alias('supplier_cnt'))

            sorted_outcome = outcome.sort(
                desc('supplier_cnt'), 'p_brand', 'p_type', 'p_size').limit(3).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_17(self, spark_session_with_tpch_dataset):
        expected = [
            Row(avg_yearly=348406.02),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            part = spark_session_with_tpch_dataset.table('part')

            fpart = part.filter(
                (col('p_brand') == 'Brand#23') & (col('p_container') == 'MED BOX')).select(
                'p_partkey').join(lineitem, col('p_partkey') == lineitem.l_partkey, 'left_outer')

            outcome = fpart.groupBy('p_partkey').agg(
                (avg('l_quantity') * 0.2).alias('avg_quantity')).select(
                col('p_partkey').alias('key'), 'avg_quantity').join(
                fpart, col('key') == fpart.p_partkey).filter(
                col('l_quantity') < col('avg_quantity')).agg(
                try_sum('l_extendedprice') / 7).alias('avg_yearly').collect()

        assert_dataframes_equal(outcome, expected)

    def test_query_18(self, spark_session_with_tpch_dataset):
        expected = [
            Row(c_name='Customer#000128120', c_custkey=128120, o_orderkey=4722021,
                o_orderdate=datetime.date(1994, 4, 7),
                o_totalprice=544089.09, sum_l_quantity=323.00),
            Row(c_name='Customer#000144617', c_custkey=144617, o_orderkey=3043270,
                o_orderdate=datetime.date(1997, 2, 12),
                o_totalprice=530604.44, sum_l_quantity=317.00),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            orders = spark_session_with_tpch_dataset.table('orders')

            outcome = lineitem.groupBy('l_orderkey').agg(
                try_sum('l_quantity').alias('sum_quantity')).filter(
                col('sum_quantity') > 300).select(col('l_orderkey').alias('key'),
                                                  'sum_quantity').join(
                orders, orders.o_orderkey == col('key')).join(
                lineitem, col('o_orderkey') == lineitem.l_orderkey).join(
                customer, col('o_custkey') == customer.c_custkey).select(
                'l_quantity', 'c_name', 'c_custkey', 'o_orderkey', 'o_orderdate',
                'o_totalprice').groupBy(
                'c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice').agg(
                try_sum('l_quantity').alias('sum_l_quantity'))

            sorted_outcome = outcome.sort(desc('o_totalprice'), 'o_orderdate').limit(2).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_19(self, spark_session_with_tpch_dataset):
        expected = [
            Row(revenue=3083843.06),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            part = spark_session_with_tpch_dataset.table('part')

            outcome = part.join(lineitem, col('l_partkey') == col('p_partkey')).filter(
                col('l_shipmode').isin(['AIR', 'AIR REG']) & (
                        col('l_shipinstruct') == 'DELIVER IN PERSON')).filter(
                ((col('p_brand') == 'Brand#12') & (
                    col('p_container').isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
                 (col('l_quantity') >= 1) & (col('l_quantity') <= 11) &
                 (col('p_size') >= 1) & (col('p_size') <= 5)) |
                ((col('p_brand') == 'Brand#23') & (
                    col('p_container').isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
                 (col('l_quantity') >= 10) & (col('l_quantity') <= 20) &
                 (col('p_size') >= 1) & (col('p_size') <= 10)) |
                ((col('p_brand') == 'Brand#34') & (
                    col('p_container').isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
                 (col('l_quantity') >= 20) & (col('l_quantity') <= 30) &
                 (col('p_size') >= 1) & (col('p_size') <= 15))).select(
                (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume')).agg(
                try_sum('volume').alias('revenue')).collect()

        assert_dataframes_equal(outcome, expected)

    def test_query_20(self, spark_session_with_tpch_dataset):
        expected = [
            Row(s_name='Supplier#000000020', s_address='iybAE,RmTymrZVYaFZva2SH,j'),
            Row(s_name='Supplier#000000091', s_address='YV45D7TkfdQanOOZ7q9QxkyGUapU1oOWU6q3'),
            Row(s_name='Supplier#000000205', s_address='rF uV8d0JNEk'),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            nation = spark_session_with_tpch_dataset.table('nation')
            part = spark_session_with_tpch_dataset.table('part')
            partsupp = spark_session_with_tpch_dataset.table('partsupp')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            flineitem = lineitem.filter(
                (col('l_shipdate') >= '1994-01-01') & (col('l_shipdate') < '1995-01-01')).groupBy(
                'l_partkey', 'l_suppkey').agg(
                try_sum(col('l_quantity') * 0.5).alias('sum_quantity'))

            fnation = nation.filter(col('n_name') == 'CANADA')
            nat_supp = supplier.select('s_suppkey', 's_name', 's_nationkey', 's_address').join(
                fnation, col('s_nationkey') == fnation.n_nationkey)

            outcome = part.filter(col('p_name').startswith('forest')).select('p_partkey').join(
                partsupp, col('p_partkey') == partsupp.ps_partkey).join(
                flineitem, (col('ps_suppkey') == flineitem.l_suppkey) & (
                        col('ps_partkey') == flineitem.l_partkey)).filter(
                col('ps_availqty') > col('sum_quantity')).select('ps_suppkey').distinct().join(
                nat_supp, col('ps_suppkey') == nat_supp.s_suppkey).select('s_name', 's_address')

            sorted_outcome = outcome.sort('s_name').limit(3).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_21(self, spark_session_with_tpch_dataset):
        # TODO -- Verify the corretness of these results against another version of the dataset.
        expected = [
            Row(s_name='Supplier#000002095', numwait=26),
            Row(s_name='Supplier#000003063', numwait=26),
            Row(s_name='Supplier#000006384', numwait=26),
            Row(s_name='Supplier#000006450', numwait=26),
            Row(s_name='Supplier#000000486', numwait=25),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            lineitem = spark_session_with_tpch_dataset.table('lineitem')
            nation = spark_session_with_tpch_dataset.table('nation')
            orders = spark_session_with_tpch_dataset.table('orders')
            supplier = spark_session_with_tpch_dataset.table('supplier')

            fsupplier = supplier.select('s_suppkey', 's_nationkey', 's_name')

            plineitem = lineitem.select('l_suppkey', 'l_orderkey', 'l_receiptdate', 'l_commitdate')

            flineitem = plineitem.filter(col('l_receiptdate') > col('l_commitdate'))

            line1 = plineitem.groupBy('l_orderkey').agg(
                countDistinct('l_suppkey').alias('suppkey_count'),
                pyspark.sql.functions.max(col('l_suppkey')).alias('suppkey_max')).select(
                col('l_orderkey').alias('key'), 'suppkey_count', 'suppkey_max')

            line2 = flineitem.groupBy('l_orderkey').agg(
                countDistinct('l_suppkey').alias('suppkey_count'),
                pyspark.sql.functions.max(col('l_suppkey')).alias('suppkey_max')).select(
                col('l_orderkey').alias('key'), 'suppkey_count', 'suppkey_max')

            forder = orders.select('o_orderkey', 'o_orderstatus').filter(
                col('o_orderstatus') == 'F')

            outcome = nation.filter(col('n_name') == 'SAUDI ARABIA').join(
                fsupplier, col('n_nationkey') == fsupplier.s_nationkey).join(
                flineitem, col('s_suppkey') == flineitem.l_suppkey).join(
                forder, col('l_orderkey') == forder.o_orderkey).join(
                line1, col('l_orderkey') == line1.key).filter(
                (col('suppkey_count') > 1) |
                ((col('suppkey_count') == 1) & (col('l_suppkey') == col('suppkey_max')))).select(
                's_name', 'l_orderkey', 'l_suppkey').join(
                line2, col('l_orderkey') == line2.key, 'left_outer').select(
                's_name', 'l_orderkey', 'l_suppkey', 'suppkey_count', 'suppkey_max').filter(
                (col('suppkey_count') == 1) & (col('l_suppkey') == col('suppkey_max'))).groupBy(
                's_name').agg(count(col('l_suppkey')).alias('numwait'))

            sorted_outcome = outcome.sort(desc('numwait'), 's_name').limit(5).collect()

        assert_dataframes_equal(sorted_outcome, expected)

    def test_query_22(self, spark_session_with_tpch_dataset):
        expected = [
            Row(cntrycode='13', numcust=888, totacctbal=6737713.99),
            Row(cntrycode='17', numcust=861, totacctbal=6460573.72),
            Row(cntrycode='18', numcust=964, totacctbal=7236687.40),
            Row(cntrycode='23', numcust=892, totacctbal=6701457.95),
            Row(cntrycode='29', numcust=948, totacctbal=7158866.63),
            Row(cntrycode='30', numcust=909, totacctbal=6808436.13),
            Row(cntrycode='31', numcust=922, totacctbal=6806670.18),
        ]

        with utilizes_valid_plans(spark_session_with_tpch_dataset):
            customer = spark_session_with_tpch_dataset.table('customer')
            orders = spark_session_with_tpch_dataset.table('orders')

            fcustomer = customer.select(
                'c_acctbal', 'c_custkey', (col('c_phone').substr(1, 2)).alias('cntrycode')).filter(
                col('cntrycode').isin(['13', '31', '23', '29', '30', '18', '17']))

            avg_customer = fcustomer.filter(col('c_acctbal') > 0.00).agg(
                avg('c_acctbal').alias('avg_acctbal'))

            outcome = orders.groupBy('o_custkey').agg(
                count('o_custkey')).select('o_custkey').join(
                fcustomer, col('o_custkey') == fcustomer.c_custkey, 'right_outer').filter(
                col('o_custkey').isNull()).join(avg_customer).filter(
                col('c_acctbal') > col('avg_acctbal')).groupBy('cntrycode').agg(
                count('c_custkey').alias('numcust'), try_sum('c_acctbal'))

            sorted_outcome = outcome.sort('cntrycode').collect()

        assert_dataframes_equal(sorted_outcome, expected)
