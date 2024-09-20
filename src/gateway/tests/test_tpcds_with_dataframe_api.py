# SPDX-License-Identifier: Apache-2.0
"""TPC-DS Dataframe tests for the Spark to Substrait Gateway server."""

import pytest
from pyspark import Row
from pyspark.sql import Window
from pyspark.sql.functions import avg, col, try_sum

from gateway.tests.compare_dataframes import assert_dataframes_equal
from gateway.tests.plan_validator import utilizes_valid_plans


@pytest.fixture(autouse=True)
def mark_tests_as_xfail(request):
    """Marks a subset of tests as expected to fail."""
    source = request.getfixturevalue("source")
    originalname = request.keywords.node.originalname
    if source == "gateway-over-duckdb" and originalname == "test_query_01":
        pytest.skip(reason="Unsupported expression type 5")


class TestTpcdsWithDataFrameAPI:
    """Runs the TPC-DS standard test suite against the dataframe side of SparkConnect."""

    def test_query_01(self, register_tpcds_dataset, spark_session):
        expected = [
            Row(c_customer_id='AAAAAAAAAAACAAAA'),
            Row(c_customer_id='AAAAAAAAAABAAAAA'),
            Row(c_customer_id='AAAAAAAAAADAAAAA'),
        ]

        with (utilizes_valid_plans(spark_session)):
            store_returns = spark_session.table("store_returns")
            date_dim = spark_session.table("date_dim")
            customer = spark_session.table("customer")
            store = spark_session.table("store")

            window = Window.partitionBy("ctr_store_sk")
            customer_total_return = (
                store_returns
                .join(date_dim, col("sr_returned_date_sk") == date_dim.d_date_sk)
                .where(col("d_year") == 2001)
                .select("sr_customer_sk", "sr_store_sk", "sr_returned_date_sk", "sr_return_amt")
                .withColumnsRenamed(
                    {"sr_customer_sk": "ctr_customer_sk",
                     "sr_store_sk": "ctr_store_sk"})
                .groupBy("ctr_customer_sk", "ctr_store_sk")
                .agg(try_sum("sr_return_amt").alias("ctr_total_return"))
                .select("ctr_customer_sk", "ctr_store_sk", "ctr_total_return")
            ).withColumn("ctr_avg_total_return", avg("ctr_total_return").over(window))
            outcome = (
                customer
                .join(customer_total_return, col("c_customer_sk") == col("ctr_customer_sk"))
                .join(store, col("ctr_store_sk") == col("s_store_sk"))
                .where(col("s_state") == "TN")
                .where(col("ctr_total_return") > col("ctr_avg_total_return") * 6 / 5)
                .select("c_customer_id")
            )

            sorted_outcome = outcome.sort("c_customer_id").limit(3).collect()

            assert_dataframes_equal(sorted_outcome, expected)
