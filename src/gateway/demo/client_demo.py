# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""

from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame

from gateway.converter.sql_to_substrait import find_tpch

USE_GATEWAY = True


# pylint: disable=fixme
def get_customer_database(spark_session: SparkSession) -> DataFrame:
    location_customer = str(find_tpch() / 'customer')

    return spark_session.read.parquet(location_customer, mergeSchema=False)


# pylint: disable=fixme
def execute_query(spark_session: SparkSession) -> None:
    """Runs a single sample query against the gateway."""
    df_customer = get_customer_database(spark_session)

    # TODO -- Enable after named table registration is implemented.
    # df_customer.createOrReplaceTempView('customer')

    # pylint: disable=singleton-comparison
    df_result = df_customer \
        .filter(col('c_mktsegment') == 'FURNITURE') \
        .sort(col('c_name')) \
        .limit(10)

    df_result.show()

    sql_results = spark_session.sql(
        'SELECT c_custkey, c_phone, c_mktsegment FROM customer LIMIT 5').collect()
    print(sql_results)


if __name__ == '__main__':
    if USE_GATEWAY:
        # TODO -- Make the port configurable.
        spark = SparkSession.builder.remote('sc://localhost:50051').getOrCreate()
    else:
        spark = SparkSession.builder.master('local').getOrCreate()
    execute_query(spark)
