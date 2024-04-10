# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

USE_GATEWAY = True


# pylint: disable=fixme
def get_customer_database(spark_session: SparkSession) -> DataFrame:
    # TODO -- Support reading schema from multiple files.
    location_customer = str(Path('../../../third_party/tpch/parquet/customer/part-0.parquet').absolute())

    return spark_session.read.parquet(location_customer,
                                      mergeSchema=False)


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


if __name__ == '__main__':
    if USE_GATEWAY:
        # TODO -- Make the port configurable.
        spark = SparkSession.builder.remote('sc://localhost:50051').getOrCreate()
    else:
        spark = SparkSession.builder.master('local').getOrCreate()
    execute_query(spark)
