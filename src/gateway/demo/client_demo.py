# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""
import os
from pathlib import Path

import click
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from gateway.config import SERVER_PORT
import logging

_LOGGER = logging.getLogger(__name__)


def find_tpch() -> Path:
    """Find the location of the TPCH dataset."""
    location = Path('third_party') / 'tpch' / 'parquet'
    if location.exists():
        return location
    raise ValueError('TPCH dataset not found')


# pylint: disable=fixme
def get_customer_database(spark_session: SparkSession) -> DataFrame:
    """Register the TPC-H customer database."""
    location_customer = str(find_tpch() / 'customer')

    return spark_session.read.parquet(location_customer, mergeSchema=False)


# pylint: disable=fixme
# ruff: noqa: T201
def execute_query(spark_session: SparkSession) -> None:
    """Run a single sample query against the gateway."""
    df_customer = get_customer_database(spark_session)

    df_customer.createOrReplaceTempView('customer')

    # pylint: disable=singleton-comparison
    df_result = df_customer \
        .filter(col('c_mktsegment') == 'FURNITURE') \
        .sort(col('c_name')) \
        .limit(10)

    df_result.show()

    sql_results = spark_session.sql(
        'SELECT c_custkey, c_phone, c_mktsegment FROM customer LIMIT 5').collect()
    print(sql_results)


@click.command()
@click.option(
    "--use-gateway/--no-use-gateway",
    type=bool,
    default=True,
    show_default=True,
    required=True,
    help="Use the gateway service."
)
@click.option(
    "--host",
    type=str,
    default="localhost",
    show_default=True,
    required=True,
    help="The gateway hostname."
)
@click.option(
    "--port",
    type=int,
    default=os.getenv("SERVER_PORT", SERVER_PORT),
    show_default=True,
    required=True,
    help="Run the Spark Substrait Gateway server on this port."
)
@click.option(
    "--use-tls/--no-use-tls",
    type=bool,
    default=False,
    required=True,
    help="Enable transport-level security (TLS/SSL)."
)
@click.option(
    "--token",
    type=str,
    default=None,
    required=False,
    help="The JWT token to use for authentication - if required."
)
def run_demo(use_gateway: bool,
             host: str,
             port: int,
             use_tls: bool,
             token: str
             ):
    logging.basicConfig(level=logging.INFO, encoding='utf-8')

    arg_dict = locals()
    if arg_dict.pop("token"):
        arg_dict["token"] = "(redacted)"

    _LOGGER.info(
        msg=f"Starting SparkConnect client demo - args: {arg_dict}"
    )

    if use_gateway:
        uri_parameters = ""

        if use_tls:
            uri_parameters += ";use_ssl=true"
        if token:
            uri_parameters += ";token=" + token

        spark = (SparkSession.builder
                 .remote(f'sc://{host}:{port}/{uri_parameters}')
                 .getOrCreate()
                 )
    else:
        spark = SparkSession.builder.master('local').getOrCreate()
    execute_query(spark)


if __name__ == '__main__':
    run_demo()
