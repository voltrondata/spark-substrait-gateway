# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""

import logging
import os
import sys
from pathlib import Path

import click
from gateway.config import SERVER_PORT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    stream=sys.stdout,
)
_LOGGER = logging.getLogger()

# Constants
CLIENT_DEMO_DATA_LOCATION = Path("data") / "tpch" / "parquet"


def find_tpch(raise_error_if_not_exists: bool) -> Path:
    """Find the location of the TPCH dataset."""
    location = CLIENT_DEMO_DATA_LOCATION
    if raise_error_if_not_exists and not location.exists():
        raise ValueError("TPCH dataset not found")
    else:
        return location


# pylint: disable=fixme
def get_customer_database(spark_session: SparkSession, use_gateway: bool) -> DataFrame:
    """Register the TPC-H customer database."""
    location_customer = str(find_tpch(raise_error_if_not_exists=(not use_gateway)) / "customer")

    return spark_session.read.parquet(location_customer, mergeSchema=False)


# pylint: disable=fixme
# ruff: noqa: T201
def execute_query(spark_session: SparkSession, use_gateway: bool) -> None:
    """Run a single sample query against the gateway."""
    df_customer = get_customer_database(spark_session=spark_session, use_gateway=use_gateway)

    df_customer.createOrReplaceTempView("customer")

    # pylint: disable=singleton-comparison
    df_result = df_customer.filter(col("c_mktsegment") == "FURNITURE").sort(col("c_name")).limit(10)

    df_result.show()

    sql_results = spark_session.sql(
        "SELECT c_custkey, c_phone, c_mktsegment FROM customer LIMIT 5"
    ).collect()
    print(sql_results)


def run_demo(
    use_gateway: bool = True,
    host: str = "localhost",
    port: int = SERVER_PORT,
    use_tls: bool = False,
    token: str | None = None,
):
    """Run a small Spark Substrait Gateway client demo."""
    logging.basicConfig(level=logging.INFO, encoding="utf-8")

    arg_dict = locals()
    if arg_dict.pop("token"):
        arg_dict["token"] = "(redacted)"

    _LOGGER.info(msg=f"Starting SparkConnect client demo - args: {arg_dict}")

    if use_gateway:
        uri_parameters = ""

        if use_tls:
            uri_parameters += ";use_ssl=true"
        if token:
            uri_parameters += ";token=" + token

        spark = SparkSession.builder.remote(f"sc://{host}:{port}/{uri_parameters}").getOrCreate()
    else:
        spark = SparkSession.builder.master("local").getOrCreate()
    execute_query(spark_session=spark, use_gateway=use_gateway)


@click.command()
@click.option(
    "--use-gateway/--no-use-gateway",
    type=bool,
    default=True,
    show_default=True,
    required=True,
    help="Use the gateway service.",
)
@click.option(
    "--host",
    type=str,
    default="localhost",
    show_default=True,
    required=True,
    help="The gateway hostname.",
)
@click.option(
    "--port",
    type=int,
    default=os.getenv("SERVER_PORT", SERVER_PORT),
    show_default=True,
    required=True,
    help="Run the Spark Substrait Gateway server on this port.",
)
@click.option(
    "--use-tls/--no-use-tls",
    type=bool,
    default=False,
    required=True,
    help="Enable transport-level security (TLS/SSL).",
)
@click.option(
    "--token",
    type=str,
    default=None,
    required=False,
    help="The JWT token to use for authentication - if required.",
)
def click_run_demo(use_gateway: bool, host: str, port: int, use_tls: bool, token: str):
    """Provide a click interface for running the Spark Substrait Gateway client demo."""
    run_demo(**locals())


if __name__ == "__main__":
    click_run_demo()
