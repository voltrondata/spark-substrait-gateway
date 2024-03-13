# SPDX-License-Identifier: Apache-2.0
"""Test fixtures for pytest of the gateway server."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.pandas.types import from_arrow_schema
import pytest

from gateway.demo.mystream_database import create_mystream_database, delete_mystream_database
from gateway.demo.mystream_database import get_mystream_schema
from gateway.server import serve


def _create_local_spark_session():
    """Creates a local spark session for testing."""
    spark = (
        SparkSession
        .builder
        .master('local')
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName('gateway')
        .getOrCreate()
    )
    yield spark
    print("stopping the local spark session")
    spark.stop()


def _create_gateway_session():
    """Creates a local gateway session for testing."""
    spark = (
        SparkSession
        .builder
        .remote('sc://localhost:50052')
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName('gateway')
        .getOrCreate()
    )
    yield spark
    print("stopping the gateway session")
    spark.stop()


@pytest.fixture(scope='session', autouse=True)
def manage_database() -> None:
    """Creates the mystream database for use throughout all the tests."""
    create_mystream_database()
    yield
    delete_mystream_database()


@pytest.fixture(scope='module', autouse=True)
def gateway_server():
    """Starts up a spark to substrait gateway service."""
    server = serve(50052, wait=False)
    yield
    server.stop(None)


@pytest.fixture(scope='session')
def users_location():
    """Provides the location of the users database."""
    return str(Path('users.parquet').absolute())


@pytest.fixture(scope='session')
def schema_users():
    """Provides the schema of the users database."""
    return get_mystream_schema('users')


@pytest.fixture(scope='module',
                params=['spark', pytest.param('gateway-over-duckdb', marks=pytest.mark.xfail)])
def spark_session(request):
    """Provides spark sessions connecting to various backends."""
    match request.param:
        case 'spark':
            session_generator = _create_local_spark_session()
        case 'gateway-over-duckdb':
            session_generator = _create_gateway_session()
        case _:
            raise NotImplementedError(f'No such session implemented: {request.param}')
    yield from session_generator


# pylint: disable=redefined-outer-name
@pytest.fixture(scope='function')
def users_dataframe(spark_session, schema_users, users_location):
    """Provides a ready to go dataframe over the users database."""
    return spark_session.read.format('parquet') \
        .schema(from_arrow_schema(schema_users)) \
        .parquet(users_location)
