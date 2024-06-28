# SPDX-License-Identifier: Apache-2.0
"""Test fixtures for pytest of the gateway server."""
import re
from pathlib import Path

import pytest
from gateway.demo.mystream_database import (
    create_mystream_database,
    delete_mystream_database,
    get_mystream_schema,
)
from gateway.server import serve
from pyspark.sql.session import SparkSession


def pytest_collection_modifyitems(items):
    for item in items:
        if 'source' in getattr(item, 'fixturenames', ()):
            source = re.search(r'\[([^,]+?)(-\d+)?]$', item.name).group(1)
            item.add_marker(source)
            continue
        item.add_marker('general')


# ruff: noqa: T201
def _create_local_spark_session() -> SparkSession:
    """Creates a local spark session for testing."""
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .config("spark.driver.memory", "2g")
        .appName('gateway')
        .getOrCreate()
    )

    conf = spark.sparkContext.getConf()
    # Dump the configuration settings for debug purposes.
    print("==== BEGIN SPARK CONFIG ====")
    for k, v in sorted(conf.getAll()):
        print(f"{k} = {v}")
    print("===== END SPARK CONFIG =====")

    yield spark
    spark.stop()


def _create_gateway_session(backend: str) -> SparkSession:
    """Creates a local gateway session for testing."""
    spark_gateway = (
        SparkSession
        .builder
        .remote('sc://localhost:50052')
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark-substrait-gateway.backend", backend)
        .appName('gateway')
        .create()
    )
    yield spark_gateway
    spark_gateway.stop()


def _get_session_generator(source: str):
    """Provides spark sessions connecting to various backends."""
    match source:
        case 'spark':
            session_generator = _create_local_spark_session()
        case 'gateway-over-arrow':
            session_generator = _create_gateway_session('arrow')
        case 'gateway-over-datafusion':
            session_generator = _create_gateway_session('datafusion')
        case 'gateway-over-duckdb':
            session_generator = _create_gateway_session('duckdb')
        case _:
            raise NotImplementedError(f'No such session implemented: {source}')
    return session_generator


@pytest.fixture(scope='session', autouse=True)
def manage_database() -> None:
    """Creates the mystream database for use throughout all the tests."""
    create_mystream_database()
    yield
    delete_mystream_database()


@pytest.fixture(scope='session', autouse=True)
def gateway_server():
    """Starts up a spark to substrait gateway service."""
    server = serve(50052, wait=False)
    yield
    server.stop(None)


@pytest.fixture(scope='session')
def users_location(manage_database) -> str:
    """Provides the location of the users database."""
    return str(Path('users.parquet').resolve())


@pytest.fixture(scope='session')
def schema_users(manage_database):
    """Provides the schema of the users database."""
    return get_mystream_schema('users')


@pytest.fixture(scope='session',
                params=['spark',
                        'gateway-over-arrow',
                        'gateway-over-duckdb',
                        'gateway-over-datafusion',
                        ])
def source(request) -> str:
    """Provides the source (backend) to be used."""
    return request.param


@pytest.fixture(scope='function')
def spark_session(source):
    """Provides spark sessions connecting to the current backend source."""
    yield from _get_session_generator(source)


@pytest.fixture(scope='session')
def spark_session_for_setup(source):
    """Provides spark sessions connecting to the current backend source."""
    yield from _get_session_generator(source)


@pytest.fixture(scope='session')
def users_dataframe(spark_session_for_setup, schema_users, users_location):
    """Provides the spark session with the users database already loaded."""
    df = spark_session_for_setup.read.parquet(users_location)
    df.createOrReplaceTempView('users')
    return spark_session_for_setup.table('users')


def find_tpch() -> Path:
    """Find the location of the TPC-H dataset."""
    current_location = Path('.').resolve()
    while current_location != Path('/'):
        location = current_location / 'third_party' / 'tpch' / 'parquet'
        if location.exists():
            return location.resolve()
        current_location = current_location.parent
    raise ValueError('TPC-H dataset not found')


def _register_table(spark_session: SparkSession, name: str) -> None:
    """Registers a TPC-H table with the given name into spark_session."""
    location = find_tpch() / name
    df = spark_session.read.parquet(str(location))
    df.createOrReplaceTempView(name)


@pytest.fixture(scope='function')
def spark_session_with_tpch_dataset(spark_session: SparkSession) -> SparkSession:
    """Add the TPC-H dataset to the current spark session."""
    _register_table(spark_session, 'customer')
    _register_table(spark_session, 'lineitem')
    _register_table(spark_session, 'nation')
    _register_table(spark_session, 'orders')
    _register_table(spark_session, 'part')
    _register_table(spark_session, 'partsupp')
    _register_table(spark_session, 'region')
    _register_table(spark_session, 'supplier')
    return spark_session
