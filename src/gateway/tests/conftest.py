# SPDX-License-Identifier: Apache-2.0
"""Test fixtures for pytest of the gateway server."""
import re
from pathlib import Path

import duckdb
import pytest
from filelock import FileLock
from pyspark.sql.session import SparkSession

from gateway.demo.mystream_database import (
    create_mystream_database,
    delete_mystream_database,
)
from gateway.server import serve


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


@pytest.fixture(scope='class', autouse=True)
def gateway_server():
    """Starts up a spark to substrait gateway service."""
    server = serve(50052, wait=False)
    yield
    server.stop(None)


@pytest.fixture(scope='session')
def users_location(manage_database) -> str:
    """Provides the location of the users database."""
    return str(Path('users.parquet').resolve())


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


@pytest.fixture(scope='class')
def spark_session_for_setup(source):
    """Provides spark sessions connecting to the current backend source."""
    yield from _get_session_generator(source)


@pytest.fixture(scope='class')
def register_users_dataset(spark_session_for_setup, users_location):
    """Registers the user dataset into the spark session."""
    df = spark_session_for_setup.read.parquet(users_location)
    df.createOrReplaceTempView('users')


@pytest.fixture(scope='function')
def users_dataframe(spark_session, register_users_dataset):
    """Provides the spark session with the users dataframe already loaded."""
    return spark_session.table('users')


def find_tpch() -> Path:
    """Find the location of the TPC-H dataset."""
    current_location = Path('.').resolve()
    while current_location != Path('/'):
        location = current_location / 'data' / 'tpch' / 'parquet'
        if location.exists():
            return location.resolve()
        current_location = current_location.parent
    raise ValueError('TPC-H dataset not found')


def find_tpcds() -> Path:
    """Find the location of the TPC-DS dataset."""
    current_location = Path('.').resolve()
    while current_location != Path('/'):
        location = current_location / 'data' / 'tpcds' / 'parquet'
        if location.exists():
            return location.resolve()
        current_location = current_location.parent
    raise ValueError('TPC-DS dataset not found')


def _register_table(spark_session: SparkSession, benchmark: str, name: str) -> None:
    """Registers a TPC table with the given name into spark_session."""
    if benchmark == 'tpch':
        location = find_tpch() / name
    elif benchmark == 'tpcds':
        location = find_tpcds() / name
    else:
        raise ValueError(f'Unknown benchmark: {benchmark}')
    df = spark_session.read.parquet(str(location.with_suffix('.parquet')))
    df.createOrReplaceTempView(name)


@pytest.fixture(scope='class')
def register_tpch_dataset(spark_session_for_setup: SparkSession, prepare_tpch_parquet_data) -> None:
    """Add the TPC-H dataset to the current spark session."""
    benchmark = 'tpch'
    _register_table(spark_session_for_setup, benchmark, 'customer')
    _register_table(spark_session_for_setup, benchmark, 'lineitem')
    _register_table(spark_session_for_setup, benchmark, 'nation')
    _register_table(spark_session_for_setup, benchmark, 'orders')
    _register_table(spark_session_for_setup, benchmark, 'part')
    _register_table(spark_session_for_setup, benchmark, 'partsupp')
    _register_table(spark_session_for_setup, benchmark, 'region')
    _register_table(spark_session_for_setup, benchmark, 'supplier')


def _get_project_root() -> Path:
    """Finds the root of the project."""
    return Path(__file__).parent.parent.parent.parent


@pytest.fixture(scope="session")
def prepare_tpch_parquet_data(scale_factor=1):
    """
    Generate TPC-H data to be used for testing.

    Parameters:
        scale_factor:
            Scale factor for TPC-H data generation.
    """
    _prepare_data("tpch", scale_factor)


@pytest.fixture(scope="session")
def prepare_tpcds_parquet_data(scale_factor=0.1):
    """
    Generate TPC-DS data to be used for testing.

    Parameters:
        scale_factor:
            Scale factor for TPC-DS data generation.
    """
    _prepare_data("tpcds", scale_factor)


def _prepare_data(benchmark, scale_factor):
    """
    Generate the benchmark data to be used for testing.

    Parameters:
        benchmark:
            TPCH or TPCDS.
    """
    data_path = _get_project_root() / "data" / benchmark / "parquet"
    if benchmark == 'tpch':
        generator = 'dbgen'
    elif benchmark == 'tpcds':
        generator = 'dsdgen'
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL {generator}(sf={scale_factor})")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")


@pytest.fixture(scope='class')
def register_tpcds_dataset(spark_session_for_setup: SparkSession, prepare_tpcds_parquet_data) -> None:
    """Add the TPC-DS dataset to the current spark session."""
    benchmark = 'tpcds'
    _register_table(spark_session_for_setup, benchmark, 'call_center')
    _register_table(spark_session_for_setup, benchmark, 'catalog_page')
    _register_table(spark_session_for_setup, benchmark, 'catalog_returns')
    _register_table(spark_session_for_setup, benchmark, 'catalog_sales')
    _register_table(spark_session_for_setup, benchmark, 'customer')
    _register_table(spark_session_for_setup, benchmark, 'customer_address')
    _register_table(spark_session_for_setup, benchmark, 'customer_demographics')
    _register_table(spark_session_for_setup, benchmark, 'date_dim')
    _register_table(spark_session_for_setup, benchmark, 'household_demographics')
    _register_table(spark_session_for_setup, benchmark, 'income_band')
    _register_table(spark_session_for_setup, benchmark, 'inventory')
    _register_table(spark_session_for_setup, benchmark, 'item')
    _register_table(spark_session_for_setup, benchmark, 'promotion')
    _register_table(spark_session_for_setup, benchmark, 'reason')
    _register_table(spark_session_for_setup, benchmark, 'ship_mode')
    _register_table(spark_session_for_setup, benchmark, 'store')
    _register_table(spark_session_for_setup, benchmark, 'store_returns')
    _register_table(spark_session_for_setup, benchmark, 'store_sales')
    _register_table(spark_session_for_setup, benchmark, 'time_dim')
    _register_table(spark_session_for_setup, benchmark, 'warehouse')
    _register_table(spark_session_for_setup, benchmark, 'web_page')
    _register_table(spark_session_for_setup, benchmark, 'web_returns')
    _register_table(spark_session_for_setup, benchmark, 'web_sales')
    _register_table(spark_session_for_setup, benchmark, 'web_site')
