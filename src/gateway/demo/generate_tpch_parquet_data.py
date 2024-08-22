import logging
import os
import shutil
import sys
from pathlib import Path

import click
import duckdb

from .client_demo import CLIENT_DEMO_DATA_LOCATION

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    stream=sys.stdout,
)
_LOGGER = logging.getLogger()


def execute_query(conn: duckdb.DuckDBPyConnection, query: str):
    _LOGGER.info(msg=f"Executing SQL: '{query}'")
    conn.execute(query=query)


def get_printable_number(num: float):
    return "{:.9g}".format(num)


def generate_tpch_parquet_data(
    tpch_scale_factor: int, data_directory: str, overwrite: bool
) -> Path:
    _LOGGER.info(
        msg=(
            "Creating a TPC-H parquet dataset - with parameters: "
            f"--tpch-scale-factor={tpch_scale_factor} "
            f"--data-directory='{data_directory}' "
            f"--overwrite={overwrite}"
        )
    )

    # Output the database version
    _LOGGER.info(msg=f"Using DuckDB Version: {duckdb.__version__}")

    # Get an in-memory DuckDB database connection
    conn = duckdb.connect()

    # Load the TPCH extension needed to generate the data...
    conn.load_extension(extension="tpch")

    # Generate the data
    execute_query(conn=conn, query=f"CALL dbgen(sf={tpch_scale_factor})")

    # Export the data
    target_directory = Path(data_directory)

    if target_directory.exists():
        if overwrite:
            _LOGGER.warning(msg=f"Directory: {target_directory.as_posix()} exists, removing...")
            shutil.rmtree(path=target_directory.as_posix())
        else:
            raise RuntimeError(f"Directory: {target_directory.as_posix()} exists, aborting.")

    target_directory.mkdir(parents=True, exist_ok=True)
    execute_query(
        conn=conn, query=f"EXPORT DATABASE '{target_directory.as_posix()}' (FORMAT PARQUET)"
    )

    _LOGGER.info(msg=f"Wrote out parquet data to path: '{target_directory.as_posix()}'")

    # Restructure the contents of the directory so that each file is in its own directory
    for filename in target_directory.glob(pattern="*.parquet"):
        file = Path(filename)
        table_name = file.name.split(".")[0]
        table_directory = target_directory / table_name
        table_directory.mkdir(parents=True, exist_ok=True)

        if file.name not in ("nation.parquet", "region.parquet"):
            new_file_name = f"{table_name}.1.parquet"
        else:
            new_file_name = file.name

        file.rename(target=table_directory / new_file_name)

    _LOGGER.info(msg="All done.")

    return target_directory


@click.command()
@click.option(
    "--tpch-scale-factor",
    type=float,
    default=1,
    show_default=True,
    required=True,
    help="The TPC-H scale factor to generate.",
)
@click.option(
    "--data-directory",
    type=str,
    default=CLIENT_DEMO_DATA_LOCATION.as_posix(),
    show_default=True,
    required=True,
    help="The target output data directory to put the files into",
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target directory if it already exists...",
)
def click_generate_tpch_parquet_data(tpch_scale_factor: int, data_directory: str, overwrite: bool):
    generate_tpch_parquet_data(**locals())


if __name__ == "__main__":
    click_generate_tpch_parquet_data()
