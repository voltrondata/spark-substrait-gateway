# SPDX-License-Identifier: Apache-2.0
"""Provides access to a generic ADBC backend."""
from pathlib import Path

import duckdb
import pyarrow
from adbc_driver_manager import dbapi
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend
from gateway.backends.backend_options import BackendOptions
from gateway.backends.backend_options import Backend as backend_engine


def _import(handle):
    return pyarrow.RecordBatchReader._import_from_c(handle.address)


def _get_backend_driver(options: BackendOptions) -> tuple[str, str]:
    """Gets the driver and entry point for the specified backend."""
    match options.backend:
        case backend_engine.DUCKDB:
            driver = duckdb.duckdb.__file__
            entry_point = "duckdb_adbc_init"
        case _:
            raise ValueError(f'Unknown backend type: {options.backend}')

    return driver, entry_point


class AdbcBackend(Backend):
    """Provides access to send ADBC backends Substrait plans."""

    def __init__(self, options: BackendOptions):
        self._options = options
        super().__init__(options)
        self.create_connection()

    def create_connection(self) -> None:
        driver, entry_point = _get_backend_driver(self._options)
        self._connection = dbapi.connect(driver=driver, entrypoint=entry_point)

    # pylint: disable=import-outside-toplevel
    def execute(self, plan: plan_pb2.Plan) -> pyarrow.lib.Table:
        """Executes the given Substrait plan against an ADBC backend."""
        with self._connection.cursor() as cur:
            cur.execute("LOAD substrait;")
            plan_data = plan.SerializeToString()
            cur.adbc_statement.set_substrait_plan(plan_data)
            res = cur.adbc_statement.execute_query()
            table = _import(res[0]).read_all()
            return table

    def register_table(self, name: str, path: Path, extension: str = 'parquet') -> None:
        """Registers the given table with the backend."""
        file_paths = sorted(Path(path).glob(f'*.{extension}'))
        if len(file_paths) > 0:
            # Sort the files because the later ones don't have enough data to construct a schema.
            file_paths = sorted([str(fp) for fp in file_paths])
            # TODO: Support multiple paths.
            reader = pyarrow.parquet.ParquetFile(file_paths[0])
            self._connection.cursor().adbc_ingest(name, reader.iter_batches(), mode="create")

    def describe_table(self, table_name: str):
        """Asks the backend to describe the given table."""
        return self._connection.adbc_get_table_schema(table_name)

    def drop_table(self, table_name: str):
        """Asks the backend to drop the given table."""
        with self._connection.cursor() as cur:
            # TODO -- Use an explicit ADBC call here.
            cur.execute(f'DROP TABLE {table_name}')
