# SPDX-License-Identifier: Apache-2.0
"""Provides access to DuckDB."""
import atexit
import threading
from pathlib import Path

import duckdb
import pyarrow as pa
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend
from src.gateway.converter.rename_functions import RenameFunctionsForDuckDB


# pylint: disable=fixme
class DuckDBBackend(Backend):
    """Provides access to send Substrait plans to DuckDB."""
    _tables = {}
    _tables_lock = threading.Lock()

    def __init__(self, options):
        """Initialize the DuckDB backend."""
        self._connection = None
        super().__init__(options)
        self.create_connection()
        self._use_duckdb_python_api = options.use_duckdb_python_api

    def close(self):
        """Close the connection to DuckDB."""
        if self._connection is not None:
            self._connection.close()

    def create_connection(self):
        """Create a connection to the backend."""
        if self._connection is not None:
            return self._connection

        self._register_cleanup_on_exit('sparkgateway.db')
        self._connection = duckdb.connect(
            'sparkgateway.db',
            config={'max_memory': '100GB',
                    "allow_unsigned_extensions": "true",
                    'temp_directory': str(Path('.').resolve())})
        self._connection.install_extension('substrait')
        self._connection.load_extension('substrait')

        return self._connection

    def _register_cleanup_on_exit(self, filename: str) -> None:
        """Register a cleanup function to delete the given file on exit."""

        def cleanup():
            Path(filename).unlink()

        # TODO -- Only do this once per server execution.
        atexit.register(cleanup)

    def reset_connection(self):
        """Reset the connection to the backend."""
        self._connection.close()
        self._connection = None
        self.create_connection()
        with self._tables_lock:
            for table in self._tables.values():
                self.register_table(*table)

    # ruff: noqa: BLE001
    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against DuckDB."""
        RenameFunctionsForDuckDB().visit_plan(plan)

        plan_data = plan.SerializeToString()

        try:
            query_result = self._connection.from_substrait(proto=plan_data)
        except Exception as err:
            raise ValueError(f'DuckDB Execution Error: {err}') from err
        return query_result.arrow()

    def register_table(
            self,
            table_name: str,
            location: Path,
            file_format: str = "parquet"
    ) -> None:
        """Register the given table with the backend."""
        files = Backend.expand_location(location)
        if not files:
            raise ValueError(f"No parquet files found at {location}")

        with self._tables_lock:
            self._tables[table_name] = (table_name, location, file_format)
            if self._use_duckdb_python_api:
                if self._connection.table(table_name):
                    # TODO -- Find the direct Python way to drop a table.
                    self._connection.execute(f'DROP TABLE {table_name}')
                self._connection.register(table_name, self._connection.read_parquet(files))
            else:
                files_str = ', '.join([f"'{f}'" for f in files])
                files_sql = (
                    f"CREATE OR REPLACE TABLE {table_name} AS FROM read_parquet([{files_str}])")
                self._connection.execute(files_sql)

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        files = paths
        if len(paths) == 1:
            files = self.expand_location(paths[0])
        # TODO -- Handle resolution of a combined schema.
        df = self._connection.read_parquet(files)
        schema = df.to_arrow_table().schema
        if 'aggr' in schema.names:
            raise ValueError("Aggr column found in schema")
        return schema

    def describe_table(self, name: str):
        """Asks the backend to describe the given table."""
        table = self._connection.table(name)
        schema = table.to_arrow_table().schema
        if 'aggr' in schema.names:
            raise ValueError("Aggr column found in schema")
        return schema

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Convert SQL into a Substrait plan."""
        plan = plan_pb2.Plan()
        proto_bytes = self._connection.get_substrait(query=sql).fetchone()[0]
        plan.ParseFromString(proto_bytes)
        return plan
