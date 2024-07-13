# SPDX-License-Identifier: Apache-2.0
"""Provides access to DuckDB."""
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow as pa
from substrait.gen.proto import plan_pb2

from backends.backend import Backend
from src.gateway.converter.rename_functions import RenameFunctionsForDuckDB


# pylint: disable=fixme
class DuckDBBackend(Backend):
    """Provides access to send Substrait plans to DuckDB."""

    def __init__(self, options):
        """Initialize the DuckDB backend."""
        self._connection = None
        self._tables = {}
        super().__init__(options)
        self.create_connection()
        self._use_duckdb_python_api = options.use_duckdb_python_api

    def create_connection(self):
        """Create a connection to the backend."""
        if self._connection is not None:
            return self._connection

        self._connection = duckdb.connect(config={'max_memory': '100GB',
                                                  "allow_unsigned_extensions": "true",
                                                  'temp_directory': str(Path('').resolve())})
        self._connection.install_extension('substrait')
        self._connection.load_extension('substrait')

        return self._connection

    def reset_connection(self):
        """Reset the connection to the backend."""
        self._connection.close()
        self._connection = None
        self.create_connection()
        for table in self._tables.values():
            self.register_table(*table)

    @contextmanager
    def adjust_plan(self, plan: plan_pb2.Plan) -> Iterator[plan_pb2.Plan]:
        """Modify the given Substrait plan for use with DuckDB."""
        RenameFunctionsForDuckDB().visit_plan(plan)

        yield plan

    # ruff: noqa: BLE001
    def _execute_plan(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against DuckDB."""
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

        self._tables[table_name] = (table_name, location, file_format)
        if self._use_duckdb_python_api:
            self._connection.register(table_name, self._connection.read_parquet(files))
        else:
            files_str = ', '.join([f"'{f}'" for f in files])
            files_sql = f"CREATE OR REPLACE TABLE {table_name} AS FROM read_parquet([{files_str}])"
            self._connection.execute(files_sql)

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        files = paths
        if len(paths) == 1:
            files = Backend.expand_location(paths[0])
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
