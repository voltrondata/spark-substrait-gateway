# SPDX-License-Identifier: Apache-2.0
"""Provides access to DuckDB."""
from pathlib import Path

import duckdb
import pyarrow as pa
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend

_DUCKDB_TO_ARROW = {
    'BOOLEAN': pa.bool_(),
    'TINYINT': pa.int8(),
    'SMALLINT': pa.int16(),
    'INTEGER': pa.int32(),
    'BIGINT': pa.int64(),
    'FLOAT': pa.float32(),
    'DOUBLE': pa.float64(),
    'DATE': pa.date32(),
    'TIMESTAMP': pa.timestamp('ns'),
    'VARCHAR': pa.string(),
}


# pylint: disable=fixme
class DuckDBBackend(Backend):
    """Provides access to send Substrait plans to DuckDB."""

    def __init__(self, options):
        """Initialize the DuckDB backend."""
        self._connection = None
        super().__init__(options)
        self.create_connection()

    def create_connection(self):
        """Create a connection to the backend."""
        if self._connection is not None:
            return self._connection

        self._connection = duckdb.connect(config={'max_memory': '100GB',
                                                  "allow_unsigned_extensions": "true",
                                                  'temp_directory': str(Path('.').resolve())})
        self._connection.install_extension('substrait')
        self._connection.load_extension('substrait')

        return self._connection

    # ruff: noqa: BLE001
    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against DuckDB."""
        plan_data = plan.SerializeToString()

        try:
            query_result = self._connection.from_substrait(proto=plan_data)
        except Exception as err:
            raise ValueError(f'DuckDB Execution Error: {err}') from err
        df = query_result.df()
        return pa.Table.from_pandas(df=df)

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
        files_str = ', '.join([f"'{f}'" for f in files])
        files_sql = f"CREATE OR REPLACE TABLE {table_name} AS FROM read_parquet([{files_str}])"

        self._connection.execute(files_sql)

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        files = paths
        if len(paths) == 1:
            files = self.expand_location(paths[0])
        df = self._connection.read_parquet(files)

        fields = []
        for name, field_type in zip(df.columns, df.types, strict=False):
            if name == 'aggr':
                # This isn't a real column.
                continue
            fields.append(pa.field(name, _DUCKDB_TO_ARROW[str(field_type)]))
        return pa.schema(fields)

    def describe_table(self, name: str):
        """Asks the backend to describe the given table."""
        df = self._connection.table(name).describe()

        fields = []
        for name, field_type in zip(df.columns, df.types, strict=False):
            if name == 'aggr':
                # This isn't a real column.
                continue
            fields.append(pa.field(name, _DUCKDB_TO_ARROW[str(field_type)]))
        return pa.schema(fields)

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Convert SQL into a Substrait plan."""
        plan = plan_pb2.Plan()
        proto_bytes = self._connection.get_substrait(query=sql).fetchone()[0]
        plan.ParseFromString(proto_bytes)
        return plan
