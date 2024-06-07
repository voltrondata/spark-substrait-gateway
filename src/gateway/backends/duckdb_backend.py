# SPDX-License-Identifier: Apache-2.0
"""Provides access to DuckDB."""
import re
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

MATCH_STRUCT_TYPE = re.compile(r'STRUCT\(([^)]+)\)')


def convert_to_arrow_data_type(name, duckdb_type: str) -> pa.Field:
    """Convert a DuckDB type to an Arrow type."""
    match = MATCH_STRUCT_TYPE.match(duckdb_type)
    if match:
        subtypes = match.group(1).split(',')
        subtype_list = []
        for subtype in subtypes:
            subtype_name, type_str = subtype.strip().split(' ')
            subtype_list.append(convert_to_arrow_data_type(subtype_name, type_str))
        return pa.field(name, type=pa.struct(subtype_list))
    arrow_type = _DUCKDB_TO_ARROW.get(str(duckdb_type), None)
    if not arrow_type:
        raise ValueError(f"Unknown DuckDB type: {duckdb_type}")
    return pa.field(name, type=arrow_type)


# pylint: disable=fixme
class DuckDBBackend(Backend):
    """Provides access to send Substrait plans to DuckDB."""

    def __init__(self, options):
        """Initialize the DuckDB backend."""
        self._connection = None
        super().__init__(options)
        self.create_connection()
        self._use_duckdb_python_api = options.use_duckdb_python_api

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
            files = self.expand_location(paths[0])
        df = self._connection.read_parquet(files)

        fields = []
        for name, field_type in zip(df.columns, df.types, strict=False):
            if name == 'aggr':
                # This isn't a real column.
                continue
            fields.append(convert_to_arrow_data_type(name, field_type))
        return pa.schema(fields)

    def describe_table(self, name: str):
        """Asks the backend to describe the given table."""
        df = self._connection.execute(f'DESCRIBE {name}').fetchdf()

        fields = []
        for name, field_type in zip(df.column_name, df.column_type, strict=False):
            fields.append(convert_to_arrow_data_type(name, field_type))
        return pa.schema(fields)

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Convert SQL into a Substrait plan."""
        plan = plan_pb2.Plan()
        proto_bytes = self._connection.get_substrait(query=sql).fetchone()[0]
        plan.ParseFromString(proto_bytes)
        return plan
