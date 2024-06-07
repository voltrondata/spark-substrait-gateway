# SPDX-License-Identifier: Apache-2.0
"""Provides access to Datafusion."""
import re
from pathlib import Path

import pyarrow as pa
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend
from gateway.converter.rename_functions import RenameFunctionsForDatafusion
from gateway.converter.replace_local_files import ReplaceLocalFilesWithNamedTable

_DATAFUSION_TO_ARROW = {
    'Boolean': pa.bool_(),
    'Int8': pa.int8(),
    'Int16': pa.int16(),
    'Int32': pa.int32(),
    'Int64': pa.int64(),
    'Float32': pa.float32(),
    'Float64': pa.float64(),
    'Date32': pa.date32(),
    'Timestamp(Nanosecond, None)': pa.timestamp('ns'),
    'Utf8': pa.string(),
}

MATCH_STRUCT_TYPE = re.compile(r'Struct\(([^)]+)\)')


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
    arrow_type = _DATAFUSION_TO_ARROW.get(str(duckdb_type), None)
    if not arrow_type:
        raise ValueError(f"Unknown Datafusion type: {duckdb_type}")
    return pa.field(name, type=arrow_type)


# pylint: disable=import-outside-toplevel
class DatafusionBackend(Backend):
    """Provides access to send Substrait plans to Datafusion."""

    def __init__(self, options):
        """Initialize the Datafusion backend."""
        self._connection = None
        super().__init__(options)
        self.create_connection()

    def create_connection(self) -> None:
        """Create a connection to the backend."""
        import datafusion

        self._connection = datafusion.SessionContext()

    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against Datafusion."""
        import datafusion.substrait

        file_groups = ReplaceLocalFilesWithNamedTable().visit_plan(plan)
        registered_tables = set()
        for files in file_groups:
            table_name = files[0]
            location = Path(files[1][0]).parent
            self.register_table(table_name, location)
            registered_tables.add(table_name)

        RenameFunctionsForDatafusion().visit_plan(plan)

        try:
            plan_data = plan.SerializeToString()
            substrait_plan = datafusion.substrait.substrait.serde.deserialize_bytes(plan_data)
            logical_plan = datafusion.substrait.substrait.consumer.from_substrait_plan(
                self._connection, substrait_plan
            )

            # Create a DataFrame from a deserialized logical plan.
            df_result = self._connection.create_dataframe_from_logical_plan(logical_plan)
            for column_number, column_name in enumerate(df_result.schema().names):
                df_result = df_result.with_column_renamed(
                    column_name,
                    plan.relations[0].root.names[column_number]
                )
            return df_result.to_arrow_table()
        finally:
            for table_name in registered_tables:
                self._connection.deregister_table(table_name)

    def register_table(
            self, name: str, location: Path, file_format: str = 'parquet'
    ) -> None:
        """Register the given table with the backend."""
        files = Backend.expand_location(location)
        if not files:
            raise ValueError(f"No parquet files found at {location}")
        # TODO: Add options to skip table registration if it already exists instead
        # of deregistering it.
        if self._connection.table_exist(name):
            self._connection.deregister_table(name)
        self._connection.register_parquet(name, str(location))

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        # TODO -- Use the ListingTable API to resolve the combined schema.
        return self._connection.read_parquet(paths[0]).schema()

    def describe_table(self, table_name: str):
        """Asks the backend to describe the given table."""
        return self._connection.table(table_name).schema()
