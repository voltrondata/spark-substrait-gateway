# SPDX-License-Identifier: Apache-2.0
"""Provides access to Datafusion."""
from pathlib import Path

import pyarrow as pa
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend
from gateway.converter.rename_functions import RenameFunctions
from gateway.converter.replace_local_files import ReplaceLocalFilesWithNamedTable


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
            for file in files[1]:
                if table_name not in registered_tables:
                    self.register_table(table_name, file)
                registered_tables.add(files[0])

        RenameFunctions().visit_plan(plan)

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

    def register_table(self, name: str, path: Path, file_format: str = 'parquet') -> None:
        """Register the given table with the backend."""
        files = Backend.expand_location(path)
        self._connection.register_parquet(name, files[0])
