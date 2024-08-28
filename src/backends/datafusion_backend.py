# SPDX-License-Identifier: Apache-2.0
"""Provides access to Datafusion."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
from substrait.gen.proto import plan_pb2

from backends.backend import Backend
from transforms.rename_functions import RenameFunctionsForDatafusion
from transforms.replace_local_files import ReplaceLocalFilesWithNamedTable


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

    @contextmanager
    def adjust_plan(self, plan: plan_pb2.Plan) -> Iterator[plan_pb2.Plan]:
        """Modify the given Substrait plan for use with Datafusion."""
        file_groups = ReplaceLocalFilesWithNamedTable().visit_plan(plan)
        registered_tables = set()
        for table_name, files in file_groups:
            # We can only register one location, so hope with the parent directory.
            location = Path(files[0]) if len(files) == 1 else Path(files[0]).parent
            self.register_table(table_name, location)
            registered_tables.add(table_name)

        RenameFunctionsForDatafusion().visit_plan(plan)

        try:
            yield plan
        finally:
            for table_name in registered_tables:
                self._connection.deregister_table(table_name)

    def _execute_plan(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against Datafusion."""
        import datafusion.substrait

        if len(plan.relations) != 1:
            raise ValueError(f"Expected exactly one relation in the plan: {plan}")
        plan_data = plan.SerializeToString()
        substrait_plan = datafusion.substrait.Serde.deserialize_bytes(plan_data)
        logical_plan = datafusion.substrait.Consumer.from_substrait_plan(
            self._connection, substrait_plan
        )

        # Create a DataFrame from a deserialized logical plan.
        df_result = self._connection.create_dataframe_from_logical_plan(logical_plan)
        # Rename the output columns to match the original plan.
        if len(df_result.schema().names) != len(plan.relations[0].root.names):
            raise ValueError(
                f"Expected {len(plan.relations[0].root.names)} columns, "
                f"but got {len(df_result.schema().names)}."
            )
        for column_number, column_name in enumerate(df_result.schema().names):
            df_result = df_result.with_column_renamed(
                column_name, plan.relations[0].root.names[column_number]
            )
        record_batch = df_result.collect()
        return pa.Table.from_batches(record_batch)

    def register_table(
        self,
        name: str,
        location: Path,
        file_format: str = "parquet",
        temporary: bool = False,
        replace: bool = False,
    ) -> None:
        """Register the given table with the backend."""
        files = Backend._expand_location(location)
        if not files:
            raise ValueError(f"No parquet files found at {location}")
        # TODO: Add options to skip table registration if it already exists instead
        # of deregistering it.
        if replace and self._connection.table_exist(name):
            self._connection.deregister_table(name)
        self._connection.register_parquet(name, str(location))

    # ruff: noqa: BLE001
    def register_table_with_arrow_data(
        self, name: str, data: bytes, temporary: bool = False, replace: bool = False
    ) -> None:
        """Register the given arrow data as a table with the backend."""
        if replace and self._connection.table_exist(name):
            self._connection.deregister_table(name)

        r = pa.ipc.open_stream(data).read_all()
        self._connection.from_arrow_table(r, name)

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        # TODO -- Use the ListingTable API to resolve the combined schema.
        return self._connection.read_parquet(paths[0]).schema()

    def describe_table(self, table_name: str):
        """Asks the backend to describe the given table."""
        return self._connection.table(table_name).schema()
