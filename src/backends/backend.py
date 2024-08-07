# SPDX-License-Identifier: Apache-2.0
"""The base class for all Substrait backends."""
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pyarrow as pa
from substrait.gen.proto import plan_pb2

from backends.backend_options import BackendOptions


class Backend:
    """Base class providing methods for contacting a backend utilizing Substrait."""

    def __init__(self, options: BackendOptions):
        """Initialize the backend."""
        self._connection = None

    def create_connection(self) -> None:
        """Create a connection to the backend."""
        raise NotImplementedError()

    def reset_connection(self):
        """Reset the connection to the backend."""
        pass

    def get_connection(self) -> Any:
        """Return the connection to the backend (creating one if necessary)."""
        if self._connection is None:
            self.create_connection()
        return self._connection

    @contextmanager
    def adjust_plan(self, plan: plan_pb2.Plan) -> Iterator[plan_pb2.Plan]:
        """Modify the given Substrait plan for use with the given backend."""
        yield plan

    def _execute_plan(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against the backend."""
        raise NotImplementedError()

    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Adapt and execute the given Substrait plan against the backend."""
        with self.adjust_plan(plan) as modified_plan:
            return self._execute_plan(modified_plan)

    def register_table(self, name: str, path: Path, file_format: str = 'parquet',
                       temporary: bool = False) -> None:
        """Register the given table with the backend."""
        raise NotImplementedError()

    def register_table_with_arrow_data(self, name: str, data: bytes, temporary: bool = False) -> None:
        """Register the given arrow data as a table with the backend."""
        raise NotImplementedError()

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        raise NotImplementedError()

    def describe_table(self, name: str):
        """Asks the backend to describe the given table."""
        raise NotImplementedError()

    def drop_table(self, name: str) -> None:
        """Asks the backend to drop the given table."""
        raise NotImplementedError()

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Convert SQL into a Substrait plan."""
        # TODO -- Remove in favor of the Ibis Substrait conversion.
        raise NotImplementedError()

    @staticmethod
    def _expand_location(location: Path | str) -> list[str]:
        """Expand the location of a file or directory into a list of files."""
        # TODO -- Handle more than just Parquet files.
        path = Path(location)
        files = Path(location).resolve().glob('*.parquet') if path.is_dir() else [path]
        return sorted(str(f) for f in files)
