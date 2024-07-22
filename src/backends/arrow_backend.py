# SPDX-License-Identifier: Apache-2.0
"""Provides access to Acero."""
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.substrait
from substrait.gen.proto import plan_pb2
from transforms.rename_functions import RenameFunctionsForArrow

from backends.backend import Backend


class ArrowBackend(Backend):
    """Provides access to send Acero Substrait plans."""

    _registered_tables: ClassVar[dict[str, Path]] = {}

    def __init__(self, options):
        """Initialize the Acero backend."""
        super().__init__(options)
        self._use_uri_workaround = options.use_arrow_uri_workaround

    @contextmanager
    def adjust_plan(self, plan: plan_pb2.Plan):
        """Modify the given Substrait plan for use with Acero."""
        RenameFunctionsForArrow(use_uri_workaround=self._use_uri_workaround).visit_plan(plan)
        yield plan

    def _execute_plan(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = pa.substrait.run_query(plan_data, table_provider=self._provide_tables)
        return reader.read_all()

    def register_table(self, name: str, path: Path, file_format: str = 'parquet',
                       temporary: bool = False) -> None:
        """Register the given table with Acero."""
        self._registered_tables[name] = path

    def drop_table(self, name: str) -> None:
        """Asks the backend to drop the given table."""
        if self._registered_tables.get(name):
            del self._registered_tables[name]

    def describe_table(self, name: str):
        """Return the schema of the given table."""
        return pa.parquet.read_table(self._registered_tables[name]).schema

    def _provide_tables(self, names: list[str], unused_schema) -> pyarrow.Table:
        """Provide the tables requested."""
        for name in names:
            if name in self._registered_tables:
                return pa.parquet.read_table(self._registered_tables[name])
        raise ValueError(f'Table {names} not found in {self._registered_tables}')
