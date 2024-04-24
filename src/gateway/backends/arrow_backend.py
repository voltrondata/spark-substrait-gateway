# SPDX-License-Identifier: Apache-2.0
"""Provides access to Acero."""
from pathlib import Path

import pyarrow as pa
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend
from gateway.converter.rename_functions import RenameFunctionsForArrow


class ArrowBackend(Backend):
    """Provides access to send Acero Substrait plans."""

    _registered_tables: ClassVar[dict[str, Path]] = {}

    def __init__(self, options):
        """Initialize the Datafusion backend."""
        super().__init__(options)
        self._use_uri_workaround = options.use_arrow_uri_workaround

    # pylint: disable=import-outside-toplevel
    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against Acero."""
        RenameFunctionsForArrow(use_uri_workaround=self._use_uri_workaround).visit_plan(plan)

        plan_data = plan.SerializeToString()
        reader = pa.substrait.run_query(plan_data)
        return reader.read_all()

    def register_table(self, name: str, path: Path, file_format: str = 'parquet') -> None:
        """Register the given table with the backend."""
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
