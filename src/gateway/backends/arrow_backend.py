# SPDX-License-Identifier: Apache-2.0
"""Provides access to Acero."""
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.substrait
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend


class ArrowBackend(Backend):
    """Provides access to send Acero Substrait plans."""

    _registered_tables: ClassVar[dict[str, Path]] = {}

    def __init__(self, options):
        """Initialize the Datafusion backend."""
        super().__init__(options)

    # pylint: disable=import-outside-toplevel
    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = pa.substrait.run_query(plan_data, table_provider=self._provide_tables)
        return reader.read_all()

    def register_table(self, name: str, path: Path) -> None:
        """Register the given table with the backend."""
        self._registered_tables[name] = path

    def drop_table(self, name: str) -> None:
        """Asks the backend to drop the given table."""
        if self._registered_tables.get(name):
            del self._registered_tables[name]

    def _provide_tables(self, names: list[str], unused_schema) -> pyarrow.Table:
        """Provide the tables requested."""
        for name in names:
            if name in self._registered_tables:
                return pa.Table.from_pandas(pa.read_parquet(self._registered_tables[name]))
        raise ValueError(f'Table {names} not found in {self._registered_tables}')
