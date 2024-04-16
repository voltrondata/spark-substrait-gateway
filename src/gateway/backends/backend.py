# SPDX-License-Identifier: Apache-2.0
"""The base class for all Substrait backends."""
from pathlib import Path
from typing import List, Any

import pyarrow
from substrait.gen.proto import plan_pb2

from gateway.backends.backend_options import BackendOptions


class Backend:
    """Base class providing methods for contacting a backend utilizing Substrait."""

    def __init__(self, options: BackendOptions):
        self._connection = None

    def create_connection(self) -> None:
        raise NotImplementedError()

    def get_connection(self) -> Any:
        """Returns the connection to the backend."""
        if self._connection is None:
            self._connection = self.create_connection()
        return self._connection

    # pylint: disable=import-outside-toplevel
    def execute(self, plan: plan_pb2.Plan) -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Datafusion."""
        raise NotImplementedError()

    def register_table(self, name: str, path: Path | str, extension: str = 'parquet') -> None:
        """Registers the given table with the backend."""
        raise NotImplementedError()

    def describe_table(self, name: str):
        """Asks the backend to describe the given table."""
        raise NotImplementedError()

    def drop_table(self, name: str):
        """Asks the backend to drop the given table."""
        raise NotImplementedError()

    @staticmethod
    def expand_location(location: Path | str) -> List[str]:
        """Expands the location of a file or directory into a list of files."""
        # TODO -- Handle more than just Parquet files.
        path = Path(location)
        if path.is_dir():
            files = Path(location).resolve().glob('*.parquet')
        else:
            files = [path]
        return sorted(str(f) for f in files)

    @staticmethod
    def find_tpch() -> Path:
        """Finds the location of the TPCH dataset."""
        current_location = Path('.').resolve()
        while current_location != Path('/'):
            location = current_location / 'third_party' / 'tpch' / 'parquet'
            if location.exists():
                return location.resolve()
            current_location = current_location.parent
        raise ValueError('TPCH dataset not found')

    def register_tpch(self):
        """Convenience function to register the entire TPC-H dataset."""
        tpch_location = Backend.find_tpch()
        self.register_table('customer', tpch_location / 'customer')
        self.register_table('lineitem', tpch_location / 'lineitem')
        self.register_table('nation', tpch_location / 'nation')
        self.register_table('orders', tpch_location / 'orders')
        self.register_table('part', tpch_location / 'part')
        self.register_table('partsupp', tpch_location / 'partsupp')
        self.register_table('region', tpch_location / 'region')
        self.register_table('supplier', tpch_location / 'supplier')
