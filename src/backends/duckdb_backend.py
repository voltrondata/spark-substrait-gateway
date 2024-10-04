# SPDX-License-Identifier: Apache-2.0
"""Provides access to DuckDB."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import List

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from substrait.gen.proto import plan_pb2

from backends.backend import Backend
from transforms.rename_functions import RenameFunctionsForDuckDB

from src.backends.arrow_tools import reapply_names


# pylint: disable=fixme
class DuckDBBackend(Backend):
    """Provides access to send Substrait plans to DuckDB."""

    _TABLES_FOUND: int = 0

    def __init__(self, options):
        """Initialize the DuckDB backend."""
        self._connection = None
        self._file_tables = {}
        self._data_tables = {}
        super().__init__(options)
        self.create_connection()
        self._use_duckdb_python_api = options.use_duckdb_python_api
        self._register_virtual_tables_as_local = options.register_virtual_tables_as_local

    def create_connection(self):
        """Create a connection to the backend."""
        if self._connection is not None:
            return self._connection

        self._connection = duckdb.connect(
            config={
                "max_memory": "100GB",
                "allow_unsigned_extensions": "true",
                "temp_directory": str(Path(".").resolve()),
            }
        )
        self._connection.execute("FORCE INSTALL substrait FROM core_nightly")
        self._connection.execute("LOAD substrait")

        return self._connection

    def reset_connection(self):
        """Reset the connection to the backend."""
        self._connection.close()
        self._connection = None
        self.create_connection()
        for table in self._file_tables.values():
            self.register_table(*table)
        for table in self._data_tables.values():
            self.register_table_with_arrow_data(*table)

    @contextmanager
    def adjust_plan(self, plan: plan_pb2.Plan) -> Iterator[plan_pb2.Plan]:
        """Modify the given Substrait plan for use with DuckDB."""
        RenameFunctionsForDuckDB().visit_plan(plan)

        yield plan

    # ruff: noqa: BLE001
    def _execute_plan(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against DuckDB."""
        if True:
            plan.relations[0].root.names.append("custid")
            plan.relations[0].root.names.append("custname")
        plan_data = plan.SerializeToString()

        try:
            query_result = self._connection.from_substrait(proto=plan_data)
        except Exception as err:
            raise ValueError(f"DuckDB Execution Error: {err}") from err
        arrow = query_result.arrow()
        return reapply_names(arrow, plan.relations[0].root.names)

    def register_table(
        self,
        table_name: str,
        location: Path,
        file_format: str = "parquet",
        temporary: bool = False,
        replace: bool = False,
    ) -> None:
        """Register the given table with the backend."""
        if replace:
            try:
                self._connection.table(table_name)
                self._connection.execute(f"DROP TABLE {table_name}")
                if table_name in self._file_tables:
                    del self._file_tables[table_name]
                if table_name in self._data_tables:
                    del self._data_tables[table_name]
            except Exception:
                pass

        files = Backend._expand_location(location)
        if not files:
            raise ValueError(f"No parquet files found at {location}")

        if not temporary:
            self._file_tables[table_name] = (table_name, location, file_format)
        if self._use_duckdb_python_api:
            self._connection.register(table_name, self._connection.read_parquet(files))
        else:
            files_str = ", ".join([f"'{f}'" for f in files])
            files_sql = f"CREATE OR REPLACE TABLE {table_name} AS FROM read_parquet([{files_str}])"
            self._connection.execute(files_sql)

    # ruff: noqa: BLE001
    def register_table_with_arrow_data(
        self, name: str, data: bytes, temporary: bool = False, replace: bool = False
    ) -> None:
        """Register the given arrow data as a table with the backend."""
        if replace:
            try:
                self._connection.table(name)
                self._connection.execute(f"DROP TABLE {name}")
                if name in self._file_tables:
                    del self._file_tables[name]
                if name in self._data_tables:
                    del self._data_tables[name]
            except Exception:
                pass

        if self._register_virtual_tables_as_local:
            DuckDBBackend._TABLES_FOUND += 1
            table_name = f"virtual_table{DuckDBBackend._TABLES_FOUND}"
            location = Path(f"./{table_name}.parquet").absolute()
            table = pa.ipc.open_stream(data).read_all()
            pq.write_table(table, location)

            self.register_table(name, location, temporary=temporary, replace=replace)
            return

        if not temporary:
            self._data_tables[name] = (name, data)
        r = pa.ipc.open_stream(data)
        self._connection.register(name, self._connection.from_arrow(r.read_all()))

    def describe_files(self, paths: list[str]):
        """Asks the backend to describe the given files."""
        files = paths
        if len(paths) == 1:
            files = Backend._expand_location(paths[0])
        # TODO -- Handle resolution of a combined schema.
        df = self._connection.read_parquet(files)
        schema = df.fetch_arrow_reader().schema
        # TODO -- Figure out if we still need this check.
        if "aggr" in schema.names:
            raise ValueError("Aggr column found in schema")
        return schema

    def describe_table(self, name: str):
        """Asks the backend to describe the given table."""
        table = self._connection.table(name)
        schema = table.to_arrow_table().schema
        if "aggr" in schema.names:
            raise ValueError("Aggr column found in schema")
        return schema

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Convert SQL into a Substrait plan."""
        plan = plan_pb2.Plan()
        proto_bytes = self._connection.get_substrait(query=sql.replace("`", "'")).fetchone()[0]
        plan.ParseFromString(proto_bytes)
        return plan
