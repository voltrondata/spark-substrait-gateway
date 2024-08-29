# SPDX-License-Identifier: Apache-2.0
"""Provides access to the SQL to Substrait compiler in Ibis."""
from pathlib import Path

import ibis
import pyarrow as pa
import pyarrow.parquet as pq
from ibis.expr.sql import parse_sql
from ibis_substrait.compiler.core import SubstraitCompiler
from substrait.gen.proto import plan_pb2

from backends.backend import Backend


# pylint: disable=fixme
class IbisBackend(Backend):
    """Provides access to convert SQL to Substrait using Ibis."""

    def __init__(self, options):
        """Initialize the Ibis compiler."""
        self._table_schemas: dict[str, ibis.Schema] = {}
        super().__init__(options)

    def register_table(
        self,
        table_name: str,
        location: Path,
        file_format: str = "parquet",
        temporary: bool = False,
        replace: bool = False,
    ) -> None:
        """Register the given table with the backend."""
        r = pq.read_table(location)
        self._table_schemas[table_name] = ibis.Schema.from_pyarrow(r.schema)

    def register_table_with_arrow_data(
        self, name: str, data: bytes, temporary: bool = False, replace: bool = False
    ) -> None:
        """Register the given arrow data as a table with the backend."""
        r = pa.ipc.open_stream(data)
        self._table_schemas[name] = ibis.Schema.from_pyarrow(r.schema)

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Convert SQL into a Substrait plan."""
        expr = parse_sql(sql, catalog=self._table_schemas, dialect="spark")
        compiler = SubstraitCompiler()
        return compiler.compile(expr)
