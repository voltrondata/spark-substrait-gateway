# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for virtual tables."""
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from substrait.gen.proto import algebra_pb2, plan_pb2, type_pb2

from substrait_visitors.substrait_plan_visitor import SubstraitPlanVisitor


# pylint: disable=no-member
class ReplaceVirtualTablesWithNamedTable(SubstraitPlanVisitor):
    """Replaces all the virtual table instances with named tables."""

    _TABLES_FOUND: int = 0

    def __init__(self):
        """Initialize the visitor."""
        self._table_definitions: list[tuple[str, Path]] = []
        self._schema: type_pb2.NamedStruct | None = None

        super().__init__()

    def get_arrow_value_from_literal(self, literal: algebra_pb2.Expression.Literal) -> Any:
        """Return the value from a Substrait literal."""
        # TODO -- Handle the rest of the types including maps, lists, and structs.
        match literal.WhichOneof('literal_type'):
            case 'boolean':
                return literal.boolean
            case 'i8':
                return literal.i8
            case 'i16':
                return literal.i16
            case 'i32':
                return literal.i32
            case 'i64':
                return literal.i64
            case 'fp32':
                return literal.fp32
            case 'fp64':
                return literal.fp64
            case 'string':
                return literal.string
            case 'binary':
                return literal.binary
            case 'fixed_char':
                return literal.fixed_char
            case 'var_char':
                return literal.var_char
            case 'null':
                return None
            case _:
                raise ValueError(f'Unknown literal type in virtual table: {literal}')

    def create_arrow_table(self, virtual_table: algebra_pb2.ReadRel.VirtualTable) -> pa.Table:
        """Create an Arrow Table from the given Substrait virtual table."""
        table_data = []
        # TODO -- Ensure that the schema of the virtual table matches the base_schema.
        for values in virtual_table.values:
            row = {}
            for idx, field in enumerate(values.fields):
                row[self._schema.names[idx]] = self.get_arrow_value_from_literal(field)
            table_data.append(row)
        return pa.Table.from_pylist(table_data)

    def visit_virtual_table(self, virtual_table: algebra_pb2.ReadRel.VirtualTable) -> Any:
        """Visit a virtual table definition."""
        super().visit_virtual_table(virtual_table)
        table = self.create_arrow_table(virtual_table)

        ReplaceVirtualTablesWithNamedTable._TABLES_FOUND += 1
        table_name = f'virtual_table{ReplaceVirtualTablesWithNamedTable._TABLES_FOUND}'
        location = Path(f'./{table_name}.parquet').absolute()
        self._table_definitions.append((table_name, location))
        pq.write_table(table, location)

    def visit_read_relation(self, rel: algebra_pb2.ReadRel) -> Any:
        """Visit a read relation node."""
        self._schema = rel.base_schema
        super().visit_read_relation(rel)
        if rel.HasField('virtual_table'):
            rel.ClearField('virtual_table')
            rel.named_table.names.append(self._table_definitions[-1][0])
        self._schema = None

    def visit_plan(self, plan: plan_pb2.Plan) -> list[tuple[str, Path]]:
        """Modify the provided plan so that Local Files are replaced with Named Tables."""
        super().visit_plan(plan)
        return self._table_definitions
