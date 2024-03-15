# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from typing import Any, List, Tuple

from substrait.gen.proto import algebra_pb2, plan_pb2

from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor


# pylint: disable=no-member
class ReplaceLocalFilesWithNamedTable(SubstraitPlanVisitor):
    """Replaces all of the local file instances with named tables."""

    def __init__(self):
        self._file_groups: List[Tuple[str, List[str]]] = []

        super().__init__()

    def visit_local_files(self, local_files: algebra_pb2.ReadRel.LocalFiles) -> Any:
        """Visits a local files node."""
        files = []
        for item in local_files.items:
            files.append(item.uri_file)
        super().visit_local_files(local_files)
        self._file_groups.append(('possible_table_name', files))

    def visit_read_relation(self, rel: algebra_pb2.ReadRel) -> Any:
        """Visits a read relation node."""
        super().visit_read_relation(rel)
        if rel.HasField('local_files'):
            rel.ClearField('local_files')
            rel.named_table.names.append(self._file_groups[-1][0])

    def visit_plan(self, plan: plan_pb2.Plan) -> List[Tuple[str, List[str]]]:
        """Modifies the provided plan so that Local Files are replaced with Named Tables."""
        super().visit_plan(plan)
        return self._file_groups
