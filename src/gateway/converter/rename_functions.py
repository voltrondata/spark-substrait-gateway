# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from substrait.gen.proto import plan_pb2

from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor


# pylint: disable=no-member,fixme
class RenameFunctions(SubstraitPlanVisitor):
    """Renames Substrait functions to match what Datafusion expects."""

    def visit_plan(self, plan: plan_pb2.Plan) -> None:
        """Modifies the provided plan so that functions are Datafusion compatible."""
        super().visit_plan(plan)

        for extension in plan.extensions:
            if extension.WhichOneof('mapping_type') != 'extension_function':
                continue

            # TODO -- Take the URI references into account.
            if extension.extension_function.name == 'substring':
                extension.extension_function.name = 'substr'
