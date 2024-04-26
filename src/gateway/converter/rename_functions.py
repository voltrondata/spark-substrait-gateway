# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor
from substrait.gen.proto import plan_pb2


# pylint: disable=no-member,fixme
class RenameFunctions(SubstraitPlanVisitor):
    """Renames Substrait functions to match what Datafusion expects."""

    def visit_plan(self, plan: plan_pb2.Plan) -> None:
        """Modify the provided plan so that functions are Datafusion compatible."""
        super().visit_plan(plan)

        for extension in plan.extensions:
            if extension.WhichOneof('mapping_type') != 'extension_function':
                continue

            if ':' in extension.extension_function.name:
                extension.extension_function.name = extension.extension_function.name.split(':')[0]

            # TODO -- Take the URI references into account.
            if extension.extension_function.name == 'substring':
                extension.extension_function.name = 'substr'
            elif extension.extension_function.name == '*':
                extension.extension_function.name = 'multiply'
            elif extension.extension_function.name == '-':
                extension.extension_function.name = 'subtract'
            elif extension.extension_function.name == '+':
                extension.extension_function.name = 'add'
            elif extension.extension_function.name == '/':
                extension.extension_function.name = 'divide'
            elif extension.extension_function.name == 'contains':
                extension.extension_function.name = 'instr'
            elif extension.extension_function.name == 'extract':
                extension.extension_function.name = 'date_part'
