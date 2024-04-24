# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor
from substrait.gen.proto import plan_pb2


# pylint: disable=no-member,fixme
class RenameFunctionsForDatafusion(SubstraitPlanVisitor):
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


# pylint: disable=no-member,fixme
class RenameFunctionsForArrow(SubstraitPlanVisitor):
    """Renames Substrait functions to match what Datafusion expects."""

    def __init__(self, use_uri_workaround=False):
        """Initialize the RenameFunctionsForArrow class."""
        self._extensions: dict[int, str] = {}
        self._use_uri_workaround = use_uri_workaround
        super().__init__()

    def normalize_extension_uris(self, plan: plan_pb2.Plan) -> None:
        """Normalize the URI."""
        for extension in plan.extension_uris:
            if self._use_uri_workaround:
                extension.uri = 'urn:arrow:substrait_simple_extension_function'
            else:
                if extension.uri.startswith('/'):
                    extension.uri = extension.uri.replace(
                        '/', 'https://github.com/substrait-io/substrait/blob/main/extensions/')

    def index_extension_uris(self, plan: plan_pb2.Plan) -> None:
        """Add the extension URIs into a dictionary."""
        self._extensions: dict[int, str] = {}
        for extension in plan.extension_uris:
            self._extensions[extension.extension_uri_anchor] = extension.uri

    def visit_plan(self, plan: plan_pb2.Plan) -> None:
        """Modify the provided plan so that functions are Arrow compatible."""
        super().visit_plan(plan)

        self.normalize_extension_uris(plan)
        self.index_extension_uris(plan)

        for extension in plan.extensions:
            if extension.WhichOneof('mapping_type') != 'extension_function':
                continue

            if ':' in extension.extension_function.name:
                name, signature = extension.extension_function.name.split(':', 2)
            else:
                name = extension.extension_function.name
                signature = None

            # TODO -- Take the URI references into account.
            changed = False
            if name == 'char_length':
                changed = True
                name = 'utf8_length'

            if not changed:
                continue

            if signature:
                extension.extension_function.name = f'{name}:{signature}'
            else:
                extension.extension_function.name = name
