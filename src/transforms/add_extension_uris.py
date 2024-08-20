# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from substrait.gen.proto import plan_pb2
from substrait.gen.proto.extensions import extensions_pb2

from substrait_visitors.substrait_plan_visitor import SubstraitPlanVisitor


# pylint: disable=E1101,no-member
class AddExtensionUris(SubstraitPlanVisitor):
    """Ensures that the plan has extension URI definitions for all references."""

    def visit_plan(self, plan: plan_pb2.Plan) -> None:
        """Modify the provided plan so that all functions have URI references."""
        super().visit_plan(plan)

        known_uris: list[int] = []
        for uri in plan.extension_uris:
            known_uris.append(uri.extension_uri_anchor)

        for extension in plan.extensions:
            if extension.WhichOneof('mapping_type') != 'extension_function':
                continue

            if extension.extension_function.extension_uri_reference not in known_uris:
                # TODO -- Make sure this hack occurs at most once.
                uri = extensions_pb2.SimpleExtensionURI(
                    uri='urn:arrow:substrait_simple_extension_function',
                    extension_uri_anchor=extension.extension_function.extension_uri_reference)
                plan.extension_uris.append(uri)
                known_uris.append(extension.extension_function.extension_uri_reference)
