# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from typing import Any, List, Optional

from substrait.gen.proto import algebra_pb2

from gateway.converter.output_field_tracking_visitor import get_plan_id
from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor
from gateway.converter.symbol_table import SymbolTable


# pylint: disable=no-member,fixme
class SimplifyCasts(SubstraitPlanVisitor):
    """Replaces all cast expressions with projects of casts instead."""

    def __init__(self, symbol_table: SymbolTable):
        super().__init__()
        self._symbol_table = symbol_table
        self._current_plan_id: Optional[int] = None  # The relation currently being processed.

        self._rewrite_expressions: List[algebra_pb2.Expression] = []
        self._previous_rewrite_expressions: List[List[algebra_pb2.Expression]] = []

    def visit_cast(self, cast: algebra_pb2.Expression.Cast) -> Any:
        """Visits a cast node."""
        super().visit_cast(cast)

        # Acero only accepts casts of selections.
        if cast.input.WhichOneof('rex_type') != 'selection':
            old_input = algebra_pb2.Expression()
            old_input.CopyFrom(cast.input)
            self._rewrite_expressions.append(old_input)

            symbol = self._symbol_table.get_symbol(self._current_plan_id)
            field_reference = len(symbol.input_fields) + symbol.cast_expressions_projected
            symbol.cast_expressions_projected += 1
            cast.input.CopyFrom(
                algebra_pb2.Expression(selection=algebra_pb2.Expression.FieldReference(
                    direct_reference=algebra_pb2.Expression.ReferenceSegment(
                        struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(
                            field=field_reference)),
                    root_reference=algebra_pb2.Expression.FieldReference.RootReference()))
            )

    @staticmethod
    def find_single_input(rel: algebra_pb2.Rel) -> algebra_pb2.Rel:
        """Finds the single input to the relation."""
        match rel.WhichOneof('rel_type'):
            case 'filter':
                return rel.filter.input
            case 'fetch':
                return rel.fetch.input
            case 'aggregate':
                return rel.aggregate.input
            case 'sort':
                return rel.sort.input
            case 'project':
                return rel.project.input
            case 'extension_single':
                return rel.extension_single.input
            case _:
                raise NotImplementedError('Finding single inputs of relations with type '
                                          f'{rel.WhichOneof('rel_type')} are not implemented')

    @staticmethod
    def replace_single_input(rel: algebra_pb2.Rel, new_input: algebra_pb2.Rel):
        """Updates the single input to the relation."""
        match rel.WhichOneof('rel_type'):
            case 'filter':
                rel.filter.input.CopyFrom(new_input)
            case 'fetch':
                rel.fetch.input.CopyFrom(new_input)
            case 'aggregate':
                rel.aggregate.input.CopyFrom(new_input)
            case 'sort':
                rel.sort.input.CopyFrom(new_input)
            case 'project':
                rel.project.input.CopyFrom(new_input)
            case 'extension_single':
                rel.extension_single.input.CopyFrom(new_input)
            case _:
                raise NotImplementedError('Modifying inputs of relations with type '
                                          f'{rel.WhichOneof('rel_type')} are not implemented')

    def update_field_references(self, plan_id: int) -> None:
        """Uses the field references using the specified portion of the plan."""
        source_symbol = self._symbol_table.get_symbol(plan_id)
        current_symbol = self._symbol_table.get_symbol(self._current_plan_id)
        current_symbol.input_fields.extend(source_symbol.output_fields)
        current_symbol.output_fields.extend(current_symbol.input_fields)

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        previous_plan_id = self._current_plan_id
        self._current_plan_id = get_plan_id(rel)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        symbol.cast_expressions_projected = 0

        self._previous_rewrite_expressions.append(self._rewrite_expressions)
        self._rewrite_expressions = []

        super().visit_relation(rel)

        if symbol.cast_expressions_projected:
            old_input = self.find_single_input(rel)
            new_input = algebra_pb2.Rel(
                project=algebra_pb2.ProjectRel(
                    common=algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct()),
                    input=old_input))
            for expr in self._rewrite_expressions:
                new_input.project.expressions.append(expr)

            # Explicitly set the output mapping to exclude our new expressions.
            # TODO -- Deal with the case where there was an output mapping already present.
            for ref in range(0, len(symbol.input_fields)):
                new_input.project.common.emit.output_mapping.append(ref)
            for ref in range(0, len(symbol.generated_fields)):
                new_input.project.common.emit.output_mapping.append(
                    len(symbol.input_fields) + len(self._rewrite_expressions) + ref)

            self.replace_single_input(rel, new_input)

        self._rewrite_expressions = self._previous_rewrite_expressions.pop()

        self._current_plan_id = previous_plan_id
