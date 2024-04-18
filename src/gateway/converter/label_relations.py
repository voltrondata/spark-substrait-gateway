# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from typing import Any

from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor
from substrait.gen.proto import algebra_pb2


# pylint: disable=E1101
def get_common_section(rel: algebra_pb2.Rel) -> algebra_pb2.RelCommon:
    """Finds the single input to the relation."""
    match rel.WhichOneof('rel_type'):
        case 'read':
            result = rel.read.common
        case 'filter':
            result = rel.filter.common
        case 'fetch':
            result = rel.fetch.common
        case 'aggregate':
            result = rel.aggregate.common
        case 'sort':
            result = rel.sort.common
        case 'join':
            result = rel.join.common
        case 'project':
            result = rel.project.common
        case 'set':
            result = rel.set.common
        case 'extension_single':
            result = rel.extension_single.common
        case 'extension_multi':
            result = rel.extension_multi.common
        case 'extension_leaf':
            result = rel.extension_leaf.common
        case 'cross':
            result = rel.cross.common
        case 'reference':
            raise ValueError('Reference relations do not have a common section')
        case 'write':
            result = rel.write.common
        case 'ddl':
            result = rel.ddl.common
        case 'hash_join':
            result = rel.hash_join.common
        case 'merge_join':
            result = rel.merge_join.common
        case 'nested_loop_join':
            result = rel.nested_loop_join.common
        case 'window':
            result = rel.window.common
        case 'exchange':
            result = rel.exchange.common
        case 'expand':
            result = rel.expand.common
        case _:
            raise NotImplementedError('Finding the common section for type '
                                      f'{rel.WhichOneof("rel_type")} is not implemented')
    return result


# pylint: disable=E1101,no-member,fixme
class LabelRelations(SubstraitPlanVisitor):
    """Replaces all cast expressions with projects of casts instead."""

    _seen_relations: int

    def __init__(self):
        super().__init__()
        self._seen_relations = 0

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        # TODO -- Use something more disciplined than ReferenceRel here.
        label = algebra_pb2.ReferenceRel(subtree_ordinal=self._seen_relations)
        get_common_section(rel).advanced_extension.optimization.Pack(label)
        self._seen_relations += 1
        super().visit_relation(rel)


# pylint: disable=E1101,no-member
class UnlabelRelations(SubstraitPlanVisitor):
    """Removes all labels created by LabelRelations from relations."""

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        get_common_section(rel).ClearField('advanced_extension')
        super().visit_relation(rel)
