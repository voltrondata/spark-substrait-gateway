# SPDX-License-Identifier: Apache-2.0
"""Abstract visitor class for Substrait plans."""
from typing import Any

from substrait.gen.proto import algebra_pb2, plan_pb2, type_pb2
from substrait.gen.proto.extensions import extensions_pb2


class SubstraitPlanVisitor:
    """Base class that visits all the parts of a Substrait plan."""

    def visit_subquery_scalar(self, subquery: algebra_pb2.Expression.Subquery.Scalar) -> Any:
        """Visits a scalar subquery."""
        if subquery.HasField('input'):
            self.visit_relation(subquery.input)

    def visit_subquery_in_predicate(self,
                                    subquery: algebra_pb2.Expression.Subquery.InPredicate) -> Any:
        """Visits an in predicate."""
        if subquery.HasField('haystack'):
            self.visit_relation(subquery.haystack)

    def visit_subquery_set_predicate(self,
                                     subquery: algebra_pb2.Expression.Subquery.SetPredicate) -> Any:
        """Visits a set predicate."""
        if subquery.HasField('tuples'):
            self.visit_relation(subquery.tuples)

    def visit_subquery_set_comparison(
            self, subquery: algebra_pb2.Expression.Subquery.SetComparison) -> Any:
        """Visits a set comparison."""
        if subquery.HasField('left'):
            self.visit_expression(subquery.left)
        if subquery.HasField('right'):
            self.visit_relation(subquery.right)

    def visit_nested_struct(self, structure: algebra_pb2.Expression.Nested.Struct) -> Any:
        """Visits a nested struct."""
        for field in structure.fields:
            self.visit_expression(field)

    def visit_nested_list(self, list_literal: algebra_pb2.Expression.Nested.List) -> Any:
        """Visits a nested list."""
        for value in list_literal.values:
            self.visit_expression(value)

    def visit_nested_map(self, map_literal: algebra_pb2.Expression.Nested.Map) -> Any:
        """Visits a nested map."""
        for key_value in map_literal.key_values:
            self.visit_nested_key_value(key_value)

    def visit_nested_key_value(self, key_value: algebra_pb2.Expression.Nested.Map.KeyValue) -> Any:
        """Visits a nested key value."""
        if key_value.HasField('key'):
            self.visit_expression(key_value.key)
        if key_value.HasField('value'):
            self.visit_expression(key_value.value)

    def visit_struct_item(self, item: algebra_pb2.Expression.MaskExpression.StructItem) -> Any:
        """Visits a struct item."""
        if item.HasField('child'):
            self.visit_select(item.child)

    def visit_reference_segment_map_key(
            self, map_key: algebra_pb2.Expression.ReferenceSegment.MapKey) -> Any:
        """Visits a map key."""
        if map_key.HasField('map_key'):
            self.visit_literal(map_key.map_key)
        if map_key.HasField('child'):
            self.visit_reference_segment(map_key.child)

    def visit_reference_segment_struct_field(
            self, field: algebra_pb2.Expression.ReferenceSegment.StructField) -> Any:
        """Visits a struct field."""
        if field.HasField('child'):
            self.visit_reference_segment(field.child)

    def visit_reference_segment_list_element(
            self, element: algebra_pb2.Expression.ReferenceSegment.ListElement) -> Any:
        """Visits a list element."""
        if element.HasField('child'):
            self.visit_reference_segment(element.child)

    def visit_select(self, select: algebra_pb2.Expression.MaskExpression.Select) -> Any:
        """Visits a select."""
        match select.WhichOneof('type'):
            case 'struct':
                return self.visit_struct_select(select.struct)
            case 'list':
                return self.visit_list_select(select.list)
            case 'map':
                return self.visit_map_select(select.map)
            case _:
                raise ValueError(f'Unexpected select type: {select.WhichOneof("type")}')

    def visit_type(self, type_def: type_pb2.Type) -> Any:
        """Visits a type."""
        match type_def.WhichOneof('kind'):
            case 'struct':
                return self.visit_struct(type_def.struct)
            case 'list':
                return self.visit_type_list(type_def.list)
            case 'map':
                return self.visit_type_map(type_def.map)
            case 'user_defined':
                return self.visit_type_user_defined(type_def.user_defined)
            case 'user_defined_type_reference':
                raise ValueError('user_defined_type_reference was replaced by user_defined_type.  '
                                 'Please update your plan version.')

    def visit_type_user_defined(self, user_defined: type_pb2.Type.UserDefined) -> Any:
        """Visits a user defined type."""
        for parameter in user_defined.type_parameters:
            self.visit_type_parameter(parameter)

    def visit_type_parameter(self, parameter: type_pb2.Type.Parameter) -> Any:
        """Visits a type parameter."""
        if parameter.WhichOneof('parameter') == 'data_type':
            self.visit_type(parameter.data_type)

    def visit_map(self, map_literal: algebra_pb2.Expression.Literal.Map) -> Any:
        """Visits a map."""
        for key_value in map_literal.key_values:
            self.visit_map_key_value(key_value)

    def visit_map_key_value(self, key_value: algebra_pb2.Expression.Literal.Map.KeyValue) -> Any:
        """Visits a map key value."""
        if key_value.HasField('key'):
            self.visit_literal(key_value.key)
        if key_value.HasField('value'):
            self.visit_literal(key_value.value)

    def visit_list(self, list_expression: algebra_pb2.Expression.Literal.List) -> Any:
        """Visits a list."""
        for value in list_expression.values:
            self.visit_literal(value)

    def visit_type_list(self, list_type: type_pb2.Type.List) -> Any:
        """Visits a list."""
        if list_type.HasField('type'):
            self.visit_type(list_type.type)

    def visit_type_map(self, map_type: type_pb2.Type.Map) -> Any:
        """Visits a map."""
        if map_type.HasField('key'):
            self.visit_type(map_type.key)
        if map_type.HasField('value'):
            self.visit_type(map_type.value)

    def visit_user_defined(self, user_defined: algebra_pb2.Expression.Literal.UserDefined) -> Any:
        """Visits a user defined expression."""
        for parameter in user_defined.type_parameters:
            self.visit_type_parameter(parameter)

    def visit_function_argument(self, argument: algebra_pb2.FunctionArgument) -> Any:
        """Visits a function argument."""
        match argument.WhichOneof('arg_type'):
            case 'enum':
                return None
            case 'type':
                return self.visit_type(argument.type)
            case 'value':
                return self.visit_expression(argument.value)
            case _:
                raise ValueError(
                    f'Unexpected argument type: {argument.WhichOneof("arg_type")}')

    def visit_function_option(self, _: algebra_pb2.FunctionOption) -> Any:
        """Visits a function option."""
        return None

    def visit_record(self, record: algebra_pb2.Expression.MultiOrList.Record) -> Any:
        """Visits a record."""
        for field in record.fields:
            self.visit_expression(field)

    def visit_if_value(self, if_value: algebra_pb2.Expression.SwitchExpression.IfValue) -> Any:
        """Visits an if value."""
        if if_value.HasField('if'):
            self.visit_literal(getattr(if_value, 'if'))
        if if_value.HasField('then'):
            self.visit_expression(if_value.then)

    def visit_struct(self, structure: type_pb2.Type.Struct) -> Any:
        """Visits a struct."""
        for t in structure.types:
            self.visit_type(t)

    def visit_literal(self, literal: algebra_pb2.Expression.Literal) -> Any:
        """Visits a literal."""
        match literal.WhichOneof('literal_type'):
            case 'struct':
                self.visit_expression_literal_struct(literal.struct)
            case 'map':
                self.visit_map(literal.map)
            case 'null':
                self.visit_type(literal.null)
            case 'list':
                self.visit_list(literal.list)
            case 'empty_list':
                self.visit_type_list(literal.empty_list)
            case 'empty_map':
                self.visit_type_map(literal.empty_map)
            case 'user_defined':
                self.visit_user_defined(literal.user_defined)

    def visit_scalar_function(self, function: algebra_pb2.Expression.ScalarFunction) -> Any:
        """Visits a scalar function."""
        for arg in function.arguments:
            self.visit_function_argument(arg)
        for option in function.options:
            self.visit_function_option(option)
        if function.HasField('output_type'):
            self.visit_type(function.output_type)
        for arg in function.args:
            self.visit_expression(arg)

    def visit_window_function(self, function: algebra_pb2.Expression.WindowFunction) -> Any:
        """Visits a window function."""
        for arg in function.arguments:
            self.visit_function_argument(arg)
        for option in function.options:
            self.visit_function_option(option)
        if function.HasField('output_type'):
            self.visit_type(function.output_type)
        for sort in function.sorts:
            self.visit_sort_field(sort)
        for partition in function.partitions:
            self.visit_expression(partition)

    def visit_window_rel_function(
            self, function: algebra_pb2.ConsistentPartitionWindowRel.WindowRelFunction) -> Any:
        """Visits a window relation function."""
        for arg in function.arguments:
            self.visit_function_argument(arg)
        for option in function.options:
            self.visit_function_option(option)
        if function.HasField('output_type'):
            self.visit_type(function.output_type)

    def visit_if_clause(self, if_clause: algebra_pb2.Expression.IfThen.IfClause) -> Any:
        """Visits an if value."""
        if if_clause.HasField('if'):
            self.visit_expression(getattr(if_clause, 'if'))
        if if_clause.HasField('then'):
            self.visit_expression(if_clause.then)

    def visit_if_then(self, if_then: algebra_pb2.Expression.IfThen) -> Any:
        """Visits an if then."""
        for if_then_if in if_then.ifs:
            self.visit_if_clause(if_then_if)
        if if_then.HasField('else'):
            self.visit_expression(getattr(if_then, 'else'))

    def visit_switch_expression(self, expression: algebra_pb2.Expression.SwitchExpression) -> Any:
        """Visits a switch expression."""
        if expression.HasField('match'):
            self.visit_expression(expression.match)
        for if_then_if in expression.ifs:
            self.visit_if_value(if_then_if)
        if expression.HasField('else'):
            self.visit_expression(getattr(expression, 'else'))

    def visit_singular_or_list(self,
                               singular_or_list: algebra_pb2.Expression.SingularOrList) -> Any:
        """Visits a singular or list."""
        if singular_or_list.HasField('value'):
            self.visit_expression(singular_or_list.value)
        for option in singular_or_list.options:
            self.visit_expression(option)

    def visit_multi_or_list(self, multi_or_list: algebra_pb2.Expression.MultiOrList) -> Any:
        """Visits a multi or list."""
        for value in multi_or_list.value:
            self.visit_expression(value)
        for option in multi_or_list.options:
            self.visit_record(option)

    def visit_cast(self, cast: algebra_pb2.Expression.Cast) -> Any:
        """Visits a cast."""
        if cast.HasField('input'):
            self.visit_expression(cast.input)
        if cast.HasField('type'):
            self.visit_type(cast.type)

    def visit_subquery(self, subquery: algebra_pb2.Expression.Subquery) -> Any:
        """Visits a subquery."""
        match subquery.WhichOneof('subquery_type'):
            case 'scalar':
                return self.visit_subquery_scalar(subquery.scalar)
            case 'in_predicate':
                return self.visit_subquery_in_predicate(subquery.in_predicate)
            case 'set_predicate':
                return self.visit_subquery_set_predicate(subquery.set_predicate)
            case 'set_comparison':
                return self.visit_subquery_set_comparison(subquery.set_comparison)
            case _:
                raise ValueError(
                    f'Unexpected subquery type: {subquery.WhichOneof("subquery_type")}')

    def visit_nested(self, structure: algebra_pb2.Expression.Nested) -> Any:
        """Visits a nested."""
        match structure.WhichOneof('nested_type'):
            case 'struct':
                return self.visit_nested_struct(structure.struct)
            case 'list':
                return self.visit_nested_list(structure.list)
            case 'map':
                return self.visit_nested_map(structure.map)
            case _:
                raise ValueError(
                    f'Unexpected nested type: {structure.WhichOneof("nested_type")}')

    def visit_enum(self, _: algebra_pb2.Expression.Enum) -> Any:
        """Visits an enum."""
        return None

    def visit_struct_select(self,
                            structure: algebra_pb2.Expression.MaskExpression.StructSelect) -> Any:
        """Visits a struct select."""
        for item in structure.struct_items:
            self.visit_struct_item(item)

    def visit_list_select(self, select: algebra_pb2.Expression.MaskExpression.ListSelect) -> Any:
        """Visits a list select."""
        for item in select.selection:
            self.visit_list_select_item(item)
        if select.HasField('child'):
            self.visit_select(select.child)

    def visit_list_select_item(
            self, _: algebra_pb2.Expression.MaskExpression.ListSelect.ListSelectItem) -> Any:
        """Visits a list select item."""
        return None

    def visit_map_select(self, select: algebra_pb2.Expression.MaskExpression.MapSelect) -> Any:
        """Visits a map select."""
        if select.HasField('child'):
            self.visit_select(select.child)
        match select.WhichOneof('select'):
            case 'key':
                return None
            case 'expression':
                return None
            case _:
                raise ValueError(f'Unexpected select case: {select.WhichOneof("select")}')

    def visit_expression_literal_struct(self, struct: algebra_pb2.Expression.Literal.Struct) -> Any:
        """Visits an expression literal struct."""
        for literal in struct.fields:
            self.visit_literal(literal)

    def visit_file_or_files(self, _: algebra_pb2.ReadRel.LocalFiles.FileOrFiles) -> Any:
        """Visits a file or files."""
        return None

    def visit_aggregate_function(self, structure: algebra_pb2.AggregateFunction) -> Any:
        """Visits an aggregate function."""
        for arg in structure.arguments:
            self.visit_function_argument(arg)
        for option in structure.options:
            self.visit_function_option(option)
        if structure.HasField('output_type'):
            self.visit_type(structure.output_type)
        for sort in structure.sorts:
            self.visit_sort_field(sort)
        for arg in structure.args:
            self.visit_expression(arg)

    def visit_reference_segment(self, segment: algebra_pb2.Expression.ReferenceSegment) -> Any:
        """Visits a reference segment."""
        match segment.WhichOneof('reference_type'):
            case 'map_key':
                return self.visit_reference_segment_map_key(segment.map_key)
            case 'struct_field':
                return self.visit_reference_segment_struct_field(segment.struct_field)
            case 'list_element':
                return self.visit_reference_segment_list_element(segment.list_element)
            case _:
                raise ValueError(
                    f'Unexpected reference type case: {segment.WhichOneof("reference_type")}')

    def visit_relation_common(self, common: algebra_pb2.RelCommon) -> Any:
        """Visits a common relation."""
        if common.HasField('advanced_extension'):
            self.visit_advanced_extension(common.advanced_extension)

    def visit_named_struct(self, struct: type_pb2.NamedStruct) -> Any:
        """Visits a named struct."""
        return self.visit_struct(struct.struct)

    def visit_expression(self, expression: algebra_pb2.Expression) -> Any:
        """Visits an expression."""
        match expression.WhichOneof('rex_type'):
            case 'literal':
                self.visit_literal(expression.literal)
            case 'selection':
                self.visit_field_reference(expression.selection)
            case 'scalar_function':
                self.visit_scalar_function(expression.scalar_function)
            case 'window_function':
                self.visit_window_function(expression.window_function)
            case 'if_then':
                self.visit_if_then(expression.if_then)
            case 'switch_expression':
                self.visit_switch_expression(expression.switch_expression)
            case 'singular_or_list':
                self.visit_singular_or_list(expression.singular_or_list)
            case 'multi_or_list':
                self.visit_multi_or_list(expression.multi_or_list)
            case 'cast':
                self.visit_cast(expression.cast)
            case 'subquery':
                self.visit_subquery(expression.subquery)
            case 'nested':
                self.visit_nested(expression.nested)
            case 'enum':
                self.visit_enum(expression.enum)
            case _:
                raise ValueError(
                    f'Unexpected expression type: {expression.WhichOneof("rex_type")}')

    def visit_mask_expression(self, expression: algebra_pb2.Expression.MaskExpression) -> Any:
        """Visits a mask expression."""
        if expression.HasField('select'):
            self.visit_struct_select(expression.select)

    def visit_virtual_table(self, table: algebra_pb2.ReadRel.VirtualTable) -> Any:
        """Visits a virtual table."""
        for value in table.values:
            self.visit_expression_literal_struct(value)

    def visit_local_files(self, local_files: algebra_pb2.ReadRel.LocalFiles) -> Any:
        """Visits a local files."""
        for item in local_files.items:
            self.visit_file_or_files(item)
        if local_files.HasField('advanced_extension'):
            self.visit_advanced_extension(local_files.advanced_extension)

    def visit_named_table(self, table: algebra_pb2.ReadRel.NamedTable) -> Any:
        """Visits a named table."""
        if table.HasField('advanced_extension'):
            self.visit_advanced_extension(table.advanced_extension)

    def visit_extension_table(self, _: algebra_pb2.ReadRel.ExtensionTable) -> Any:
        """Visits an extension table."""
        return None

    def visit_grouping(self, grouping: algebra_pb2.AggregateRel.Grouping) -> Any:
        """Visits a grouping."""
        for expr in grouping.grouping_expressions:
            self.visit_expression(expr)

    def visit_measure(self, measure: algebra_pb2.AggregateRel.Measure) -> Any:
        """Visits a measure."""
        if measure.HasField('measure'):
            self.visit_aggregate_function(measure.measure)
        if measure.HasField('filter'):
            self.visit_expression(measure.filter)

    def visit_sort_field(self, sort: algebra_pb2.SortField) -> Any:
        """Visits a sort field."""
        if sort.HasField('expr'):
            self.visit_expression(sort.expr)

    def visit_field_reference(self, ref: algebra_pb2.Expression.FieldReference) -> Any:
        """Visits a field reference."""
        if ref.HasField('direct_reference'):
            self.visit_reference_segment(ref.direct_reference)
        if ref.HasField('masked_reference'):
            self.visit_mask_expression(ref.masked_reference)
        if ref.HasField('expression'):
            self.visit_expression(ref.expression)

    def visit_expand_field(self, field: algebra_pb2.ExpandRel.ExpandField) -> Any:
        """Visits an expand field."""
        match field.WhichOneof('field_type'):
            case 'switching_field':
                for switching_field in field.switching_field.duplicates:
                    self.visit_expression(switching_field)
            case 'consistent_field':
                if field.HasField('consistent_field'):
                    self.visit_expression(field.consistent_field)
            case _:
                raise ValueError(f'Unexpected expand field type: {field.WhichOneof("field_type")}')

    def visit_read_relation(self, rel: algebra_pb2.ReadRel) -> Any:
        """Visits a read relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('base_schema'):
            self.visit_named_struct(rel.base_schema)
        if rel.HasField('filter'):
            self.visit_expression(rel.filter)
        if rel.HasField('best_effort_filter'):
            self.visit_expression(rel.best_effort_filter)
        if rel.HasField('projection'):
            self.visit_mask_expression(rel.projection)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)
        match rel.WhichOneof('read_type'):
            case 'virtual_table':
                self.visit_virtual_table(rel.virtual_table)
            case 'local_files':
                self.visit_local_files(rel.local_files)
            case 'named_table':
                self.visit_named_table(rel.named_table)
            case 'extension_table':
                self.visit_extension_table(rel.extension_table)
            case _:
                raise ValueError(f'Unexpected read type: {rel.WhichOneof("read_type")}')

    def visit_filter_relation(self, rel: algebra_pb2.FilterRel) -> Any:
        """Visits a filter relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('condition'):
            self.visit_expression(rel.condition)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)
        if rel.HasField('input'):
            self.visit_relation(rel.input)

    def visit_fetch_relation(self, rel: algebra_pb2.FetchRel) -> Any:
        """Visits a fetch relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)
        if rel.HasField('input'):
            self.visit_relation(rel.input)

    def visit_aggregate_relation(self, rel: algebra_pb2.AggregateRel) -> Any:
        """Visits an aggregate relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        for grouping in rel.groupings:
            self.visit_grouping(grouping)
        for measure in rel.measures:
            self.visit_measure(measure)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)
        if rel.HasField('input'):
            self.visit_relation(rel.input)

    def visit_sort_relation(self, rel: algebra_pb2.SortRel) -> Any:
        """Visits a sort relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        for sort in rel.sorts:
            self.visit_sort_field(sort)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)
        if rel.HasField('input'):
            self.visit_relation(rel.input)

    def visit_join_relation(self, rel: algebra_pb2.JoinRel) -> Any:
        """Visits a join relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('left'):
            self.visit_relation(rel.left)
        if rel.HasField('right'):
            self.visit_relation(rel.right)
        if rel.HasField('expression'):
            self.visit_expression(rel.expression)
        if rel.HasField('post_join_filter'):
            self.visit_expression(rel.post_join_filter)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_project_relation(self, rel: algebra_pb2.ProjectRel) -> Any:
        """Visits a project relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('input'):
            self.visit_relation(rel.input)
        for expr in rel.expressions:
            self.visit_expression(expr)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_set_relation(self, rel: algebra_pb2.SetRel) -> Any:
        """Visits a set relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        for relation in rel.inputs:
            self.visit_relation(relation)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_extension_single_relation(self, rel: algebra_pb2.ExtensionSingleRel) -> Any:
        """Visits an extension single relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('input'):
            self.visit_relation(rel.input)

    def visit_extension_multi_relation(self, rel: algebra_pb2.ExtensionMultiRel) -> Any:
        """Visits an extension multi relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        for relation in rel.inputs:
            self.visit_relation(relation)

    def visit_extension_leaf_relation(self, rel: algebra_pb2.ExtensionLeafRel) -> Any:
        """Visits an extension leaf relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)

    def visit_cross_relation(self, rel: algebra_pb2.CrossRel) -> Any:
        """Visits a cross relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('left'):
            self.visit_relation(rel.left)
        if rel.HasField('right'):
            self.visit_relation(rel.right)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_reference_relation(self, rel: algebra_pb2.ReferenceRel) -> Any:
        """Visits a reference relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)

    def visit_write_relation(self, rel: algebra_pb2.WriteRel) -> Any:
        """Visits a write relation."""
        # TODO -- Add support for write type.
        if rel.HasField('table_schema'):
            self.visit_named_struct(rel.table_schema)
        if rel.HasField('input'):
            self.visit_relation(rel.input)
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)

    def visit_ddl_relation(self, rel: algebra_pb2.DdlRel) -> Any:
        """Visits a DDL relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('table_schema'):
            self.visit_named_struct(rel.table_schema)
        if rel.HasField('table_defaults'):
            self.visit_expression_literal_struct(rel.table_defaults)
        if rel.HasField('view_definition'):
            self.visit_relation(rel.view_definition)
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)

    def visit_hash_join_relation(self, rel: algebra_pb2.HashJoinRel) -> Any:
        """Visits a hash join relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('left'):
            self.visit_relation(rel.left)
        if rel.HasField('right'):
            self.visit_relation(rel.right)
        for key in rel.left_keys:
            self.visit_field_reference(key)
        for key in rel.right_keys:
            self.visit_field_reference(key)
        if rel.HasField('post_join_filter'):
            self.visit_expression(rel.post_join_filter)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_merge_join_relation(self, rel: algebra_pb2.MergeJoinRel) -> Any:
        """Visits a merge join loop relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('left'):
            self.visit_relation(rel.left)
        if rel.HasField('right'):
            self.visit_relation(rel.right)
        for key in rel.left_keys:
            self.visit_field_reference(key)
        for key in rel.right_keys:
            self.visit_field_reference(key)
        if rel.HasField('post_join_filter'):
            self.visit_expression(rel.post_join_filter)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_nested_loop_join_relation(self, rel: algebra_pb2.NestedLoopJoinRel) -> Any:
        """Visits a nested loop join relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('left'):
            self.visit_relation(rel.left)
        if rel.HasField('right'):
            self.visit_relation(rel.right)
        if rel.HasField('expression'):
            self.visit_expression(rel.expression)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_window_relation(self, rel: algebra_pb2.ConsistentPartitionWindowRel) -> Any:
        """Visits a window relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('input'):
            self.visit_relation(rel.input)
        for func in rel.window_functions:
            self.visit_window_function(func)
        for exp in rel.partition_expressions:
            self.visit_expression(exp)
        for sort in rel.sorts:
            self.visit_sort_field(sort)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_exchange_relation(self, rel: algebra_pb2.ExchangeRel) -> Any:
        """Visits an exchange relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('input'):
            self.visit_relation(rel.input)
        if rel.HasField('advanced_extension'):
            self.visit_advanced_extension(rel.advanced_extension)

    def visit_expand_relation(self, rel: algebra_pb2.ExpandRel) -> Any:
        """Visits an expand relation."""
        if rel.HasField('common'):
            self.visit_relation_common(rel.common)
        if rel.HasField('input'):
            self.visit_relation(rel.input)
        for field in rel.fields:
            self.visit_expand_field(field)
        # ExpandRel does not have an advanced_extension like other relations do.

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a Substrait relation."""
        match rel.WhichOneof('rel_type'):
            case 'read':
                self.visit_read_relation(rel.read)
            case 'filter':
                self.visit_filter_relation(rel.filter)
            case 'fetch':
                self.visit_fetch_relation(rel.fetch)
            case 'aggregate':
                self.visit_aggregate_relation(rel.aggregate)
            case 'sort':
                self.visit_sort_relation(rel.sort)
            case 'join':
                self.visit_join_relation(rel.join)
            case 'project':
                self.visit_project_relation(rel.project)
            case 'extension_single':
                self.visit_extension_single_relation(rel.extension_single)
            case 'extension_multi':
                self.visit_extension_multi_relation(rel.extension_multi)
            case 'extension_leaf':
                self.visit_extension_leaf_relation(rel.extension_leaf)
            case 'cross':
                self.visit_cross_relation(rel.cross)
            case 'reference':
                self.visit_reference_relation(rel.reference)
            case 'write':
                self.visit_write_relation(rel.write)
            case 'ddl':
                self.visit_ddl_relation(rel.ddl)
            case 'hash_join':
                self.visit_hash_join_relation(rel.hash_join)
            case 'merge_join':
                self.visit_merge_join_relation(rel.merge_join)
            case 'nested_loop_join':
                self.visit_nested_loop_join_relation(rel.nested_loop_join)
            case 'window':
                self.visit_window_relation(rel.window)
            case 'exchange':
                self.visit_exchange_relation(rel.exchange)
            case 'expand':
                self.visit_expand_relation(rel.expand)
            case 'set':
                self.visit_set_relation(rel.set)
            case _:
                raise ValueError(f'Unexpected rel type: {rel.WhichOneof("rel_type")}')

    def visit_relation_root(self, rel: algebra_pb2.RelRoot) -> Any:
        """Visits a relation root."""
        return self.visit_relation(rel.input)

    def visit_extension_uri(self, _: extensions_pb2.SimpleExtensionURI) -> Any:
        """Visits an extension URI."""
        return None

    def visit_extension(self, _: extensions_pb2.SimpleExtensionDeclaration) -> Any:
        """Visits an extension."""
        return None

    def visit_plan_relation(self, relation: plan_pb2.PlanRel) -> Any:
        """Visits a plan relation."""
        match relation.WhichOneof('rel_type'):
            case 'rel':
                return self.visit_relation(relation.rel)
            case 'root':
                return self.visit_relation_root(relation.root)
            case _:
                raise ValueError(
                    f'Unexpected relation type: {relation.WhichOneof("rel_type")}')

    def visit_advanced_extension(self, _: extensions_pb2.AdvancedExtension) -> Any:
        """Visits an advanced extension."""
        return None

    def visit_expected_type_url(self, _: str) -> Any:
        """Visits an expected type URL."""
        return None

    def visit_plan(self, plan: plan_pb2.Plan) -> Any:
        """Visits the entire Substrait plan."""
        for uri in plan.extension_uris:
            self.visit_extension(uri)
        for extension in plan.extensions:
            self.visit_extension(extension)
        for relation in plan.relations:
            self.visit_plan_relation(relation)
        if plan.HasField('advanced_extensions'):
            self.visit_advanced_extension(plan.advanced_extensions)
        for url in plan.expected_type_urls:
            self.visit_expected_type_url(url)
