# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
import glob
import json
import operator
import pathlib
import re
from enum import Enum

import pyarrow as pa
import pyspark.sql.connect.proto.base_pb2 as spark_pb2
import pyspark.sql.connect.proto.expressions_pb2 as spark_exprs_pb2
import pyspark.sql.connect.proto.relations_pb2 as spark_relations_pb2
import pyspark.sql.connect.proto.types_pb2 as spark_types_pb2
from google.protobuf.internal.wire_format import INT64_MAX
from pyspark.sql.connect.proto import types_pb2
from substrait.gen.proto import algebra_pb2, plan_pb2, type_pb2
from substrait.gen.proto.extensions import extensions_pb2

from gateway.converter.conversion_options import ConversionOptions
from gateway.converter.spark_functions import ExtensionFunction, FunctionType, lookup_spark_function
from gateway.converter.substrait_builder import (
    add_function,
    aggregate_relation,
    bigint_literal,
    bool_literal,
    bool_type,
    cast_operation,
    concat,
    equal_function,
    fetch_relation,
    field_reference,
    flatten,
    greater_function,
    greater_or_equal_function,
    greatest_function,
    if_then_else_operation,
    integer_literal,
    is_null_function,
    join_relation,
    least_function,
    lpad_function,
    max_agg_function,
    minus_function,
    project_relation,
    regexp_like_function,
    regexp_strpos_function,
    repeat_function,
    string_concat_agg_function,
    string_literal,
    string_type,
    strlen,
)
from gateway.converter.symbol_table import SymbolTable


class ExpressionProcessingMode(Enum):
    """The mode of processing expressions."""

    # Processing of an expression outside of an aggregate relation.
    NORMAL = 0
    # Processing of a measure at depth 0.
    AGGR_TOP_LEVEL = 1
    # Processing of a measure =at depth > 0.  No aggregate function has yet been encountered.
    AGGR_NOT_TOP_LEVEL = 2
    # Processing of a measure after encountering an aggregate function.
    AGGR_UNDER_AGGREGATE = 3


# ruff: noqa: RUF005
class SparkSubstraitConverter:
    """Converts SparkConnect plans to Substrait plans."""

    def __init__(self, options: ConversionOptions):
        """Initialize the converter."""
        self._function_uris: dict[str, int] = {}
        self._functions: dict[str, ExtensionFunction] = {}
        self._current_plan_id: int | None = None  # The relation currently being processed.
        self._symbol_table = SymbolTable()
        self._conversion_options = options
        self._seen_generated_names = {}
        self._saved_extension_uris = {}
        self._saved_extensions = {}
        self._backend = None
        self._sql_backend = None

        # These are used when processing expressions inside aggregate relations.
        self._expression_processing_mode = ExpressionProcessingMode.NORMAL
        self._top_level_projects: list[algebra_pb2.Rel] = []
        self._next_aggregation_reference_id = None
        self._aggregations: list[algebra_pb2.AggregateFunction] = []
        self._next_under_aggregation_reference_id = 0
        self._under_aggregation_projects: list[algebra_pb2.Rel] = []
        self._aggregation_arguments_use_computation = False

    def set_backends(self, backend, sql_backend) -> None:
        """Save the backends being used to resolve tables and convert to SQL."""
        self._backend = backend
        self._sql_backend = sql_backend

    def lookup_function_by_name(self, name: str) -> ExtensionFunction:
        """Find the function reference for a given Spark function name."""
        if name in self._functions:
            return self._functions.get(name)
        func = lookup_spark_function(name, self._conversion_options)
        if not func:
            raise LookupError(
                f'Spark function named {name} does not have a known Substrait conversion.')
        func.anchor = len(self._functions) + 1
        self._functions[name] = func
        if not self._function_uris.get(func.uri):
            self._function_uris[func.uri] = len(self._function_uris) + 1
        return self._functions.get(name)

    def update_field_references(self, plan_id: int) -> None:
        """Use the field references using the specified portion of the plan."""
        source_symbol = self._symbol_table.get_symbol(plan_id)
        current_symbol = self._symbol_table.get_symbol(self._current_plan_id)
        original_output_fields = current_symbol.output_fields
        for symbol in source_symbol.output_fields:
            new_name = symbol
            while new_name in original_output_fields:
                new_name = new_name + '_dup'
            current_symbol.input_fields.append(new_name)
            current_symbol.output_fields.append(new_name)

    def find_field_by_name(self, field_name: str) -> int | None:
        """Look up the field name in the current set of field references."""
        current_symbol = self._symbol_table.get_symbol(self._current_plan_id)
        try:
            return current_symbol.input_fields.index(field_name)
        except ValueError:
            return None

    def convert_boolean_literal(
            self, boolean: bool) -> algebra_pb2.Expression.Literal:
        """Transform a boolean into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(boolean=boolean)

    def convert_short_literal(
            self, i: int) -> algebra_pb2.Expression.Literal:
        """Transform a short integer into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(i16=i)

    def convert_integer_literal(
            self, i: int) -> algebra_pb2.Expression.Literal:
        """Transform an integer into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(i32=i)

    def convert_float_literal(
            self, f: float) -> algebra_pb2.Expression.Literal:
        """Transform a float into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(fp32=f)

    def convert_double_literal(
            self, d: float) -> algebra_pb2.Expression.Literal:
        """Transform a double into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(fp64=d)

    def convert_string_literal(
            self, s: str) -> algebra_pb2.Expression.Literal:
        """Transform a string into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(string=s)

    def convert_literal_expression(
            self, literal: spark_exprs_pb2.Expression.Literal) -> algebra_pb2.Expression:
        """Convert a Spark literal into a Substrait literal."""
        match literal.WhichOneof('literal_type'):
            case 'null':
                # TODO -- Finish with the type implementation (or peek at sibling types).
                result = algebra_pb2.Expression.Literal(
                    null=type_pb2.Type(fp64=type_pb2.Type.FP64(
                        nullability=type_pb2.Type.NULLABILITY_NULLABLE)))
            case 'binary':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'boolean':
                result = self.convert_boolean_literal(literal.boolean)
            case 'byte':
                # TODO -- Determine the nullability of literals using a view of type information.
                result = algebra_pb2.Expression.Literal(null=type_pb2.Type(
                    i8=type_pb2.Type.I8(nullability=type_pb2.Type.NULLABILITY_NULLABLE)))
            case 'short':
                result = self.convert_short_literal(literal.short)
            case 'integer':
                result = self.convert_integer_literal(literal.integer)
            case 'long':
                result = algebra_pb2.Expression.Literal(null=type_pb2.Type(
                    i64=type_pb2.Type.I64(nullability=type_pb2.Type.NULLABILITY_NULLABLE)))
            case 'float':
                result = self.convert_float_literal(literal.float)
            case 'double':
                result = self.convert_double_literal(literal.double)
            case 'decimal':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'string':
                result = self.convert_string_literal(literal.string)
            case 'date':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'timestamp':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'timestamp_ntz':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'calendar_interval':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'year_month_interval':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'day_time_interval':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case 'array':
                raise NotImplementedError(
                    f'Literal type {literal.WhichOneof("literal_type")} not yet implemented.')
            case _:
                raise NotImplementedError(
                    f'Unexpected literal type: {literal.WhichOneof("literal_type")}')
        return algebra_pb2.Expression(literal=result)

    def convert_unresolved_attribute(
            self,
            attr: spark_exprs_pb2.Expression.UnresolvedAttribute) -> algebra_pb2.Expression:
        """Convert a Spark unresolved attribute into a Substrait field reference."""
        field_ref = self.find_field_by_name(attr.unparsed_identifier)
        if field_ref is None:
            raise ValueError(
                f'could not locate field named {attr.unparsed_identifier} in plan id '
                f'{self._current_plan_id}')

        return algebra_pb2.Expression(selection=algebra_pb2.Expression.FieldReference(
            direct_reference=algebra_pb2.Expression.ReferenceSegment(
                struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(
                    field=field_ref)),
            root_reference=algebra_pb2.Expression.FieldReference.RootReference()))

    def determine_type_of_expression(self, expr: algebra_pb2.Expression) -> type_pb2.Type:
        """Determine the type of the Substrait expression."""
        if expr.WhichOneof('rex_type') == 'literal':
            match expr.literal.WhichOneof('literal_type'):
                case 'boolean':
                    return bool_type()
                case 'i8':
                    return type_pb2.Type(i8=type_pb2.Type.I8())
                case 'i16':
                    return type_pb2.Type(i16=type_pb2.Type.I16())
                case 'i32':
                    return type_pb2.Type(i32=type_pb2.Type.I32())
                case 'i64':
                    return type_pb2.Type(i64=type_pb2.Type.I64())
                case 'float':
                    return type_pb2.Type(fp32=type_pb2.Type.FP32())
                case 'double':
                    return type_pb2.Type(fp64=type_pb2.Type.FP64())
                case 'string':
                    return type_pb2.Type(string=type_pb2.Type.String())
                case _:
                    raise NotImplementedError(
                        'Type determination not implemented for literal of type '
                        f'{expr.literal.WhichOneof("literal_type")}.')
        if expr.WhichOneof('rex_type') == 'scalar_function':
            return expr.scalar_function.output_type
        if expr.WhichOneof('rex_type') == 'selection':
            # TODO -- Figure out how to determine the type of a field reference.
            return type_pb2.Type(i64=type_pb2.Type.I64())
        raise NotImplementedError(
            'Type determination not implemented for expressions of type '
            f'{expr.WhichOneof("rex_type")}.')

    def convert_when_function(
            self,
            when: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark when function into a Substrait if-then expression."""
        ifthen = algebra_pb2.Expression.IfThen()
        for i in range(0, len(when.arguments) - 1, 2):
            clause = algebra_pb2.Expression.IfThen.IfClause()
            getattr(clause, 'if').CopyFrom(self.convert_expression(when.arguments[i]))
            clause.then.CopyFrom(self.convert_expression(when.arguments[i + 1]))
            ifthen.ifs.append(clause)
        if len(when.arguments) % 2 == 1:
            getattr(ifthen, 'else').CopyFrom(
                self.convert_expression(when.arguments[len(when.arguments) - 1]))
        else:
            nullable_literal = self.determine_type_of_expression(ifthen.ifs[-1].then)
            kind = nullable_literal.WhichOneof('kind')
            getattr(nullable_literal, kind).nullability = (
                type_pb2.Type.Nullability.NULLABILITY_NULLABLE)
            getattr(ifthen, 'else').CopyFrom(
                algebra_pb2.Expression(
                    literal=algebra_pb2.Expression.Literal(
                        null=nullable_literal)))

        return algebra_pb2.Expression(if_then=ifthen)

    def convert_in_function(
            self, in_: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark in function into a Substrait switch expression."""

        def is_switch_expression_appropriate() -> bool:
            """Determine if the IN function is appropriate for a switch expression."""
            if not self._conversion_options.use_switch_expressions_where_possible:
                return False
            return all(a.WhichOneof("expr_type") == "literal" for a in in_.arguments[1:])

        if is_switch_expression_appropriate():
            switch = algebra_pb2.Expression.SwitchExpression(
                match=self.convert_expression(in_.arguments[0]))

            for arg in in_.arguments[1:]:
                ifvalue = algebra_pb2.Expression.SwitchExpression.IfValue(then=bool_literal(True))
                expr = self.convert_literal_expression(arg.literal)
                getattr(ifvalue, 'if').CopyFrom(expr.literal)
                switch.ifs.append(ifvalue)

            getattr(switch, 'else').CopyFrom(bool_literal(False))

            return algebra_pb2.Expression(switch_expression=switch)

        equal_func = self.lookup_function_by_name('==')

        ifthen = algebra_pb2.Expression.IfThen()

        match = self.convert_expression(in_.arguments[0])
        for arg in in_.arguments[1:]:
            clause = algebra_pb2.Expression.IfThen.IfClause(then=bool_literal(True))
            getattr(clause, 'if').CopyFrom(
                equal_function(equal_func, match, self.convert_expression(arg)))
            ifthen.ifs.append(clause)

        getattr(ifthen, 'else').CopyFrom(bool_literal(False))

        return algebra_pb2.Expression(if_then=ifthen)

    def convert_rlike_function(
            self, in_: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark rlike function into a Substrait expression."""
        if self._conversion_options.use_duckdb_regexp_matches_function:
            regexp_matches_func = self.lookup_function_by_name('DUCKDB_regexp_matches')
            return algebra_pb2.Expression(
                scalar_function=algebra_pb2.Expression.ScalarFunction(
                    function_reference=regexp_matches_func.anchor,
                    arguments=[
                        algebra_pb2.FunctionArgument(
                            value=self.convert_expression(in_.arguments[0])),
                        algebra_pb2.FunctionArgument(
                            value=self.convert_expression(in_.arguments[1]))
                    ],
                    output_type=regexp_matches_func.output_type))

        if self._conversion_options.use_regexp_like_function:
            regexp_like_func = self.lookup_function_by_name('regexp_like')
            return regexp_like_function(regexp_like_func,
                                        self.convert_expression(in_.arguments[0]),
                                        self.convert_expression(in_.arguments[1]))

        regexp_strpos_func = self.lookup_function_by_name('regexp_strpos')
        greater_func = self.lookup_function_by_name('>')

        regexp_expr = regexp_strpos_function(regexp_strpos_func,
                                             self.convert_expression(in_.arguments[1]),
                                             self.convert_expression(in_.arguments[0]),
                                             bigint_literal(1), bigint_literal(1))
        return greater_function(greater_func, regexp_expr, bigint_literal(0))

    def convert_nanvl_function(
            self, in_: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark nanvl function into a Substrait expression."""
        isnan_func = self.lookup_function_by_name('isnan')
        arg0 = self.convert_expression(in_.arguments[0])
        arg1 = self.convert_expression(in_.arguments[1])
        expr = algebra_pb2.Expression(
            scalar_function=algebra_pb2.Expression.ScalarFunction(
                function_reference=isnan_func.anchor,
                arguments=[
                    algebra_pb2.FunctionArgument(value=arg0),
                ],
                output_type=isnan_func.output_type))

        return if_then_else_operation(expr, arg1, arg0)

    def convert_nvl_function(
            self, in_: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark nvl function into a Substrait expression."""
        isnull_func = self.lookup_function_by_name('isnull')
        arg0 = self.convert_expression(in_.arguments[0])
        arg1 = self.convert_expression(in_.arguments[1])
        expr = algebra_pb2.Expression(
            scalar_function=algebra_pb2.Expression.ScalarFunction(
                function_reference=isnull_func.anchor,
                arguments=[
                    algebra_pb2.FunctionArgument(value=arg0),
                ],
                output_type=isnull_func.output_type))

        return if_then_else_operation(expr, arg1, arg0)

    def convert_nvl2_function(
            self, in_: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark nvl2 function into a Substrait expression."""
        isnotnull_func = self.lookup_function_by_name('isnotnull')
        arg0 = self.convert_expression(in_.arguments[0])
        arg1 = self.convert_expression(in_.arguments[1])
        arg2 = self.convert_expression(in_.arguments[2])
        expr = algebra_pb2.Expression(
            scalar_function=algebra_pb2.Expression.ScalarFunction(
                function_reference=isnotnull_func.anchor,
                arguments=[
                    algebra_pb2.FunctionArgument(value=arg0),
                ],
                output_type=isnotnull_func.output_type))

        return if_then_else_operation(expr, arg1, arg2)

    def convert_ifnull_function(
            self, in_: spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Convert a Spark ifnull function into a Substrait expression."""
        isnan_func = self.lookup_function_by_name('isnull')
        arg0 = self.convert_expression(in_.arguments[0])
        arg1 = self.convert_expression(in_.arguments[1])
        expr = algebra_pb2.Expression(
            scalar_function=algebra_pb2.Expression.ScalarFunction(
                function_reference=isnan_func.anchor,
                arguments=[
                    algebra_pb2.FunctionArgument(value=arg0),
                ],
                output_type=isnan_func.output_type))

        return if_then_else_operation(expr, arg1, arg0)

    def convert_unresolved_function(
            self,
            unresolved_function: spark_exprs_pb2.Expression.UnresolvedFunction
    ) -> algebra_pb2.Expression | algebra_pb2.AggregateFunction:
        """Convert a Spark unresolved function into a Substrait scalar function."""
        parent_processing_mode = self._expression_processing_mode
        if parent_processing_mode == ExpressionProcessingMode.AGGR_TOP_LEVEL:
            self._expression_processing_mode = ExpressionProcessingMode.AGGR_NOT_TOP_LEVEL
        try:
            if unresolved_function.function_name == 'when':
                return self.convert_when_function(unresolved_function)
            if unresolved_function.function_name == 'in':
                return self.convert_in_function(unresolved_function)
            if unresolved_function.function_name in ['rlike', 'regexp', 'regexp_like']:
                return self.convert_rlike_function(unresolved_function)
            if unresolved_function.function_name == 'nanvl':
                return self.convert_nanvl_function(unresolved_function)
            if unresolved_function.function_name == 'nvl':
                return self.convert_nvl_function(unresolved_function)
            if unresolved_function.function_name == 'nvl2':
                return self.convert_nvl2_function(unresolved_function)
            if unresolved_function.function_name == 'ifnull':
                return self.convert_ifnull_function(unresolved_function)
            func = algebra_pb2.Expression.ScalarFunction()
            function_def = self.lookup_function_by_name(unresolved_function.function_name)
            if (parent_processing_mode == ExpressionProcessingMode.AGGR_NOT_TOP_LEVEL and
                    function_def.function_type == FunctionType.AGGREGATE):
                self._expression_processing_mode = ExpressionProcessingMode.AGGR_UNDER_AGGREGATE
            func.function_reference = function_def.anchor
            for idx, arg in enumerate(unresolved_function.arguments):
                if function_def.max_args is not None and idx >= function_def.max_args:
                    break
                if unresolved_function.function_name == 'count' and arg.WhichOneof(
                        'expr_type') == 'unresolved_star':
                    # Ignore all the rest of the arguments.
                    func.arguments.append(
                        algebra_pb2.FunctionArgument(value=bigint_literal(1)))
                    break
                func.arguments.append(
                    algebra_pb2.FunctionArgument(value=self.convert_expression(arg)))
            func.output_type.CopyFrom(function_def.output_type)
            if unresolved_function.function_name == 'substring':
                original_argument = func.arguments[0]
                func.arguments[0].CopyFrom(algebra_pb2.FunctionArgument(
                    value=cast_operation(original_argument.value, string_type())))
            if function_def.options:
                func.options.extend(function_def.options)
            match function_def.function_type:
                case FunctionType.SCALAR:
                    return algebra_pb2.Expression(scalar_function=func)
                case FunctionType.WINDOW:
                    return algebra_pb2.Expression(window_function=func)
                case FunctionType.AGGREGATE:
                    if self._expression_processing_mode == ExpressionProcessingMode.NORMAL:
                        raise ValueError(
                            f'Aggregate function {unresolved_function.function_name} used in a '
                            'non-aggregate context.')
                    aggr = algebra_pb2.AggregateFunction(
                        phase=algebra_pb2.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
                        function_reference=function_def.anchor,
                        output_type=function_def.output_type)
                    if unresolved_function.is_distinct:
                        aggr.invocation = (
                            algebra_pb2.AggregateFunction.AGGREGATION_INVOCATION_DISTINCT)
                    aggr.arguments.extend(func.arguments)
                    if function_def.options:
                        aggr.options.extend(function_def.options)
                    return aggr
        finally:
            self._expression_processing_mode = parent_processing_mode

    def convert_alias_expression(
            self, alias: spark_exprs_pb2.Expression.Alias) -> algebra_pb2.Expression:
        """Convert a Spark alias into a Substrait expression."""
        # We do nothing here and let the magic happen in the calling project relation.
        return self.convert_expression(alias.expr)

    def convert_type_str(self, spark_type_str: str | None) -> type_pb2.Type:
        """Convert a Spark type string into a Substrait type."""
        # TODO -- Properly handle nullability.
        match spark_type_str:
            case 'boolean':
                return type_pb2.Type(bool=type_pb2.Type.Boolean(
                    nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED
                ))
            case 'integer':
                return type_pb2.Type(i32=type_pb2.Type.I32(
                    nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED
                ))
            # TODO -- Add all of the other types.
            case _:
                raise NotImplementedError(
                    f'type {spark_type_str} not yet implemented.')

    def convert_type(self, spark_type: spark_types_pb2.DataType) -> type_pb2.Type:
        """Convert a Spark type into a Substrait type."""
        return self.convert_type_str(spark_type.WhichOneof('kind'))

    def convert_cast_expression(
            self, cast: spark_exprs_pb2.Expression.Cast) -> algebra_pb2.Expression:
        """Convert a Spark cast expression into a Substrait cast expression."""
        cast_rel = algebra_pb2.Expression.Cast(
            input=self.convert_expression(cast.expr),
            failure_behavior=algebra_pb2.Expression.Cast.FAILURE_BEHAVIOR_THROW_EXCEPTION)
        match cast.WhichOneof('cast_to_type'):
            case 'type':
                cast_rel.type.CopyFrom(self.convert_type(cast.type))
            case 'type_str':
                cast_rel.type.CopyFrom(self.convert_type_str(cast.type_str))
            case _:
                raise NotImplementedError(
                    f'unknown cast_to_type {cast.WhichOneof("cast_to_type")}'
                )
        return algebra_pb2.Expression(cast=cast_rel)

    def convert_extract_value(
            self,
            extract: spark_exprs_pb2.Expression.UnresolvedExtractValue) -> algebra_pb2.Expression:
        """Convert a Spark extract value expression into a Substrait extract value expression."""
        # TODO -- Add support for list and map operations.
        func = algebra_pb2.Expression.ScalarFunction()
        function_def = self.lookup_function_by_name('struct_extract')
        func.function_reference = function_def.anchor
        func.arguments.append(
            algebra_pb2.FunctionArgument(
                value=self.convert_unresolved_attribute(extract.child.unresolved_attribute)))
        func.arguments.append(
            algebra_pb2.FunctionArgument(
                value=string_literal(extract.extraction.literal.string)))
        return algebra_pb2.Expression(scalar_function=func)

    def convert_expression(self, expr: spark_exprs_pb2.Expression) -> algebra_pb2.Expression | None:
        """Convert a SparkConnect expression to a Substrait expression."""
        parent_processing_mode = self._expression_processing_mode
        try:
            match expr.WhichOneof('expr_type'):
                case 'literal':
                    result = self.convert_literal_expression(expr.literal)
                    if parent_processing_mode == ExpressionProcessingMode.AGGR_TOP_LEVEL:
                        self._top_level_projects.append(result)
                        return None
                case 'unresolved_attribute':
                    result = self.convert_unresolved_attribute(expr.unresolved_attribute)
                case 'unresolved_function':
                    result = self.convert_unresolved_function(expr.unresolved_function)
                    if isinstance(result, algebra_pb2.AggregateFunction):
                        match parent_processing_mode:
                            case ExpressionProcessingMode.AGGR_TOP_LEVEL:
                                self._aggregations.append(result)
                                self._top_level_projects.append(
                                    field_reference(self._next_aggregation_reference_id))
                                self._next_aggregation_reference_id += 1
                                return None
                            case ExpressionProcessingMode.AGGR_NOT_TOP_LEVEL:
                                self._aggregations.append(result)
                                result = field_reference(self._next_aggregation_reference_id)
                                self._next_aggregation_reference_id += 1
                            case _:
                                raise ValueError('Unexpected aggregate function nesting.')
                case 'expression_string':
                    raise NotImplementedError(
                        'SQL expressions through selectExpr is not supported')
                case 'unresolved_star':
                    raise NotImplementedError(
                        '* expressions are only supported within count aggregations')
                case 'alias':
                    result = self.convert_alias_expression(expr.alias)
                case 'cast':
                    result = self.convert_cast_expression(expr.cast)
                case 'unresolved_regex':
                    raise NotImplementedError(
                        'colRegex is only supported at the top level of an expression')
                case 'sort_order':
                    raise NotImplementedError(
                        'sort_order expression type not supported')
                case 'lambda_function':
                    raise NotImplementedError(
                        'lambda_function expression type not supported')
                case 'window':
                    raise NotImplementedError(
                        'window expression type not supported')
                case 'unresolved_extract_value':
                    result = self.convert_extract_value(expr.unresolved_extract_value)
                case 'update_fields':
                    raise NotImplementedError(
                        'update_fields expression type not supported')
                case 'unresolved_named_lambda_variable':
                    raise NotImplementedError(
                        'unresolved_named_lambda_variable expression type not supported')
                case 'common_inline_user_defined_function':
                    raise NotImplementedError(
                        'common_inline_user_defined_function expression type not supported')
                case _:
                    raise NotImplementedError(
                        f'Unexpected expression type: {expr.WhichOneof("expr_type")}')
        finally:
            self._expression_processing_mode = parent_processing_mode
        return result

    def get_number_of_names(self, substrait_type: type_pb2.Type) -> int:
        """Get the number of names consumed used in a Substrait type."""
        if substrait_type.WhichOneof('kind') == 'struct':
            return sum([self.get_number_of_names(t) for t in substrait_type.struct.types]) + 1
        return 1

    def get_primary_names(self, schema: type_pb2.NamedStruct) -> list[str]:
        """Get the primary names from a Substrait schema."""
        primary_names = []
        curr_type = 0
        while curr_type < len(schema.struct.types):
            primary_names.append(schema.names[curr_type])
            curr_type += self.get_number_of_names(schema.struct.types[curr_type])
        return primary_names

    def convert_read_named_table_relation(
            self,
            rel: spark_relations_pb2.Read.NamedTable
    ) -> algebra_pb2.Rel:
        """Convert a read named table relation to a Substrait relation."""
        table_name = rel.unparsed_identifier

        arrow_schema = self._backend.describe_table(table_name)

        schema = self.convert_arrow_schema(arrow_schema)

        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        if self._conversion_options.use_duckdb_struct_name_behavior:
            for field_name in self.get_primary_names(schema):
                symbol.output_fields.append(field_name)
        else:
            symbol.output_fields.extend(schema.names)

        return algebra_pb2.Rel(
            read=algebra_pb2.ReadRel(
                base_schema=schema,
                named_table=algebra_pb2.ReadRel.NamedTable(names=[table_name]),
                common=self.create_common_relation()))

    def convert_type_name(self, type_name: str) -> type_pb2.Type:
        """Convert a Spark type name into a Substrait type."""
        nullability = type_pb2.Type.NULLABILITY_REQUIRED
        match type_name:
            case 'boolean':
                return type_pb2.Type(bool=type_pb2.Type.Boolean(nullability=nullability))
            case 'byte':
                return type_pb2.Type(i8=type_pb2.Type.I8(nullability=nullability))
            case 'short':
                return type_pb2.Type(i16=type_pb2.Type.I16(nullability=nullability))
            case 'integer':
                return type_pb2.Type(i32=type_pb2.Type.I32(nullability=nullability))
            case 'long':
                return type_pb2.Type(i64=type_pb2.Type.I64(nullability=nullability))
            case 'float':
                return type_pb2.Type(fp32=type_pb2.Type.FP32(nullability=nullability))
            case 'double':
                return type_pb2.Type(fp64=type_pb2.Type.FP64(nullability=nullability))
            case 'decimal':
                return type_pb2.Type(decimal=type_pb2.Type.Decimal(nullability=nullability))
            case 'string':
                return type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))
            case 'binary':
                return type_pb2.Type(binary=type_pb2.Type.Binary(nullability=nullability))
            case _:
                raise NotImplementedError(f'Unexpected type name: {type_name}')

    def convert_field(self, field: types_pb2.DataType) -> (type_pb2.Type, list[str]):
        """Convert a Spark field into a Substrait field."""
        if field.get('nullable'):
            nullability = type_pb2.Type.NULLABILITY_NULLABLE
        else:
            nullability = type_pb2.Type.NULLABILITY_REQUIRED
        more_names = []
        match field.get('type'):
            case 'boolean':
                field_type = type_pb2.Type(bool=type_pb2.Type.Boolean(nullability=nullability))
            case 'byte':
                field_type = type_pb2.Type(i8=type_pb2.Type.I8(nullability=nullability))
            case 'short':
                field_type = type_pb2.Type(i16=type_pb2.Type.I16(nullability=nullability))
            case 'integer':
                field_type = type_pb2.Type(i32=type_pb2.Type.I32(nullability=nullability))
            case 'long':
                field_type = type_pb2.Type(i64=type_pb2.Type.I64(nullability=nullability))
            case 'float':
                field_type = type_pb2.Type(fp32=type_pb2.Type.FP32(nullability=nullability))
            case 'double':
                field_type = type_pb2.Type(fp64=type_pb2.Type.FP64(nullability=nullability))
            case 'decimal':
                field_type = type_pb2.Type(
                    decimal=type_pb2.Type.Decimal(nullability=nullability))
            case 'string':
                field_type = type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))
            case 'binary':
                field_type = type_pb2.Type(binary=type_pb2.Type.Binary(nullability=nullability))
            case _:
                ft = field.get('type')
                if ft == 'array':
                    field_type = type_pb2.Type(list=type_pb2.Type.List(
                        nullability=nullability,
                        type=self.convert_type_name(field.get('elementType'))))
                elif ft == 'map':
                    field_type = type_pb2.Type(map=type_pb2.Type.Map(
                        nullability=nullability,
                        key=self.convert_type_name(field.get('keyType')),
                        value=self.convert_type_name(field.get('valueType'))))
                elif ft.get('type') == 'struct':
                    field_type = type_pb2.Type(
                        struct=type_pb2.Type.Struct(nullability=nullability))
                    sub_type = self.convert_schema_dict(ft)
                    more_names.extend(sub_type.names)
                    field_type.struct.types.extend(sub_type.struct.types)
                else:
                    raise NotImplementedError(
                        f'Spark data type conversion not yet implemented: {ft.get("type")}')

        return field_type, more_names

    def convert_schema_dict(self, schema_data: dict) -> type_pb2.NamedStruct | None:
        """Convert the Spark JSON schema dict into a Substrait named type structure."""
        schema = type_pb2.NamedStruct()
        schema.struct.nullability = type_pb2.Type.NULLABILITY_REQUIRED
        for field in schema_data.get('fields'):
            schema.names.append(field.get('name'))
            field_type, more_names = self.convert_field(field)
            schema.names.extend(more_names)
            schema.struct.types.append(field_type)
        return schema

    def convert_schema(self, schema_str: str) -> type_pb2.NamedStruct | None:
        """Convert the Spark JSON schema string into a Substrait named type structure."""
        if not schema_str:
            return None
        # TODO -- Deal with potential denial of service due to malformed JSON.
        schema_data = json.loads(schema_str)

        return self.convert_schema_dict(schema_data)

    def convert_arrow_datatype(
            self, arrow_type: pa.DataType, nullable: bool = False) -> (
            type_pb2.Type, list[str]):
        """Convert an Arrow datatype into a Substrait type."""
        if nullable:
            nullability = type_pb2.Type.NULLABILITY_NULLABLE
        else:
            nullability = type_pb2.Type.NULLABILITY_REQUIRED

        more_names = []

        match str(arrow_type):
            case 'bool':
                field_type = type_pb2.Type(bool=type_pb2.Type.Boolean(nullability=nullability))
            case 'int8':
                field_type = type_pb2.Type(i8=type_pb2.Type.I8(nullability=nullability))
            case 'int16':
                field_type = type_pb2.Type(i16=type_pb2.Type.I16(nullability=nullability))
            case 'int32':
                field_type = type_pb2.Type(i32=type_pb2.Type.I32(nullability=nullability))
            case 'int64':
                field_type = type_pb2.Type(i64=type_pb2.Type.I64(nullability=nullability))
            case 'float':
                field_type = type_pb2.Type(fp32=type_pb2.Type.FP32(nullability=nullability))
            case 'double':
                field_type = type_pb2.Type(fp64=type_pb2.Type.FP64(nullability=nullability))
            case 'string':
                field_type = type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))
            case 'timestamp[us]':
                field_type = type_pb2.Type(
                    timestamp=type_pb2.Type.Timestamp(nullability=nullability))
            case 'date32[day]':
                field_type = type_pb2.Type(date=type_pb2.Type.Date(nullability=nullability))
            case _:
                if str(arrow_type).startswith('struct'):
                    field_type = type_pb2.Type(
                        struct=type_pb2.Type.Struct(nullability=nullability))
                    x: pa.StructType = arrow_type
                    for i in range(x.num_fields):
                        y = x.field(i)
                        sub_type = self.convert_arrow_schema(y.type.schema)
                        more_names.extend(y.name)
                        field_type.struct.types.extend(sub_type.struct.types)
                    return field_type
                raise NotImplementedError(f'Unexpected field type: {arrow_type}')

        return field_type, more_names

    def convert_arrow_schema(self, arrow_schema: pa.Schema) -> type_pb2.NamedStruct:
        """Convert an Arrow schema into a Substrait named type structure."""
        schema = type_pb2.NamedStruct()
        schema.struct.nullability = type_pb2.Type.NULLABILITY_REQUIRED

        for field_idx in range(len(arrow_schema)):
            field = arrow_schema[field_idx]
            schema.names.append(field.name)
            if field.nullable:
                nullability = type_pb2.Type.NULLABILITY_NULLABLE
            else:
                nullability = type_pb2.Type.NULLABILITY_REQUIRED

            match str(field.type):
                case 'bool':
                    field_type = type_pb2.Type(bool=type_pb2.Type.Boolean(nullability=nullability))
                case 'int8':
                    field_type = type_pb2.Type(i8=type_pb2.Type.I8(nullability=nullability))
                case 'int16':
                    field_type = type_pb2.Type(i16=type_pb2.Type.I16(nullability=nullability))
                case 'int32':
                    field_type = type_pb2.Type(i32=type_pb2.Type.I32(nullability=nullability))
                case 'int64':
                    field_type = type_pb2.Type(i64=type_pb2.Type.I64(nullability=nullability))
                case 'float':
                    field_type = type_pb2.Type(fp32=type_pb2.Type.FP32(nullability=nullability))
                case 'double':
                    field_type = type_pb2.Type(fp64=type_pb2.Type.FP64(nullability=nullability))
                case 'string':
                    field_type = type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))
                case 'timestamp[us]':
                    field_type = type_pb2.Type(
                        timestamp=type_pb2.Type.Timestamp(nullability=nullability))
                case 'date32[day]':
                    field_type = type_pb2.Type(date=type_pb2.Type.Date(nullability=nullability))
                case _:
                    if str(field.type).startswith('struct'):
                        field_type = type_pb2.Type(
                            struct=type_pb2.Type.Struct(nullability=nullability))
                        x: pa.StructType = field.type
                        for i in range(x.num_fields):
                            y = x.field(i)
                            schema.names.append(y.name)
                            sub_type, more_names = self.convert_arrow_datatype(y.type)
                            schema.names.extend(more_names)
                            field_type.struct.types.append(sub_type)
                    elif str(field.type).startswith('list'):
                        subtype, more_names = self.convert_arrow_datatype(field.type.value_type)
                        schema.names.extend(more_names)
                        field_type = type_pb2.Type(
                            list=type_pb2.Type.List(nullability=nullability, type=subtype))
                    elif str(field.type).startswith('map'):
                        key_type, more_names = self.convert_arrow_datatype(field.type.key_type)
                        schema.names.extend(more_names)
                        value_type, more_names = self.convert_arrow_datatype(field.type.item_type)
                        schema.names.extend(more_names)
                        field_type = type_pb2.Type(
                            map=type_pb2.Type.Map(nullability=nullability, key=key_type,
                                                  value=value_type))
                    else:
                        raise NotImplementedError(f'Unexpected field type: {field.type}')

            schema.struct.types.append(field_type)
        return schema

    def convert_read_data_source_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Convert a read data source relation into a Substrait relation."""
        local = algebra_pb2.ReadRel.LocalFiles()
        schema = self.convert_schema(rel.schema)
        paths = ([str(path) for path in rel.paths] if rel.paths
                 else list(rel.options.values()))
        if not schema:
            arrow_schema = self._backend.describe_files(paths)
            schema = self.convert_arrow_schema(arrow_schema)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_name in schema.names:
            symbol.output_fields.append(field_name)
        if self._conversion_options.use_named_table_workaround:
            return algebra_pb2.Rel(
                read=algebra_pb2.ReadRel(base_schema=schema,
                                         named_table=algebra_pb2.ReadRel.NamedTable(
                                             names=['demotable']),
                                         common=self.create_common_relation()))
        if pathlib.Path(paths[0]).is_dir():
            file_paths = glob.glob(f'{paths[0]}/*{rel.format}')
        else:
            file_paths = paths
        for path in file_paths:
            uri_path = path
            if self._conversion_options.needs_scheme_in_path_uris and uri_path.startswith('/'):
                uri_path = "file:" + uri_path
            file_or_files = algebra_pb2.ReadRel.LocalFiles.FileOrFiles(uri_file=uri_path)
            match rel.format:
                case 'parquet':
                    file_or_files.parquet.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions())
                case 'orc':
                    file_or_files.orc.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.OrcReadOptions())
                case 'text':
                    raise NotImplementedError('the only supported formats are parquet and orc')
                case 'json':
                    raise NotImplementedError('the only supported formats are parquet and orc')
                case 'csv':
                    # TODO -- Implement CSV once Substrait has support.
                    pass
                case 'avro':
                    raise NotImplementedError('the only supported formats are parquet and orc')
                case 'arrow':
                    file_or_files.parquet.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.ArrowReadOptions())
                case 'dwrf':
                    file_or_files.parquet.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions())
                case _:
                    raise NotImplementedError(f'Unexpected file format: {rel.format}')
            local.items.append(file_or_files)
        result = algebra_pb2.Rel(read=algebra_pb2.ReadRel(base_schema=schema, local_files=local,
                                                          common=self.create_common_relation()))
        if not self._conversion_options.safety_project_read_relations:
            return result

        project = algebra_pb2.ProjectRel(
            input=result,
            common=algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct()))
        for field_number in range(len(symbol.output_fields)):
            project.expressions.append(field_reference(field_number))
            project.common.emit.output_mapping.append(field_number)

        return algebra_pb2.Rel(project=project)

    def create_common_relation(self, emit_overrides=None) -> algebra_pb2.RelCommon:
        """Create the common metadata relation used by all relations."""
        if not self._conversion_options.use_emits_instead_of_direct:
            return algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct())
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        emit = algebra_pb2.RelCommon.Emit()
        if emit_overrides:
            for field_number in emit_overrides:
                emit.output_mapping.append(field_number)
        else:
            field_number = 0
            for _ in symbol.output_fields:
                emit.output_mapping.append(field_number)
                field_number += 1
        return algebra_pb2.RelCommon(emit=emit)

    def convert_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Convert a read relation into a Substrait relation."""
        match rel.WhichOneof('read_type'):
            case 'named_table':
                result = self.convert_read_named_table_relation(rel.named_table)
            case 'data_source':
                result = self.convert_read_data_source_relation(rel.data_source)
            case _:
                raise ValueError(f'Unexpected read type: {rel.WhichOneof("read_type")}')
        return result

    def convert_filter_relation(self, rel: spark_relations_pb2.Filter) -> algebra_pb2.Rel:
        """Convert a filter relation into a Substrait relation."""
        filter_rel = algebra_pb2.FilterRel(input=self.convert_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        filter_rel.common.CopyFrom(self.create_common_relation())
        filter_rel.condition.CopyFrom(self.convert_expression(rel.condition))
        return algebra_pb2.Rel(filter=filter_rel)

    def convert_sort_relation(self, rel: spark_relations_pb2.Sort) -> algebra_pb2.Rel:
        """Convert a sort relation into a Substrait relation."""
        sort = algebra_pb2.SortRel(input=self.convert_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        sort.common.CopyFrom(self.create_common_relation())
        for order in rel.order:
            if order.direction == spark_exprs_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING:
                if order.null_ordering == spark_exprs_pb2.Expression.SortOrder.SORT_NULLS_FIRST:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_FIRST
                else:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_LAST
            else:
                if order.null_ordering == spark_exprs_pb2.Expression.SortOrder.SORT_NULLS_FIRST:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_DESC_NULLS_FIRST
                else:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_DESC_NULLS_LAST
            sort.sorts.append(algebra_pb2.SortField(
                expr=self.convert_expression(order.child),
                direction=direction))
        return algebra_pb2.Rel(sort=sort)

    def convert_limit_relation(self, rel: spark_relations_pb2.Limit) -> algebra_pb2.Rel:
        """Convert a limit relation into a Substrait FetchRel relation."""
        input_relation = self.convert_relation(rel.input)
        self.update_field_references(rel.input.common.plan_id)
        fetch = algebra_pb2.FetchRel(common=self.create_common_relation(), input=input_relation,
                                     count=rel.limit)
        return algebra_pb2.Rel(fetch=fetch)

    def determine_expression_name(self, expr: spark_exprs_pb2.Expression) -> str | None:
        """Determine the name of the expression."""
        if expr.HasField('alias'):
            return expr.alias.name[0]
        self._seen_generated_names.setdefault('aggregate_expression', 0)
        self._seen_generated_names['aggregate_expression'] += 1
        return f'aggregate_expression{self._seen_generated_names["aggregate_expression"]}'

    def determine_name_for_grouping(self, expr: spark_exprs_pb2.Expression) -> str:
        """Determine the field name the grouping should use."""
        if expr.WhichOneof('expr_type') == 'unresolved_attribute':
            return expr.unresolved_attribute.unparsed_identifier
        return 'grouping'

    def convert_aggregate_relation(self, rel: spark_relations_pb2.Aggregate) -> algebra_pb2.Rel:
        """Convert an aggregate relation into a Substrait relation."""
        aggregate = algebra_pb2.AggregateRel(input=self.convert_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        aggregate.common.CopyFrom(self.create_common_relation())
        symbol = self._symbol_table.get_symbol(self._current_plan_id)

        # Start tracking the parts of the expressions we are interested in.
        self._top_level_projects = []
        self._next_aggregation_reference_id = len(rel.grouping_expressions)
        self._aggregations = []
        self._next_under_aggregation_reference_id = 0
        self._under_aggregation_projects = []

        # TODO -- Deal with mixed groupings and measures.
        grouping_expression_list = []
        for idx, grouping in enumerate(rel.grouping_expressions):
            grouping_expression_list.append(self.convert_expression(grouping))
            symbol.generated_fields.append(self.determine_name_for_grouping(grouping))
            self._top_level_projects.append(field_reference(idx))
        aggregate.groupings.append(
            algebra_pb2.AggregateRel.Grouping(
                grouping_expressions=grouping_expression_list))

        self._expression_processing_mode = ExpressionProcessingMode.AGGR_TOP_LEVEL

        for expr in rel.aggregate_expressions:
            result = self.convert_expression(expr)
            if result:
                self._top_level_projects.append(result)
            symbol.generated_fields.append(self.determine_expression_name(expr))
        symbol.output_fields.clear()
        symbol.output_fields.extend(symbol.generated_fields)
        if len(rel.grouping_expressions) > 1:
            # Hide the grouping source from the downstream relations.
            for i in range(len(rel.grouping_expressions) + len(rel.aggregate_expressions)):
                aggregate.common.emit.output_mapping.append(i)

        self._expression_processing_mode = ExpressionProcessingMode.NORMAL

        # Now put all the pieces together.
        if self._aggregation_arguments_use_computation:
            project = algebra_pb2.ProjectRel(
                input=aggregate.input,
                common=algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct()))
            for expr in self._under_aggregation_projects:
                project.expressions.append(expr)
            aggregate.input.CopyFrom(project)
        if self._aggregations:
            for aggr in self._aggregations:
                aggregate.measures.append(algebra_pb2.AggregateRel.Measure(measure=aggr))
        if self._top_level_projects:
            # TODO -- If all top level projects are field references, we don't need a project.
            top_project = algebra_pb2.ProjectRel(
                input=algebra_pb2.Rel(aggregate=aggregate),
                common=algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct()))
            for expr in self._top_level_projects:
                top_project.expressions.append(expr)
            for i in range(len(self._top_level_projects)):
                top_project.common.emit.output_mapping.append(i)
            return algebra_pb2.Rel(project=top_project)

        return algebra_pb2.Rel(aggregate=aggregate)

    # pylint: disable=too-many-locals,pointless-string-statement
    def convert_show_string_relation(self, rel: spark_relations_pb2.ShowString) -> algebra_pb2.Rel:
        """Convert a show string relation into a Substrait subplan."""
        if not self._conversion_options.implement_show_string:
            result = self.convert_relation(rel.input)
            self.update_field_references(rel.input.common.plan_id)
            return result

        if rel.vertical:
            raise NotImplementedError('vertical show strings are not yet implemented')

        if rel.truncate < 3:
            raise NotImplementedError(
                'show_string values of truncate of less than 3 not yet implemented')

        """
        The subplan implementing the show_string relation has this flow:

        Input -> Fetch1 -> Project1 -> Aggregate1 -> Project2 -> Project3
        Fetch1 + Aggregate1 -> Join1
        Join1 -> Project4 -> Aggregate2
        Project3 + Aggregate2 -> Join2
        Join2 -> Project5

        Input - The plan to run the show_string on.
        Fetch1 - Restricts the input to the number of rows (if needed).
        Project1 - Finds the length of each column of the remaining rows.
        Aggregate1 - Finds the maximum length of each column.
        Project2 - Uses the best of truncate, column name length, and max length.
        Project3 - Constructs the header and the footer based on the lines.
        Join1 - Combines the original rows with the maximum lengths.
        Project4 - Combines all of the columns for each row into a single string.
        Aggregate2 - Combines all the strings into the body of the result.
        Join2 - Combines the header and footer along with the body of the result.
        Project5 - Organizes the header, footer, and body in the right order.
        """

        # Find the functions we'll need.
        strlen_func = self.lookup_function_by_name('length')
        max_func = self.lookup_function_by_name('max')
        string_concat_func = self.lookup_function_by_name('string_agg')
        concat_func = self.lookup_function_by_name('concat')
        repeat_func = self.lookup_function_by_name('repeat')
        lpad_func = self.lookup_function_by_name('lpad')
        greater_func = self.lookup_function_by_name('>')
        minus_func = self.lookup_function_by_name('-')

        # Get the input and restrict it to the number of requested rows if necessary.
        input_rel = self.convert_relation(rel.input)
        if rel.num_rows > 0:
            input_rel = fetch_relation(input_rel, rel.num_rows)

        # Now that we've processed the input, do the bookkeeping.
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)

        # Find the length of each column in every row.
        project1 = project_relation(
            input_rel,
            [strlen(strlen_func, cast_operation(field_reference(column_number), string_type())) for
             column_number in range(len(symbol.input_fields))])

        # Find the maximum width of each column (for the rows in that we will display).
        aggregate1 = aggregate_relation(
            project1,
            measures=[
                max_agg_function(max_func, column_number)
                for column_number in range(len(symbol.input_fields))])

        # Find the maximum we will use based on the truncate, max size, and column name length.
        project2 = project_relation(
            aggregate1,
            [greatest_function(greater_func,
                               least_function(greater_func, field_reference(column_number),
                                              bigint_literal(rel.truncate)),
                               strlen(strlen_func,
                                      string_literal(symbol.input_fields[column_number]))) for
             column_number in range(len(symbol.input_fields))])

        def field_header_fragment(field_number: int) -> list[algebra_pb2.Expression]:
            return [string_literal('|'),
                    lpad_function(lpad_func, string_literal(symbol.input_fields[field_number]),
                                  field_reference(field_number))]

        def field_line_fragment(field_number: int) -> list[algebra_pb2.Expression]:
            return [string_literal('+'),
                    repeat_function(repeat_func, '-', field_reference(field_number))]

        def field_body_fragment(field_number: int) -> list[algebra_pb2.Expression]:
            return [string_literal('|'),
                    if_then_else_operation(
                        greater_function(greater_func,
                                         strlen(strlen_func,
                                                cast_operation(
                                                    field_reference(field_number),
                                                    string_type())),
                                         field_reference(
                                             field_number + len(symbol.input_fields))),
                        concat(concat_func,
                               [lpad_function(lpad_func, field_reference(field_number),
                                              minus_function(minus_func, field_reference(
                                                  field_number + len(symbol.input_fields)),
                                                             bigint_literal(3))),
                                string_literal('...')]),
                        lpad_function(lpad_func, field_reference(field_number),
                                      field_reference(
                                          field_number + len(symbol.input_fields))),

                    )]

        def header_line(fields: list[str]) -> list[algebra_pb2.Expression]:
            return [concat(concat_func,
                           flatten([
                               field_header_fragment(field_number) for field_number in
                               range(len(fields))
                           ]) + [
                               string_literal('|\n'),
                           ])]

        def full_line(fields: list[str]) -> list[algebra_pb2.Expression]:
            return [concat(concat_func,
                           flatten([
                               field_line_fragment(field_number) for field_number in
                               range(len(fields))
                           ]) + [
                               string_literal('+\n'),
                           ])]

        # Construct the header and footer lines.
        project3 = project_relation(project2, [
            concat(concat_func,
                   full_line(symbol.input_fields) +
                   header_line(symbol.input_fields) +
                   full_line(symbol.input_fields))] +
                                    full_line(symbol.input_fields))

        # Combine the original rows with the maximum lengths we are using.
        join1 = join_relation(input_rel, project2)

        # Construct the body of the result row by row.
        project4 = project_relation(join1, [
            concat(concat_func,
                   flatten([field_body_fragment(field_number) for field_number in
                            range(len(symbol.input_fields))
                            ]) + [
                       string_literal('|\n'),
                   ]
                   ),
        ])

        # Merge all of the rows of the result body into a single string.
        aggregate2 = aggregate_relation(project4, measures=[
            string_concat_agg_function(string_concat_func, 0)])

        # Create one row with the header, the body, and the footer in it.
        join2 = join_relation(project3, aggregate2)

        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        symbol.output_fields.clear()
        symbol.output_fields.append('show_string')

        def compute_row_count_footer(num_rows: int) -> str:
            if num_rows == 1:
                return 'only showing top 1 row\n'
            if num_rows < 20:
                return f'only showing top {num_rows} rows\n'
            return ''

        # Combine the header, body, and footer into the final result.
        project5 = project_relation(join2, [
            concat(concat_func, [
                field_reference(0),
                field_reference(2),
                field_reference(1),
            ] + [string_literal(
                compute_row_count_footer(rel.num_rows)) if rel.num_rows else None
                 ]),
        ])
        project5.project.common.emit.output_mapping.append(len(symbol.input_fields))
        return project5

    def convert_with_columns_relation(
            self, rel: spark_relations_pb2.WithColumns) -> algebra_pb2.Rel:
        """Convert a with columns relation into a Substrait project relation."""
        input_rel = self.convert_relation(rel.input)
        project = algebra_pb2.ProjectRel(input=input_rel)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        proposed_expressions = [field_reference(i) for i in range(len(symbol.input_fields))]
        for alias in rel.aliases:
            if len(alias.name) != 1:
                raise ValueError('Only one name part is supported in an alias.')
            name = alias.name[0]
            if name in symbol.input_fields:
                proposed_expressions[symbol.input_fields.index(name)] = self.convert_expression(
                    alias.expr)
            else:
                proposed_expressions.append(self.convert_expression(alias.expr))
                symbol.generated_fields.append(name)
                symbol.output_fields.append(name)
        project.common.CopyFrom(self.create_common_relation())
        project.expressions.extend(proposed_expressions)
        for i in range(len(proposed_expressions)):
            project.common.emit.output_mapping.append(len(symbol.input_fields) + i)
        return algebra_pb2.Rel(project=project)

    def convert_with_columns_renamed_relation(
            self, rel: spark_relations_pb2.WithColumnsRenamed) -> algebra_pb2.Rel:
        """Update the columns names based on the Spark with columns renamed relation."""
        input_rel = self.convert_relation(rel.input)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        self.update_field_references(rel.input.common.plan_id)
        symbol.output_fields.clear()
        if hasattr(rel, 'renames'):
            aliases = {r.col_name: r.new_col_name for r in rel.renames}
        else:
            aliases = rel.rename_columns_map
        for field_name in symbol.input_fields:
            if field_name in aliases:
                symbol.output_fields.append(aliases[field_name])
            else:
                symbol.output_fields.append(field_name)
        return input_rel

    def convert_drop_relation(self, rel: spark_relations_pb2.Drop) -> algebra_pb2.Rel:
        """Convert a drop relation into a Substrait project relation."""
        input_rel = self.convert_relation(rel.input)
        project = algebra_pb2.ProjectRel(input=input_rel)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        if rel.columns:
            column_names = [c.unresolved_attribute.unparsed_identifier for c in rel.columns]
        else:
            column_names = rel.column_names
        symbol.output_fields.clear()
        for field_number, field_name in enumerate(symbol.input_fields):
            if field_name not in column_names:
                symbol.output_fields.append(field_name)
                if self._conversion_options.drop_emit_workaround:
                    project.common.emit.output_mapping.append(len(project.expressions))
                    project.expressions.append(field_reference(field_number))
                else:
                    project.expressions.append(field_reference(field_number))
        if not project.expressions:
            raise ValueError(f"No columns remaining after drop in plan id {self._current_plan_id}")
        return algebra_pb2.Rel(project=project)

    def convert_to_df_relation(self, rel: spark_relations_pb2.ToDF) -> algebra_pb2.Rel:
        """Convert a to dataframe relation into a Substrait project relation."""
        input_rel = self.convert_relation(rel.input)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        if len(rel.column_names) != len(symbol.input_fields):
            raise ValueError('column_names does not match the number of input fields at '
                             f'plan id {self._current_plan_id}')
        symbol.output_fields.clear()
        for field_name in rel.column_names:
            symbol.output_fields.append(field_name)
        return input_rel

    def convert_arrow_to_literal(self, val: pa.Scalar) -> algebra_pb2.Expression.Literal:
        """Convert an Arrow scalar into a Substrait literal."""
        literal = algebra_pb2.Expression.Literal()
        if val.as_py() is None:
            if isinstance(val, pa.BooleanScalar):
                literal.null.binary.nullability = type_pb2.Type.NULLABILITY_NULLABLE
            elif isinstance(val, pa.Int8Scalar):
                literal.null.i8.nullability = type_pb2.Type.NULLABILITY_NULLABLE
            elif isinstance(val, pa.Int16Scalar):
                literal.null.i16.nullability = type_pb2.Type.NULLABILITY_NULLABLE
            elif isinstance(val, pa.Int32Scalar):
                literal.null.i32.nullability = type_pb2.Type.NULLABILITY_NULLABLE
            elif isinstance(val, pa.Int64Scalar):
                literal.null.i64.nullability = type_pb2.Type.NULLABILITY_NULLABLE
            elif isinstance(val, pa.StringScalar):
                literal.null.string.nullability = type_pb2.Type.NULLABILITY_NULLABLE
            else:
                raise NotImplementedError(
                    f'Null conversion for arrow type {val.type} not yet implemented.')
            return literal
        if isinstance(val, pa.BooleanScalar):
            literal.boolean = val.as_py()
        elif isinstance(val, pa.Int8Scalar):
            literal.i8 = val.as_py()
        elif isinstance(val, pa.Int16Scalar):
            literal.i16 = val.as_py()
        elif isinstance(val, pa.Int32Scalar):
            literal.i32 = val.as_py()
        elif isinstance(val, pa.Int64Scalar):
            literal.i64 = val.as_py()
        elif isinstance(val, pa.StringScalar):
            literal.string = val.as_py()
        elif isinstance(val, pa.StructScalar):
            for key in val:
                literal.struct.fields.append(self.convert_arrow_to_literal(val[key]))
        elif isinstance(val, pa.ListScalar):
            for item in val.values:
                literal.list.values.append(self.convert_arrow_to_literal(item))
        else:
            raise NotImplementedError(
                f'Conversion from arrow type {val.type} not yet implemented.')
        literal.nullable = True
        return literal

    def convert_arrow_data_to_virtual_table(self,
                                            data: bytes) -> algebra_pb2.ReadRel.VirtualTable:
        """Convert a Spark local relation into a virtual table."""
        table = algebra_pb2.ReadRel.VirtualTable()
        if not data:
            return table
        # Use pyarrow to convert the bytes into an arrow structure.
        with pa.ipc.open_stream(data) as arrow:
            for batch in arrow.iter_batches_with_custom_metadata():
                for row_number in range(batch.batch.num_rows):
                    values = algebra_pb2.Expression.Literal.Struct()
                    for item in batch.batch.columns:
                        values.fields.append(self.convert_arrow_to_literal(item[row_number]))
                    table.values.append(values)
        return table

    def convert_local_relation(self, rel: spark_relations_pb2.LocalRelation) -> algebra_pb2.Rel:
        """Convert a Spark local relation into a virtual table."""
        read = algebra_pb2.ReadRel(
            virtual_table=self.convert_arrow_data_to_virtual_table(rel.data))
        schema = self.convert_schema(rel.schema)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_name in schema.names:
            symbol.output_fields.append(field_name)
        read.base_schema.CopyFrom(schema)
        read.common.CopyFrom(self.create_common_relation())
        return algebra_pb2.Rel(read=read)

    def convert_sql_relation(self, rel: spark_relations_pb2.SQL) -> algebra_pb2.Rel:
        """Convert a Spark SQL relation into a Substrait relation."""
        # TODO -- Handle multithreading in the case with a persistent backend.
        plan = self._sql_backend.convert_sql(rel.query)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_name in plan.relations[0].root.names:
            symbol.output_fields.append(field_name)
        # TODO -- Correctly capture all the used functions and extensions.
        self._saved_extension_uris = plan.extension_uris
        self._saved_extensions = plan.extensions
        # TODO -- Merge those references into the current context.
        # TODO -- Renumber all of the functions/extensions in the captured subplan.
        return plan.relations[0].root.input

    def convert_spark_join_type(
            self, join_type: spark_relations_pb2.Join.JoinType) -> algebra_pb2.JoinRel.JoinType:
        """Convert a Spark join type into a Substrait join type."""
        match join_type:
            case spark_relations_pb2.Join.JOIN_TYPE_UNSPECIFIED:
                return algebra_pb2.JoinRel.JOIN_TYPE_UNSPECIFIED
            case spark_relations_pb2.Join.JOIN_TYPE_INNER:
                return algebra_pb2.JoinRel.JOIN_TYPE_INNER
            case spark_relations_pb2.Join.JOIN_TYPE_FULL_OUTER:
                return algebra_pb2.JoinRel.JOIN_TYPE_OUTER
            case spark_relations_pb2.Join.JOIN_TYPE_LEFT_OUTER:
                return algebra_pb2.JoinRel.JOIN_TYPE_LEFT
            case spark_relations_pb2.Join.JOIN_TYPE_RIGHT_OUTER:
                return algebra_pb2.JoinRel.JOIN_TYPE_RIGHT
            case spark_relations_pb2.Join.JOIN_TYPE_LEFT_ANTI:
                return algebra_pb2.JoinRel.JOIN_TYPE_ANTI
            case spark_relations_pb2.Join.JOIN_TYPE_LEFT_SEMI:
                return algebra_pb2.JoinRel.JOIN_TYPE_SEMI
            case spark_relations_pb2.Join.CROSS:
                raise RuntimeError('Internal error:  cross joins should be handled elsewhere')
            case _:
                raise ValueError(f'Unexpected join type: {join_type}')

    def convert_cross_join_relation(self, rel: spark_relations_pb2.Join) -> algebra_pb2.Rel:
        """Convert a Spark join relation into a Substrait join relation."""
        join = algebra_pb2.CrossRel(left=self.convert_relation(rel.left),
                                    right=self.convert_relation(rel.right))
        self.update_field_references(rel.left.common.plan_id)
        self.update_field_references(rel.right.common.plan_id)
        if rel.HasField('join_condition'):
            raise ValueError('Cross joins do not support having a join condition.')
        join.common.CopyFrom(self.create_common_relation())
        return algebra_pb2.Rel(cross=join)

    def convert_join_relation(self, rel: spark_relations_pb2.Join) -> algebra_pb2.Rel:
        """Convert a Spark join relation into a Substrait join relation."""
        if rel.join_type == spark_relations_pb2.Join.JOIN_TYPE_CROSS:
            return self.convert_cross_join_relation(rel)
        if not rel.HasField('join_condition') and not rel.using_columns:
            return self.convert_cross_join_relation(rel)
        join = algebra_pb2.JoinRel(left=self.convert_relation(rel.left),
                                   right=self.convert_relation(rel.right))
        self.update_field_references(rel.left.common.plan_id)
        self.update_field_references(rel.right.common.plan_id)
        if rel.HasField('join_condition'):
            join.expression.CopyFrom(self.convert_expression(rel.join_condition))
        join.type = self.convert_spark_join_type(rel.join_type)
        join.common.CopyFrom(self.create_common_relation())
        return algebra_pb2.Rel(join=join)

    def convert_project_relation(
            self, rel: spark_relations_pb2.Project) -> algebra_pb2.Rel:
        """Convert a Spark project relation into a Substrait project relation."""
        input_rel = self.convert_relation(rel.input)
        project = algebra_pb2.ProjectRel(input=input_rel)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_number, expr in enumerate(rel.expressions):
            if expr.WhichOneof('expr_type') == 'unresolved_regex':
                regex = expr.unresolved_regex.col_name.replace('`', '')
                matcher = re.compile(regex)
                found = False
                for column in symbol.input_fields:
                    if matcher.match(column):
                        project.expressions.append(
                            field_reference(symbol.input_fields.index(column)))
                        symbol.generated_fields.append(column)
                        symbol.output_fields.append(column)
                        found = True
                if not found:
                    raise ValueError(
                        f'No columns match the regex {regex} in plan id {self._current_plan_id}')
                continue
            project.expressions.append(self.convert_expression(expr))
            if expr.WhichOneof('expr_type') == 'alias':
                name = expr.alias.name[0]
            elif expr.WhichOneof('expr_type') == 'unresolved_attribute':
                name = expr.unresolved_attribute.unparsed_identifier
            else:
                name = f'generated_field_{field_number}'
            symbol.generated_fields.append(name)
            symbol.output_fields.append(name)
        project.common.CopyFrom(self.create_common_relation())
        symbol.output_fields = symbol.generated_fields
        for field_number in range(len(symbol.output_fields)):
            project.common.emit.output_mapping.append(field_number + len(symbol.input_fields))
        return algebra_pb2.Rel(project=project)

    def convert_subquery_alias_relation(self,
                                        rel: spark_relations_pb2.SubqueryAlias) -> algebra_pb2.Rel:
        """Convert a Spark subquery alias relation into a Substrait relation."""
        raise NotImplementedError('Subquery alias relations are not yet implemented')

    def convert_deduplicate_relation(self, rel: spark_relations_pb2.Deduplicate) -> algebra_pb2.Rel:
        """Convert a Spark deduplicate relation into a Substrait aggregation."""
        if self._conversion_options.use_first_value_as_any_value:
            any_value_func = self.lookup_function_by_name('first_value')
        else:
            any_value_func = self.lookup_function_by_name('any_value')

        aggregate = algebra_pb2.AggregateRel(input=self.convert_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        aggregate.common.CopyFrom(self.create_common_relation())
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        grouping = aggregate.groupings.add()
        for idx, field in enumerate(symbol.input_fields):
            grouping.grouping_expressions.append(field_reference(idx))
            aggregate.measures.append(
                algebra_pb2.AggregateRel.Measure(
                    measure=algebra_pb2.AggregateFunction(
                        function_reference=any_value_func.anchor,
                        arguments=[algebra_pb2.FunctionArgument(value=field_reference(idx))],
                        phase=algebra_pb2.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
                        output_type=type_pb2.Type(bool=type_pb2.Type.Boolean(
                            nullability=type_pb2.Type.NULLABILITY_REQUIRED)))))
            symbol.generated_fields.append(field)
        aggr = algebra_pb2.Rel(aggregate=aggregate)
        project = project_relation(
            aggr, [field_reference(idx) for idx in range(len(symbol.input_fields))])
        for idx in range(len(symbol.input_fields)):
            project.project.common.emit.output_mapping.append(idx)
        return project

    def convert_set_operation_relation(
            self, rel: spark_relations_pb2.SetOperation) -> algebra_pb2.Rel:
        """Convert a Spark set operation relation into a Substrait set operation relation."""
        left = self.convert_relation(rel.left_input)
        right = self.convert_relation(rel.right_input)
        set_operation = algebra_pb2.SetRel(inputs=[left, right])
        self.update_field_references(rel.left_input.common.plan_id)
        set_operation.common.CopyFrom(self.create_common_relation())

        if rel.by_name:
            # TODO -- Support by_name and allow_missing_columns.
            raise NotImplementedError('Substrait cannot represent set operations by name')
        match rel.set_op_type:
            case spark_relations_pb2.SetOperation.SetOpType.SET_OP_TYPE_UNION:
                if rel.is_all:
                    operation = algebra_pb2.SetRel.SET_OP_UNION_ALL
                else:
                    operation = algebra_pb2.SetRel.SET_OP_UNION_DISTINCT
            case spark_relations_pb2.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT:
                if rel.is_all:
                    raise NotImplementedError('Substrait cannot represent INTERSECT ALL')
                operation = algebra_pb2.SetRel.SET_OP_INTERSECTION_PRIMARY
            case spark_relations_pb2.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT:
                if rel.is_all:
                    raise NotImplementedError('Substrait cannot represent MINUS ALL')
                operation = algebra_pb2.SetRel.SET_OP_MINUS_PRIMARY
            case _:
                raise ValueError(f'Unexpected set operation type: {rel.set_op_type}')

        set_operation.op = operation
        return algebra_pb2.Rel(set=set_operation)

    def convert_offset_relation(self, rel: spark_relations_pb2.Offset) -> algebra_pb2.Rel:
        """Convert a Spark offset relation into a Substrait fetch relation."""
        input_rel = self.convert_relation(rel.input)
        rest_of_input = INT64_MAX if self._conversion_options.fetch_return_all_workaround else -1
        fetch = algebra_pb2.FetchRel(input=input_rel, offset=rel.offset,
                                     count=rest_of_input)
        self.update_field_references(rel.input.common.plan_id)
        fetch.common.CopyFrom(self.create_common_relation())
        return algebra_pb2.Rel(fetch=fetch)

    def convert_tail_relation(self, rel: spark_relations_pb2.Tail) -> algebra_pb2.Rel:
        """Convert a Spark tail relation into a set of Substrait relation."""
        # TODO -- Consider finding a nearby sort relation and rewriting the order.
        raise NotImplementedError(
            'Substrait does not represent tail relations.  Use sort and head relations instead.')

    def convert_dropna_relation(self, rel: spark_relations_pb2.NADrop) -> algebra_pb2.Rel:
        """Convert a Spark NADrop relation into a Substrait filter relation."""
        filter_rel = algebra_pb2.FilterRel(input=self.convert_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        filter_rel.common.CopyFrom(self.create_common_relation())
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        if rel.cols:
            cols = [symbol.input_fields.index(col) for col in rel.cols]
        else:
            cols = range(len(symbol.input_fields))
        min_non_nulls = rel.min_non_nulls if rel.min_non_nulls else len(cols)
        is_null_func = self.lookup_function_by_name('isnull')
        add_func = self.lookup_function_by_name('+')
        ge_func = self.lookup_function_by_name('>=')
        condition = algebra_pb2.Expression(literal=self.convert_boolean_literal(True))
        for col_count, field_number in enumerate(cols):
            new_condition = if_then_else_operation(
                is_null_function(is_null_func, field_reference(field_number)),
                integer_literal(1), integer_literal(0))
            if col_count == 0:
                condition = new_condition
            else:
                condition = add_function(add_func, condition, new_condition)
        filter_rel.condition.CopyFrom(
            greater_or_equal_function(ge_func, condition, integer_literal(min_non_nulls)))
        return algebra_pb2.Rel(filter=filter_rel)

    def convert_hint_relation(self, rel: spark_relations_pb2.Hint) -> algebra_pb2.Rel:
        """Convert a Spark hint relation into a Substrait relation."""
        result = self.convert_relation(rel.input)
        self.update_field_references(rel.input.common.plan_id)
        # There are no well-defined hints yet in Substrait so just ignore them for now.
        return result

    def convert_relation(self, rel: spark_relations_pb2.Relation) -> algebra_pb2.Rel:
        """Convert a Spark relation into a Substrait one."""
        self._symbol_table.add_symbol(rel.common.plan_id, parent=self._current_plan_id,
                                      symbol_type=rel.WhichOneof('rel_type'))
        old_plan_id = self._current_plan_id
        self._current_plan_id = rel.common.plan_id
        match rel.WhichOneof('rel_type'):
            case 'read':
                result = self.convert_read_relation(rel.read)
            case 'filter':
                result = self.convert_filter_relation(rel.filter)
            case 'sort':
                result = self.convert_sort_relation(rel.sort)
            case 'limit':
                result = self.convert_limit_relation(rel.limit)
            case 'aggregate':
                result = self.convert_aggregate_relation(rel.aggregate)
            case 'show_string':
                result = self.convert_show_string_relation(rel.show_string)
            case 'with_columns':
                result = self.convert_with_columns_relation(rel.with_columns)
            case 'with_columns_renamed':
                result = self.convert_with_columns_renamed_relation(rel.with_columns_renamed)
            case 'drop':
                result = self.convert_drop_relation(rel.drop)
            case 'to_df':
                result = self.convert_to_df_relation(rel.to_df)
            case 'local_relation':
                result = self.convert_local_relation(rel.local_relation)
            case 'sql':
                result = self.convert_sql_relation(rel.sql)
            case 'join':
                result = self.convert_join_relation(rel.join)
            case 'project':
                result = self.convert_project_relation(rel.project)
            case 'subquery_alias':
                result = self.convert_subquery_alias_relation(rel.subquery_alias)
            case 'deduplicate':
                result = self.convert_deduplicate_relation(rel.deduplicate)
            case 'set_op':
                result = self.convert_set_operation_relation(rel.set_op)
            case 'offset':
                result = self.convert_offset_relation(rel.offset)
            case 'tail':
                result = self.convert_tail_relation(rel.tail)
            case 'drop_na':
                result = self.convert_dropna_relation(rel.drop_na)
            case 'hint':
                result = self.convert_hint_relation(rel.hint)
            case _:
                raise ValueError(
                    f'Unexpected Spark plan rel_type: {rel.WhichOneof("rel_type")}')
        self._current_plan_id = old_plan_id
        return result

    def convert_plan(self, plan: spark_pb2.Plan) -> plan_pb2.Plan:
        """Convert a Spark plan into a Substrait plan."""
        result = plan_pb2.Plan()
        result.version.CopyFrom(
            plan_pb2.Version(minor_number=52, producer='spark-substrait-gateway'))
        if plan.HasField('root'):
            rel_root = algebra_pb2.RelRoot(input=self.convert_relation(plan.root))
            symbol = self._symbol_table.get_symbol(plan.root.common.plan_id)
            for name in symbol.output_fields:
                rel_root.names.append(name)
            result.relations.append(plan_pb2.PlanRel(root=rel_root))
        for uri in sorted(self._function_uris.items(), key=operator.itemgetter(1)):
            result.extension_uris.append(
                extensions_pb2.SimpleExtensionURI(extension_uri_anchor=uri[1],
                                                  uri=uri[0]))
        for f in sorted(self._functions.values()):
            result.extensions.append(extensions_pb2.SimpleExtensionDeclaration(
                extension_function=extensions_pb2.SimpleExtensionDeclaration.ExtensionFunction(
                    extension_uri_reference=self._function_uris.get(f.uri),
                    function_anchor=f.anchor, name=f.name)))
        # As a workaround use the saved extensions and URIs without fixing them.
        result.extension_uris.extend(self._saved_extension_uris)
        result.extensions.extend(self._saved_extensions)
        return result
