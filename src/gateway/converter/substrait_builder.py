# SPDX-License-Identifier: Apache-2.0
"""Convenience builder for constructing Substrait plans."""
import itertools
from typing import Any

from gateway.converter.spark_functions import ExtensionFunction
from substrait.gen.proto import algebra_pb2, type_pb2


def flatten(list_of_lists: list[list[Any]]) -> list[Any]:
    """Flatten a list of lists into a list."""
    return list(itertools.chain.from_iterable(list_of_lists))


# pylint: disable=E1101

def fetch_relation(input_relation: algebra_pb2.Rel, num_rows: int) -> algebra_pb2.Rel:
    """Construct a Substrait fetch plan node."""
    return algebra_pb2.Rel(fetch=algebra_pb2.FetchRel(input=input_relation, count=num_rows))


def project_relation(input_relation: algebra_pb2.Rel,
                     expressions: list[algebra_pb2.Expression]) -> algebra_pb2.Rel:
    """Construct a Substrait project plan node."""
    return algebra_pb2.Rel(
        project=algebra_pb2.ProjectRel(input=input_relation, expressions=expressions))


# pylint: disable=fixme
def aggregate_relation(input_relation: algebra_pb2.Rel,
                       measures: list[algebra_pb2.AggregateFunction]) -> algebra_pb2.Rel:
    """Construct a Substrait aggregate plan node."""
    aggregate = algebra_pb2.Rel(
        aggregate=algebra_pb2.AggregateRel(
            common=algebra_pb2.RelCommon(emit=algebra_pb2.RelCommon.Emit(
                output_mapping=range(len(measures)))),
            input=input_relation))
    # TODO -- Add support for groupings.
    for measure in measures:
        aggregate.aggregate.measures.append(
            algebra_pb2.AggregateRel.Measure(measure=measure))
    return aggregate


def join_relation(left: algebra_pb2.Rel, right: algebra_pb2.Rel) -> algebra_pb2.Rel:
    """Construct a Substrait join plan node."""
    return algebra_pb2.Rel(
        join=algebra_pb2.JoinRel(common=algebra_pb2.RelCommon(), left=left, right=right,
                                 expression=algebra_pb2.Expression(
                                     literal=algebra_pb2.Expression.Literal(boolean=True)),
                                 type=algebra_pb2.JoinRel.JoinType.JOIN_TYPE_INNER))


def concat(function_info: ExtensionFunction,
           expressions: list[algebra_pb2.Expression]) -> algebra_pb2.Expression:
    """Construct a Substrait concat expression."""
    return algebra_pb2.Expression(
        scalar_function=algebra_pb2.Expression.ScalarFunction(
            function_reference=function_info.anchor,
            output_type=function_info.output_type,
            arguments=[algebra_pb2.FunctionArgument(value=expression) for expression in expressions]
        ))


def strlen(function_info: ExtensionFunction,
           expression: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait concat expression."""
    return algebra_pb2.Expression(
        scalar_function=algebra_pb2.Expression.ScalarFunction(
            function_reference=function_info.anchor,
            output_type=function_info.output_type,
            arguments=[algebra_pb2.FunctionArgument(value=expression)]))


def cast_operation(expression: algebra_pb2.Expression,
                   output_type: type_pb2.Type) -> algebra_pb2.Expression:
    """Construct a Substrait cast expression."""
    return algebra_pb2.Expression(
        cast=algebra_pb2.Expression.Cast(
            input=expression, type=output_type,
            failure_behavior=algebra_pb2.Expression.Cast.FAILURE_BEHAVIOR_THROW_EXCEPTION))


def if_then_else_operation(if_expr: algebra_pb2.Expression, then_expr: algebra_pb2.Expression,
                           else_expr: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a simplistic Substrait if-then-else expression."""
    return algebra_pb2.Expression(
        if_then=algebra_pb2.Expression.IfThen(
            **{'ifs': [
                algebra_pb2.Expression.IfThen.IfClause(**{'if': if_expr, 'then': then_expr})],
                'else': else_expr})
    )


def field_reference(field_number: int) -> algebra_pb2.Expression:
    """Construct a Substrait field reference expression."""
    return algebra_pb2.Expression(
        selection=algebra_pb2.Expression.FieldReference(
            direct_reference=algebra_pb2.Expression.ReferenceSegment(
                struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(
                    field=field_number)),
            root_reference=algebra_pb2.Expression.FieldReference.RootReference()))


def max_agg_function(function_info: ExtensionFunction,
                     field_number: int) -> algebra_pb2.AggregateFunction:
    """Construct a Substrait max aggregate function."""
    # TODO -- Reorganize all functions to belong to a class which determines the info.
    return algebra_pb2.AggregateFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=field_reference(field_number))],
        phase=algebra_pb2.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT)


def string_concat_agg_function(function_info: ExtensionFunction,
                               field_number: int,
                               separator: str = '') -> algebra_pb2.AggregateFunction:
    """Construct a Substrait string concat aggregate function."""
    return algebra_pb2.AggregateFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=field_reference(field_number)),
                   algebra_pb2.FunctionArgument(value=string_literal(separator))],
        phase=algebra_pb2.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT)


def least_function(greater_function_info: ExtensionFunction, expr1: algebra_pb2.Expression,
                   expr2: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait min expression."""
    return if_then_else_operation(
        greater_function(greater_function_info, expr1, expr2),
        expr2,
        expr1
    )


def greatest_function(greater_function_info: ExtensionFunction, expr1: algebra_pb2.Expression,
                      expr2: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait max expression."""
    return if_then_else_operation(
        greater_function(greater_function_info, expr1, expr2),
        expr1,
        expr2
    )


def equal_function(function_info: ExtensionFunction,
                   expr1: algebra_pb2.Expression,
                   expr2: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait min expression."""
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=expr1),
                   algebra_pb2.FunctionArgument(value=expr2)]))


def greater_or_equal_function(function_info: ExtensionFunction,
                              expr1: algebra_pb2.Expression,
                              expr2: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait min expression."""
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=expr1),
                   algebra_pb2.FunctionArgument(value=expr2)]))


def greater_function(function_info: ExtensionFunction,
                     expr1: algebra_pb2.Expression,
                     expr2: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait min expression."""
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=expr1),
                   algebra_pb2.FunctionArgument(value=expr2)]))


def minus_function(function_info: ExtensionFunction,
                   expr1: algebra_pb2.Expression,
                   expr2: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait min expression."""
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=expr1),
                   algebra_pb2.FunctionArgument(value=expr2)]))


def repeat_function(function_info: ExtensionFunction,
                    string: str,
                    count: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait concat expression."""
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=string_literal(string)),
                   algebra_pb2.FunctionArgument(value=count)]))


def lpad_function(function_info: ExtensionFunction,
                  expression: algebra_pb2.Expression, count: algebra_pb2.Expression,
                  pad_string: str = ' ') -> algebra_pb2.Expression:
    """Construct a Substrait concat expression."""
    # TODO -- Avoid a cast if we don't need it.
    cast_type = string_type()
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[
            algebra_pb2.FunctionArgument(value=cast_operation(expression, cast_type)),
            algebra_pb2.FunctionArgument(value=cast_operation(count, integer_type())),
            algebra_pb2.FunctionArgument(
                value=cast_operation(string_literal(pad_string), cast_type))]))


def rpad_function(function_info: ExtensionFunction,
                  expression: algebra_pb2.Expression, count: algebra_pb2.Expression,
                  pad_string: str = ' ') -> algebra_pb2.Expression:
    """Construct a Substrait concat expression."""
    # TODO -- Avoid a cast if we don't need it.
    cast_type = string_type()
    return algebra_pb2.Expression(scalar_function=
    algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[
            algebra_pb2.FunctionArgument(value=cast_operation(expression, cast_type)),
            algebra_pb2.FunctionArgument(value=cast_operation(count, integer_type())),
            algebra_pb2.FunctionArgument(
                value=cast_operation(string_literal(pad_string), cast_type))]))


def regexp_strpos_function(function_info: ExtensionFunction,
                           input: algebra_pb2.Expression, pattern: algebra_pb2.Expression,
                           position: algebra_pb2.Expression,
                           occurrence: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Construct a Substrait regex substring expression."""
    return algebra_pb2.Expression(scalar_function=algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[
            algebra_pb2.FunctionArgument(value=input),
            algebra_pb2.FunctionArgument(value=pattern),
            algebra_pb2.FunctionArgument(value=occurrence),
            algebra_pb2.FunctionArgument(value=position)]))


def regexp_like_function(function_info: ExtensionFunction,
                  input: algebra_pb2.Expression,
                  pattern: algebra_pb2.Expression,
                  flags: str | None = None) -> algebra_pb2.Expression:
    """Construct a Substrait regex like expression."""
    result = algebra_pb2.Expression(scalar_function=algebra_pb2.Expression.ScalarFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[
            algebra_pb2.FunctionArgument(value=input),
            algebra_pb2.FunctionArgument(value=pattern)]))
    if flags is not None:
        result.scalar_function.arguments.append(
            algebra_pb2.FunctionArgument(value=string_literal(flags)))
    return result


def bool_literal(val: bool) -> algebra_pb2.Expression:
    """Construct a Substrait boolean literal expression."""
    return algebra_pb2.Expression(literal=algebra_pb2.Expression.Literal(boolean=val))


def bool_type(required: bool = True) -> type_pb2.Type:
    """Construct a Substrait boolean type."""
    if required:
        nullability = type_pb2.Type.Nullability.NULLABILITY_REQUIRED
    else:
        nullability = type_pb2.Type.Nullability.NULLABILITY_NULLABLE
    return type_pb2.Type(bool=type_pb2.Type.Boolean(nullability=nullability))


def string_literal(val: str) -> algebra_pb2.Expression:
    """Construct a Substrait string literal expression."""
    return algebra_pb2.Expression(literal=algebra_pb2.Expression.Literal(string=val))


def bigint_literal(val: int) -> algebra_pb2.Expression:
    """Construct a Substrait string literal expression."""
    return algebra_pb2.Expression(literal=algebra_pb2.Expression.Literal(i64=val))


def string_type(required: bool = True) -> type_pb2.Type:
    """Construct a Substrait string type."""
    if required:
        nullability = type_pb2.Type.Nullability.NULLABILITY_REQUIRED
    else:
        nullability = type_pb2.Type.Nullability.NULLABILITY_NULLABLE
    return type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))


def varchar_type(length: int = 1000, required: bool = True) -> type_pb2.Type:
    """Construct a Substrait varchar type."""
    if required:
        nullability = type_pb2.Type.Nullability.NULLABILITY_REQUIRED
    else:
        nullability = type_pb2.Type.Nullability.NULLABILITY_NULLABLE
    return type_pb2.Type(varchar=type_pb2.Type.VarChar(length=length, nullability=nullability))


def integer_type(required: bool = True) -> type_pb2.Type:
    """Construct a Substrait i32 type."""
    if required:
        nullability = type_pb2.Type.Nullability.NULLABILITY_REQUIRED
    else:
        nullability = type_pb2.Type.Nullability.NULLABILITY_NULLABLE
    return type_pb2.Type(i32=type_pb2.Type.I32(nullability=nullability))
