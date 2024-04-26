# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
import glob
import json
import operator
import pathlib

import pyarrow as pa
import pyspark.sql.connect.proto.base_pb2 as spark_pb2
import pyspark.sql.connect.proto.expressions_pb2 as spark_exprs_pb2
import pyspark.sql.connect.proto.relations_pb2 as spark_relations_pb2
import pyspark.sql.connect.proto.types_pb2 as spark_types_pb2
from gateway.backends.backend_options import BackendOptions
from gateway.backends.backend_selector import find_backend
from gateway.converter.conversion_options import ConversionOptions
from gateway.converter.spark_functions import ExtensionFunction, lookup_spark_function
from gateway.converter.sql_to_substrait import convert_sql
from gateway.converter.substrait_builder import (
    aggregate_relation,
    bigint_literal,
    bool_literal,
    cast_operation,
    concat,
    equal_function,
    fetch_relation,
    field_reference,
    flatten,
    greater_function,
    greatest_function,
    if_then_else_operation,
    join_relation,
    least_function,
    lpad_function,
    max_agg_function,
    minus_function,
    project_relation,
    regexp_strpos_function,
    repeat_function,
    string_concat_agg_function,
    string_literal,
    string_type,
    strlen,
)
from gateway.converter.symbol_table import SymbolTable
from substrait.gen.proto import algebra_pb2, plan_pb2, type_pb2
from substrait.gen.proto.extensions import extensions_pb2

TABLE_NAME = "my_table"


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
        current_symbol.input_fields.extend(source_symbol.output_fields)
        current_symbol.output_fields.extend(current_symbol.input_fields)

    def find_field_by_name(self, field_name: str) -> int | None:
        """Look up the field name in the current set of field references."""
        current_symbol = self._symbol_table.get_symbol(self._current_plan_id)
        try:
            return current_symbol.output_fields.index(field_name)
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
                # TODO -- Finish with the type implementation.
                result = algebra_pb2.Expression.Literal()
            case 'binary':
                result = algebra_pb2.Expression.Literal()
            case 'boolean':
                result = self.convert_boolean_literal(literal.boolean)
            case 'byte':
                result = algebra_pb2.Expression.Literal()
            case 'short':
                result = self.convert_short_literal(literal.short)
            case 'integer':
                result = self.convert_integer_literal(literal.integer)
            case 'long':
                result = algebra_pb2.Expression.Literal()
            case 'float':
                result = self.convert_float_literal(literal.float)
            case 'double':
                result = self.convert_double_literal(literal.double)
            case 'decimal':
                result = algebra_pb2.Expression.Literal()
            case 'string':
                result = self.convert_string_literal(literal.string)
            case 'date':
                result = algebra_pb2.Expression.Literal()
            case 'timestamp':
                result = algebra_pb2.Expression.Literal()
            case 'timestamp_ntz':
                result = algebra_pb2.Expression.Literal()
            case 'calendar_interval':
                result = algebra_pb2.Expression.Literal()
            case 'year_month_interval':
                result = algebra_pb2.Expression.Literal()
            case 'day_time_interval':
                result = algebra_pb2.Expression.Literal()
            case 'array':
                result = algebra_pb2.Expression.Literal()
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
        """Determine the type of a Substrait expression."""
        if expr.WhichOneof('rex_type') == 'literal':
            match expr.literal.WhichOneof('literal_type'):
                case 'boolean':
                    return type_pb2.Type(bool=type_pb2.Type.Boolean())
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
            return type_pb2.Type(i32=type_pb2.Type.I32())
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
            getattr(ifthen, 'else').CopyFrom(
                algebra_pb2.Expression(
                    literal=algebra_pb2.Expression.Literal(
                        null=self.determine_type_of_expression(ifthen.ifs[-1].then))))

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

        regexp_strpos_func = self.lookup_function_by_name('regexp_strpos')
        greater_func = self.lookup_function_by_name('>')

        regexp_expr = regexp_strpos_function(regexp_strpos_func,
                                             self.convert_expression(in_.arguments[1]),
                                             self.convert_expression(in_.arguments[0]),
                                             bigint_literal(1), bigint_literal(1))
        return greater_function(greater_func, regexp_expr, bigint_literal(0))

    def convert_unresolved_function(
            self,
            unresolved_function: spark_exprs_pb2.Expression.UnresolvedFunction
    ) -> algebra_pb2.Expression:
        """Convert a Spark unresolved function into a Substrait scalar function."""
        if unresolved_function.function_name == 'when':
            return self.convert_when_function(unresolved_function)
        if unresolved_function.function_name == 'in':
            return self.convert_in_function(unresolved_function)
        if unresolved_function.function_name == 'rlike':
            return self.convert_rlike_function(unresolved_function)
        func = algebra_pb2.Expression.ScalarFunction()
        function_def = self.lookup_function_by_name(unresolved_function.function_name)
        func.function_reference = function_def.anchor
        for idx, arg in enumerate(unresolved_function.arguments):
            if function_def.max_args is not None and idx >= function_def.max_args:
                break
            if unresolved_function.function_name == 'count' and arg.WhichOneof(
                    'expr_type') == 'unresolved_star':
                # Ignore all the rest of the arguments.
                break
            func.arguments.append(
                algebra_pb2.FunctionArgument(value=self.convert_expression(arg)))
        if unresolved_function.is_distinct:
            raise NotImplementedError(
                'Treating arguments as distinct is not supported for unresolved functions.')
        func.output_type.CopyFrom(function_def.output_type)
        return algebra_pb2.Expression(scalar_function=func)

    def convert_alias_expression(
            self, alias: spark_exprs_pb2.Expression.Alias) -> algebra_pb2.Expression:
        """Convert a Spark alias into a Substrait expression."""
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
        cast_rel = algebra_pb2.Expression.Cast(input=self.convert_expression(cast.expr))
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

    def convert_expression(self, expr: spark_exprs_pb2.Expression) -> algebra_pb2.Expression:
        """Convert a SparkConnect expression to a Substrait expression."""
        match expr.WhichOneof('expr_type'):
            case 'literal':
                result = self.convert_literal_expression(expr.literal)
            case 'unresolved_attribute':
                result = self.convert_unresolved_attribute(expr.unresolved_attribute)
            case 'unresolved_function':
                result = self.convert_unresolved_function(expr.unresolved_function)
            case 'expression_string':
                raise NotImplementedError(
                    'expression_string expression type not supported')
            case 'unresolved_star':
                raise NotImplementedError(
                    '* expressions are only supported within count aggregations')
            case 'alias':
                result = self.convert_alias_expression(expr.alias)
            case 'cast':
                result = self.convert_cast_expression(expr.cast)
            case 'unresolved_regex':
                raise NotImplementedError(
                    'unresolved_regex expression type not supported')
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
                raise NotImplementedError(
                    'unresolved_extract_value expression type not supported')
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
        return result

    def convert_expression_to_aggregate_function(
            self,
            expr: spark_exprs_pb2.Expression) -> algebra_pb2.AggregateFunction:
        """Convert a SparkConnect expression to a Substrait expression."""
        func = algebra_pb2.AggregateFunction()
        expression = self.convert_expression(expr)
        match expression.WhichOneof('rex_type'):
            case 'scalar_function':
                function = expression.scalar_function
            case 'window_function':
                function = expression.window_function
            case _:
                raise NotImplementedError(
                    'only functions of type unresolved function are supported in aggregate '
                    'relations')
        func.function_reference = function.function_reference
        func.arguments.extend(function.arguments)
        func.options.extend(function.options)
        func.output_type.CopyFrom(function.output_type)
        return func

    def convert_read_named_table_relation(
            self,
            rel: spark_relations_pb2.Read.named_table
    ) -> algebra_pb2.Rel:
        """Convert a read named table relation to a Substrait relation."""
        table_name = rel.unparsed_identifier

        backend = find_backend(BackendOptions(self._conversion_options.backend.backend, True))
        tpch_location = backend.find_tpch()
        backend.register_table(table_name, tpch_location / table_name)
        arrow_schema = backend.describe_table(table_name)
        schema = self.convert_arrow_schema(arrow_schema)

        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_name in schema.names:
            symbol.output_fields.append(field_name)

        return algebra_pb2.Rel(
            read=algebra_pb2.ReadRel(
                base_schema=schema,
                named_table=algebra_pb2.ReadRel.NamedTable(names=[table_name])))

    def convert_schema(self, schema_str: str) -> type_pb2.NamedStruct | None:
        """Convert the Spark JSON schema string into a Substrait named type structure."""
        if not schema_str:
            return None
        # TODO -- Deal with potential denial of service due to malformed JSON.
        schema_data = json.loads(schema_str)
        schema = type_pb2.NamedStruct()
        schema.struct.nullability = type_pb2.Type.NULLABILITY_REQUIRED
        for field in schema_data.get('fields'):
            schema.names.append(field.get('name'))
            if field.get('nullable'):
                nullability = type_pb2.Type.NULLABILITY_NULLABLE
            else:
                nullability = type_pb2.Type.NULLABILITY_REQUIRED
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
                    raise NotImplementedError(
                        f'Schema field type not yet implemented: {field.get("type")}')

            schema.struct.types.append(field_type)
        return schema

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
                    raise NotImplementedError(f'Unexpected field type: {field.type}')

            schema.struct.types.append(field_type)
        return schema

    def convert_read_data_source_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Convert a read data source relation into a Substrait relation."""
        local = algebra_pb2.ReadRel.LocalFiles()
        schema = self.convert_schema(rel.schema)
        if not schema:
            backend = find_backend(BackendOptions(self._conversion_options.backend.backend, True))
            try:
                backend.register_table(TABLE_NAME, rel.paths[0], rel.format)
                arrow_schema = backend.describe_table(TABLE_NAME)
                schema = self.convert_arrow_schema(arrow_schema)
            finally:
                backend.drop_table(TABLE_NAME)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_name in schema.names:
            symbol.output_fields.append(field_name)
        if self._conversion_options.use_named_table_workaround:
            return algebra_pb2.Rel(
                read=algebra_pb2.ReadRel(base_schema=schema,
                                         named_table=algebra_pb2.ReadRel.NamedTable(
                                             names=['demotable'])))
        if pathlib.Path(rel.paths[0]).is_dir():
            file_paths = glob.glob(f'{rel.paths[0]}/*{rel.format}')
        else:
            file_paths = rel.paths
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
        return algebra_pb2.Rel(read=algebra_pb2.ReadRel(base_schema=schema, local_files=local))

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
        result.read.common.CopyFrom(self.create_common_relation())
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
        for grouping in rel.grouping_expressions:
            aggregate.groupings.append(
                algebra_pb2.AggregateRel.Grouping(
                    grouping_expressions=[self.convert_expression(grouping)]))
            symbol.generated_fields.append(self.determine_name_for_grouping(grouping))
        for expr in rel.aggregate_expressions:
            aggregate.measures.append(
                algebra_pb2.AggregateRel.Measure(
                    measure=self.convert_expression_to_aggregate_function(expr))
            )
            symbol.generated_fields.append(self.determine_expression_name(expr))
        symbol.output_fields.clear()
        symbol.output_fields.extend(symbol.generated_fields)
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
        remapped = False
        mapping = list(range(len(symbol.input_fields)))
        field_number = len(symbol.input_fields)
        for alias in rel.aliases:
            if len(alias.name) != 1:
                raise ValueError('every column alias must have exactly one name')
            name = alias.name[0]
            project.expressions.append(self.convert_expression(alias.expr))
            if name in symbol.input_fields:
                remapped = True
                mapping[symbol.input_fields.index(name)] = len(symbol.input_fields) + (
                    len(project.expressions)) - 1
            else:
                mapping.append(field_number)
                field_number += 1
                symbol.generated_fields.append(name)
                symbol.output_fields.append(name)
        project.common.CopyFrom(self.create_common_relation())
        if remapped:
            for item in mapping:
                project.common.emit.output_mapping.append(item)
        return algebra_pb2.Rel(project=project)

    def convert_to_df_relation(self, rel: spark_relations_pb2.ToDF) -> algebra_pb2.Rel:
        """Convert a to dataframe relation into a Substrait project relation."""
        input_rel = self.convert_relation(rel.input)
        project = algebra_pb2.ProjectRel(input=input_rel)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        if len(rel.column_names) != len(symbol.input_fields):
            raise ValueError('column_names does not match the number of input fields at '
                             f'plan id {self._current_plan_id}')
        symbol.output_fields.clear()
        for field_name in rel.column_names:
            symbol.output_fields.append(field_name)
        project.common.CopyFrom(self.create_common_relation())
        return algebra_pb2.Rel(project=project)

    def convert_arrow_to_literal(self, val: pa.Scalar) -> algebra_pb2.Expression.Literal:
        """Convert an Arrow scalar into a Substrait literal."""
        literal = algebra_pb2.Expression.Literal()
        if isinstance(val, pa.BooleanScalar):
            literal.boolean = val.as_py()
        elif isinstance(val, pa.StringScalar):
            literal.string = val.as_py()
        else:
            raise NotImplementedError(
                f'Conversion from arrow type {val.type} not yet implemented.')
        return literal

    def convert_arrow_data_to_virtual_table(self,
                                            data: bytes) -> algebra_pb2.ReadRel.VirtualTable:
        """Convert a Spark local relation into a virtual table."""
        table = algebra_pb2.ReadRel.VirtualTable()
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
        plan = convert_sql(rel.query)
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
        return algebra_pb2.Rel(join=join)

    def convert_join_relation(self, rel: spark_relations_pb2.Join) -> algebra_pb2.Rel:
        """Convert a Spark join relation into a Substrait join relation."""
        if rel.join_type == spark_relations_pb2.Join.JOIN_TYPE_CROSS:
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
            project.expressions.append(self.convert_expression(expr))
            if expr.HasField('alias'):
                name = expr.alias.name[0]
            else:
                name = f'generated_field_{field_number}'
            symbol.generated_fields.append(name)
            symbol.output_fields.append(name)
        project.common.CopyFrom(self.create_common_relation())
        return algebra_pb2.Rel(project=project)

    def convert_subquery_alias_relation(self,
                                        rel: spark_relations_pb2.SubqueryAlias) -> algebra_pb2.Rel:
        """Convert a Spark subquery alias relation into a Substrait relation."""
        # TODO -- Utilize rel.alias somehow.
        result = self.convert_relation(rel.input)
        self.update_field_references(rel.input.common.plan_id)
        return result

    def convert_deduplicate_relation(self, rel: spark_relations_pb2.Deduplicate) -> algebra_pb2.Rel:
        """Convert a Spark deduplicate relation into a Substrait aggregation."""
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
                        output_type=type_pb2.Type(bool=type_pb2.Type.Boolean()))))
            symbol.generated_fields.append(field)
        return algebra_pb2.Rel(aggregate=aggregate)

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
            case _:
                raise ValueError(
                    f'Unexpected Spark plan rel_type: {rel.WhichOneof("rel_type")}')
        self._current_plan_id = old_plan_id
        return result

    def convert_plan(self, plan: spark_pb2.Plan) -> plan_pb2.Plan:
        """Convert a Spark plan into a Substrait plan."""
        result = plan_pb2.Plan()
        result.version.CopyFrom(
            plan_pb2.Version(minor_number=42, producer='spark-substrait-gateway'))
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
