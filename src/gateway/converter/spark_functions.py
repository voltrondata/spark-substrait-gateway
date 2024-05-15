# SPDX-License-Identifier: Apache-2.0
"""Provides the mapping of Spark functions to Substrait."""
import dataclasses

from gateway.converter.conversion_options import ConversionOptions
from substrait.gen.proto import type_pb2


# pylint: disable=E1101
@dataclasses.dataclass
class ExtensionFunction:
    """Represents a Substrait function."""

    uri: str
    name: str
    output_type: type_pb2.Type
    anchor: int
    max_args: int | None

    def __init__(self, uri: str, name: str, output_type: type_pb2.Type,
                 max_args: int | None = None):
        """Create the ExtensionFunction structure."""
        self.uri = uri
        self.name = name
        self.output_type = output_type
        self.max_args = max_args

    def __lt__(self, obj) -> bool:
        """Compare two ExtensionFunction objects."""
        return self.uri < obj.uri and self.name < obj.name


# pylint: disable=E1101
SPARK_SUBSTRAIT_MAPPING = {
    'split': ExtensionFunction(
        '/functions_string.yaml', 'string_split:str_str', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED)),
        max_args=2),
    '==': ExtensionFunction(
        '/functions_comparison.yaml', 'equal:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '<=': ExtensionFunction(
        '/functions_comparison.yaml', 'lte:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '>=': ExtensionFunction(
        '/functions_comparison.yaml', 'gte:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '<': ExtensionFunction(
        '/functions_comparison.yaml', 'lt:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '>': ExtensionFunction(
        '/functions_comparison.yaml', 'gt:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '+': ExtensionFunction(
        '/functions_arithmetic.yaml', 'add:i64_i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '-': ExtensionFunction(
        '/functions_arithmetic.yaml', 'subtract:i64_i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '*': ExtensionFunction(
        '/functions_arithmetic.yaml', 'multiply:i64_i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    '/': ExtensionFunction(
        '/functions_arithmetic.yaml', 'divide:i64_i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'array_contains': ExtensionFunction(
        '/functions_set.yaml', 'index_in:str_list<str>', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'sum': ExtensionFunction(
        '/functions_arithmetic.yaml', 'sum:int', type_pb2.Type(
            i32=type_pb2.Type.I32(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'try_sum': ExtensionFunction(
        '/functions_arithmetic.yaml', 'sum:int', type_pb2.Type(
            i32=type_pb2.Type.I32(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'avg': ExtensionFunction(
        '/functions_arithmetic.yaml', 'avg:int', type_pb2.Type(
            i32=type_pb2.Type.I32(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'regexp_extract_all': ExtensionFunction(
        '/functions_string.yaml', 'regexp_match:str_binary_str', type_pb2.Type(
            list=type_pb2.Type.List(type=type_pb2.Type(string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))))),
    'regexp_substring': ExtensionFunction(
        '/functions_string.yaml', 'regexp_substring:str_str_i64_i64', type_pb2.Type(
            i64=type_pb2.Type.I64(nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'DUCKDB_regexp_matches': ExtensionFunction(
        '/functions_string.yaml', 'regexp_matches:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'substring': ExtensionFunction(
        '/functions_string.yaml', 'substring:str_int_int', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'startswith': ExtensionFunction(
        '/functions_string.yaml', 'starts_with:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'endswith': ExtensionFunction(
        '/functions_string.yaml', 'ends_with:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'contains': ExtensionFunction(
        '/functions_string.yaml', 'contains:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'length': ExtensionFunction(
        '/functions_string.yaml', 'char_length:str', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'max': ExtensionFunction(
        '/unknown.yaml', 'max:i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'min': ExtensionFunction(
        '/unknown.yaml', 'min:i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'string_agg': ExtensionFunction(
        '/functions_string.yaml', 'string_agg:str', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'least': ExtensionFunction(
        '/functions_comparison.yaml', 'least:i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'greatest': ExtensionFunction(
        '/functions_comparison.yaml', 'greatest:i64', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'concat': ExtensionFunction(
        '/functions_string.yaml', 'concat:str_str', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'repeat': ExtensionFunction(
        '/functions_string.yaml', 'repeat:str_i64', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'lpad': ExtensionFunction(
        '/functions_string.yaml', 'lpad:str_i64_str', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'rpad': ExtensionFunction(
        '/functions_string.yaml', 'rpad:str_i64_str', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'count': ExtensionFunction(
        '/functions_aggregate_generic.yaml', 'count:any', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'any_value': ExtensionFunction(
        '/functions_aggregate_generic.yaml', 'any_value:any', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'and': ExtensionFunction(
        '/functions_boolean.yaml', 'and:bool_bool', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'or': ExtensionFunction(
        '/functions_boolean.yaml', 'or:bool_bool', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'not': ExtensionFunction(
        '/functions_boolean.yaml', 'not:bool', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED)))
}


def lookup_spark_function(name: str, options: ConversionOptions) -> ExtensionFunction:
    """Return a Substrait function given a spark function name."""
    definition = SPARK_SUBSTRAIT_MAPPING.get(name)
    if not options.return_names_with_types:
        definition.name = definition.name.split(':', 1)[0]
    return definition
