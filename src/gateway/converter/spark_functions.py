# SPDX-License-Identifier: Apache-2.0
"""Provides the mapping of Spark functions to Substrait."""
import dataclasses
from typing import Optional

from substrait.gen.proto import type_pb2

from gateway.converter.conversion_options import ConversionOptions


# pylint: disable=E1101
@dataclasses.dataclass
class ExtensionFunction:
    """Represents a Substrait function."""
    uri: str
    name: str
    output_type: type_pb2.Type
    anchor: int
    max_args: Optional[int]

    def __init__(self, uri: str, name: str, output_type: type_pb2.Type,
                 max_args: Optional[int] = None):
        self.uri = uri
        self.name = name
        self.output_type = output_type
        self.max_args = max_args

    def __lt__(self, obj) -> bool:
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
    'array_contains': ExtensionFunction(
        '/functions_set.yaml', 'index_in:str_list<str>', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'sum': ExtensionFunction(
        '/functions_arithmetic.yaml', 'sum:int', type_pb2.Type(
            i32=type_pb2.Type.I32(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'regexp_extract_all': ExtensionFunction(
        '/functions_string.yaml', 'regexp_match:str_binary_str', type_pb2.Type(
            list=type_pb2.Type.List(type=type_pb2.Type(string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))))),
    'substring': ExtensionFunction(
        '/functions_string.yaml', 'substring:str_int_int', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'count': ExtensionFunction(
        '/functions_aggregate_generic.yaml', 'count:any', type_pb2.Type(
            i64=type_pb2.Type.I64(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED)))
}


def lookup_spark_function(name: str, options: ConversionOptions) -> ExtensionFunction:
    """Returns a Substrait function given a spark function name."""
    definition = SPARK_SUBSTRAIT_MAPPING.get(name)
    if not options.return_names_with_types:
        definition.name = definition.name.split(':', 1)[0]
    return definition
