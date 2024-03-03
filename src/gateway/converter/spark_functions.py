# SPDX-License-Identifier: Apache-2.0
"""Provides the mapping of Spark functions to Substrait."""
import dataclasses


@dataclasses.dataclass
class ExtensionFunction:
    """Represents a Substrait function."""
    uri: str
    name: str
    output_type: str
    anchor: int

    def __init__(self, uri: str, name: str):
        self.uri = uri
        self.name = name

    def __lt__(self, obj):
        return ((self.uri) < (obj.uri) and (self.name) < (obj.name))


SPARK_SUBSTRAIT_MAPPING = {
    'split': ExtensionFunction('/functions_string.yaml', 'string_split:str_str'),
    '==': ExtensionFunction('/functions_comparison.yaml', 'equal:str_str'),
    'array_contains': ExtensionFunction('/functions_set.yaml', 'index_in:str_liststr')
}


def lookup_spark_function(name: str) -> ExtensionFunction:
    """Returns a Substrait function given a spark function name."""
    return SPARK_SUBSTRAIT_MAPPING.get(name)
