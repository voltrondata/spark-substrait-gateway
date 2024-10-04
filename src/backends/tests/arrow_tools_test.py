from dataclasses import dataclass
from typing import List
import pytest
import pyarrow as pa

from src.backends.arrow_tools import reapply_names


@dataclass
class TestCase:
    name: str
    input: pa.Table
    names: List[str]
    expected: pa.table


cases: List[TestCase] = [
    TestCase('empty table', pa.Table.from_arrays([]), ['a', 'b'], pa.Table.from_arrays([])),
    TestCase('normal columns',
             pa.Table.from_pydict(
                 {"name": [None, "Joe", "Sarah", None], "age": [99, None, 42, None]},
                 schema=pa.schema({"name": pa.string(), "age": pa.int32()})
             ),
             ['renamed_name', 'renamed_age'],
             pa.Table.from_pydict(
                 {"renamed_name": [None, "Joe", "Sarah", None], "renamed_age": [99, None, 42, None]},
                 schema=pa.schema({"renamed_name": pa.string(), "renamed_age": pa.int32()})
             )),
    TestCase('struct column',
             pa.Table.from_arrays(
                 [pa.array([{"": 1, "b": "b"}], type=pa.struct([("", pa.int64()), ("b", pa.string())]))],
                 names=["r"]),
             ['r', 'a', 'b'],
             pa.Table.from_arrays(
                 [pa.array([{"a": 1, "b": "b"}], type=pa.struct([("a", pa.int64()), ("b", pa.string())]))], names=["r"])
             ),
    TestCase('nested structs', pa.Table.from_arrays([]), ['a', 'b'], pa.Table.from_arrays([])),
    # TODO -- Test a list.
    # TODO -- Test a map.
    # TODO -- Test a mixture of complex and simple types.
]


class TestArrowTools:
    """Tests the functionality of the arrow tools package."""

    @pytest.mark.parametrize(
        "case", cases, ids=lambda case: case.name
    )
    def test_reapply_names(self, case):
        result = reapply_names(case.input, case.names)
        assert result == case.expected
