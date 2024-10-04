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
    TestCase('normal columns', pa.Table.from_arrays([]), ['a', 'b', 'c'], pa.Table.from_arrays([])),
    TestCase('struct column', pa.Table.from_arrays([]), ['a', 'b'], pa.Table.from_arrays([])),
    TestCase('nested structs', pa.Table.from_arrays([]), ['a', 'b'], pa.Table.from_arrays([])),
]


class TestArrowTools:
    """Tests the functionality of the arrow tools package."""

    @pytest.mark.parametrize(
        "case", cases, ids=lambda case: case.name
    )
    def test_reapply_names(self, case):
        result = reapply_names(case.input, case.names)
        assert result == case.expected
