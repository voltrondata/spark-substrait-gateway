# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""

import dataclasses


@dataclasses.dataclass
class PlanMetadata:
    """Tracks various information about a specific plan id."""

    plan_id: int
    type: str | None
    parent_plan_id: int | None
    input_fields: list[str]  # And maybe type with additional names
    generated_fields: list[str]
    output_fields: list[str]

    def __init__(self, plan_id: int):
        """Create the PlanMetadata structure."""
        self.plan_id = plan_id
        self.symbol_type = None  # Useful when debugging.
        self.parent_plan_id = None
        self.input_fields = []
        self.generated_fields = []
        self.output_fields = []


class SymbolTable:
    """Manages metadata related to symbols and provides easy lookup."""

    _symbols: dict[int, PlanMetadata]

    def __init__(self):
        """Initialize the symbol table."""
        self._symbols = {}

    # pylint: disable=E1101
    def add_symbol(self, plan_id: int, parent: int | None, symbol_type: str | None):
        """Create a new symbol and returns it."""
        symbol = PlanMetadata(plan_id)
        symbol.symbol_type = symbol_type
        symbol.parent_plan_id = parent
        self._symbols[plan_id] = symbol
        return symbol

    def get_symbol(self, plan_id: int) -> PlanMetadata | None:
        """Fetch the symbol with the requested plan id."""
        return self._symbols.get(plan_id)
