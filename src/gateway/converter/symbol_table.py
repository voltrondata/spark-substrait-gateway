# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
import dataclasses
from typing import Optional, List


@dataclasses.dataclass
class PlanMetadata:
    """Tracks various information about a specific plan id."""
    parent_plan_id: Optional[int]
    input_fields: List[str]  # And maybe type
    output_fields: List[str]

    def __init__(self):
        self.parent_plan_id = None
        self.input_fields = []
        self.output_fields = []
