# SPDX-License-Identifier: Apache-2.0
"""Tracks backend related options."""
import dataclasses
from enum import Enum


class Backend(Enum):
    """Represents the different backends we have support for."""

    ARROW = 1
    DATAFUSION = 2
    DUCKDB = 3


@dataclasses.dataclass
class BackendOptions:
    """Holds all the possible backend options."""

    backend: Backend
    use_adbc: bool

    def __init__(self, backend: Backend, use_adbc: bool = False):
        """Create a BackendOptions structure."""
        self.backend = backend
        self.use_adbc = use_adbc

        self.use_arrow_uri_workaround = False
