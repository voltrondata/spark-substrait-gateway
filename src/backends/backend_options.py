# SPDX-License-Identifier: Apache-2.0
"""Tracks backend related options."""

import dataclasses
from enum import Enum


class BackendEngine(Enum):
    """Represents the different backends we have support for."""

    ARROW = 1
    DATAFUSION = 2
    DUCKDB = 3

    def __str__(self):
        """Return the string representation of the backend."""
        return self.name.lower()


@dataclasses.dataclass
class BackendOptions:
    """Holds all the possible backend options."""

    backend: BackendEngine
    use_adbc: bool

    def __init__(self, backend: BackendEngine, use_adbc: bool = False):
        """Create a BackendOptions structure."""
        self.backend = backend
        self.use_adbc = use_adbc

        self.use_arrow_uri_workaround = False
        self.use_duckdb_python_api = False
        self.register_virtual_tables_as_local = False
