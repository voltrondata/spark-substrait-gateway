# SPDX-License-Identifier: Apache-2.0
"""Given a backend enum, returns an instance of the correct Backend descendant."""

from backends import backend
from backends.adbc_backend import AdbcBackend
from backends.arrow_backend import ArrowBackend
from backends.backend_options import BackendEngine, BackendOptions
from backends.datafusion_backend import DatafusionBackend
from backends.duckdb_backend import DuckDBBackend
from backends.ibis_backend import IbisBackend


def find_backend(options: BackendOptions) -> backend.Backend:
    """Given a backend enum, returns an instance of the correct Backend descendant."""
    match options.backend:
        case BackendEngine.ARROW:
            return ArrowBackend(options)
        case BackendEngine.DATAFUSION:
            return DatafusionBackend(options)
        case BackendEngine.DUCKDB:
            if options.use_adbc:
                return AdbcBackend(options)
            return DuckDBBackend(options)
        case BackendEngine.IBIS:
            return IbisBackend(options)
        case _:
            raise ValueError(f"Unknown backend {options.backend} requested.")
