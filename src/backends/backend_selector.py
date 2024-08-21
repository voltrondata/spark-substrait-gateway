# SPDX-License-Identifier: Apache-2.0
"""Given a backend enum, returns an instance of the correct Backend descendant."""

from backends import backend
from backends.adbc_backend import AdbcBackend
from backends.arrow_backend import ArrowBackend
from backends.backend_options import BackendEngine, BackendOptions
from backends.datafusion_backend import DatafusionBackend
from backends.duckdb_backend import DuckDBBackend


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
        case _:
            raise ValueError(f"Unknown backend {options.backend} requested.")

def backend_type(backend_instance: backend.Backend) -> BackendEngine:
    if isinstance(backend_instance, DuckDBBackend):
        return BackendEngine.DUCKDB
    if isinstance(backend_instance, DatafusionBackend):
        return BackendEngine.DATAFUSION
    if isinstance(backend_instance, ArrowBackend):
        return BackendEngine.ARROW
    raise ValueError(f"Unknown backend type.")
