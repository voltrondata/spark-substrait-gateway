# SPDX-License-Identifier: Apache-2.0
"""Given a backend enum, returns an instance of the correct Backend descendant."""
from gateway.backends import backend
from gateway.backends.adbc_backend import AdbcBackend
from gateway.backends.arrow_backend import ArrowBackend
from gateway.backends.backend_options import BackendEngine, BackendOptions
from gateway.backends.datafusion_backend import DatafusionBackend
from gateway.backends.duckdb_backend import DuckDBBackend


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
            raise ValueError(f'Unknown backend {options.backend} requested.')
