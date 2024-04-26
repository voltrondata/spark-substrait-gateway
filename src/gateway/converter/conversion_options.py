# SPDX-License-Identifier: Apache-2.0
"""Tracks conversion related options."""
import dataclasses

from gateway.backends.backend_options import Backend, BackendOptions


# pylint: disable=too-many-instance-attributes
@dataclasses.dataclass
class ConversionOptions:
    """Holds all the possible conversion options."""

    def __init__(self, backend: BackendOptions = None):
        """Initialize the conversion options."""
        self.use_named_table_workaround = False
        self.needs_scheme_in_path_uris = False
        self.use_emits_instead_of_direct = False
        self.use_switch_expressions_where_possible = True
        self.use_duckdb_regexp_matches_function = False

        self.return_names_with_types = False

        self.implement_show_string = True

        self.backend = backend


def arrow():
    """Return standard options to connect to the Acero backend."""
    options = ConversionOptions(backend=BackendOptions(Backend.ARROW))
    options.needs_scheme_in_path_uris = True
    options.return_names_with_types = True
    options.implement_show_string = False
    options.backend.use_arrow_uri_workaround = True
    return options


def datafusion():
    """Return standard options to connect to a Datafusion backend."""
    return ConversionOptions(backend=BackendOptions(Backend.DATAFUSION))


def duck_db():
    """Return standard options to connect to a DuckDB backend."""
    options = ConversionOptions(backend=BackendOptions(Backend.DUCKDB))
    options.return_names_with_types = True
    options.use_switch_expressions_where_possible = False
    options.use_duckdb_regexp_matches_function = True
    return options
