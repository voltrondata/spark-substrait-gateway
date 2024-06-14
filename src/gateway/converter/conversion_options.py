# SPDX-License-Identifier: Apache-2.0
"""Tracks conversion related options."""
import dataclasses

from gateway.backends.backend_options import BackendEngine, BackendOptions


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
        self.use_first_value_as_any_value = False
        self.use_regexp_like_function = False
        self.duckdb_project_emit_workaround = False
        self.drop_emit_workaround = True
        self.safety_project_read_relations = False
        self.use_duckdb_struct_name_behavior = False
        self.fetch_return_all_workaround = True
        self.only_use_binary_boolean_operators = False

        self.return_names_with_types = False

        self.implement_show_string = True

        self.backend = backend


def arrow():
    """Return standard options to connect to the Acero backend."""
    options = ConversionOptions(backend=BackendOptions(BackendEngine.ARROW))
    options.needs_scheme_in_path_uris = True
    options.return_names_with_types = True
    options.backend.use_arrow_uri_workaround = True
    options.safety_project_read_relations = True
    return options


def datafusion():
    """Return standard options to connect to a Datafusion backend."""
    options = ConversionOptions(backend=BackendOptions(BackendEngine.DATAFUSION))
    options.use_switch_expressions_where_possible = False
    options.use_regexp_like_function = True
    options.use_first_value_as_any_value = True
    options.only_use_binary_boolean_operators = True
    return options


def duck_db():
    """Return standard options to connect to a DuckDB backend."""
    options = ConversionOptions(backend=BackendOptions(BackendEngine.DUCKDB))
    options.return_names_with_types = True
    options.use_switch_expressions_where_possible = False
    options.use_duckdb_regexp_matches_function = True
    options.duckdb_project_emit_workaround = True
    options.backend.use_duckdb_python_api = False
    options.use_duckdb_struct_name_behavior = True
    return options
