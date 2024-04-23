# SPDX-License-Identifier: Apache-2.0
"""Tracks conversion related options."""
import dataclasses

from gateway.backends.backend_options import Backend, BackendOptions


# pylint: disable=too-many-instance-attributes
@dataclasses.dataclass
class ConversionOptions:
    """Holds all the possible conversion options."""

    use_named_table_workaround: bool
    needs_scheme_in_path_uris: bool
    use_project_emit_workaround: bool
    use_project_emit_workaround2: bool
    use_emits_instead_of_direct: bool

    return_names_with_types: bool

    def __init__(self, backend: BackendOptions = None):
        """Initialize the conversion options."""
        self.use_named_table_workaround = False
        self.needs_scheme_in_path_uris = False
        self.use_emits_instead_of_direct = False

        self.return_names_with_types = False

        self.implement_show_string = True

        self.backend = backend


def arrow():
    """Return standard options to connect to the Acero backend."""
    options = ConversionOptions(backend=BackendOptions(Backend.ARROW))
    options.needs_scheme_in_path_uris = True
    return options


def datafusion():
    """Return standard options to connect to a Datafusion backend."""
    return ConversionOptions(backend=BackendOptions(Backend.DATAFUSION))


def duck_db():
    """Return standard options to connect to a DuckDB backend."""
    options = ConversionOptions(backend=BackendOptions(Backend.DUCKDB))
    options.return_names_with_types = True
    return options
