# Substrait Backends

This module contains an API to access Substrait backends in a uniform way.

Use `backend_selector.find_backend` to return a backend instance.  The instance may be used over
the course of several queries.  After an error the behavior is undefined (it will likely need
to be reset or replaced).

Once you have an instance you can call `get_connection` to retrieve a connection object.  You may
use the `register_table` and `describe_files` method to work with files and tables.  Pass a
Substrait plan to the `execute` method to run a plan.  Any associated plan modifications will
silently occur as part of the backend's `alter_plan` method.

Work is ongoing to reduce the number of workarounds defined in the `alter_plan` methods.  Rename
functions is used to workaround missing Substrait mappings while the backend's implementation is
updated.  Datafusion does not support local files (registering files for every query is prohibitive
anyway) so they are converted to named tables.  Ideally this should be done externally to avoid
costly table registration.  Acero's needs are less understood but it is known that casts require
field references and not expressions which can be handled with a plan rewrite.  It's not known if
the need for this will be eliminated.  Another behavior, not handled here, is that Datafusion does
not accept mixed scalar and aggregated functions as measures in aggregate relations.  Correction
of this behavior is currently handled externally but can be implemented here if it becomes needed.

