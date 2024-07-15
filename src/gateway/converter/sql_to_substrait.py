# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from backends.backend import Backend
from gateway.converter.add_extension_uris import AddExtensionUris
from substrait.gen.proto import plan_pb2


def convert_sql(backend: Backend, sql: str) -> plan_pb2.Plan:
    """Convert SQL into a Substrait plan."""
    plan = backend.convert_sql(sql)

    # Perform various fixes to make the plan more compatible.
    # TODO -- Remove this after the SQL converter is fixed.
    AddExtensionUris().visit_plan(plan)

    return plan
