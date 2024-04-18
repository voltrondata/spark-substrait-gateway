# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from gateway.backends import backend_selector
from gateway.backends.backend_options import Backend, BackendOptions
from substrait.gen.proto import plan_pb2


def convert_sql(sql: str) -> plan_pb2.Plan:
    """Convert SQL into a Substrait plan."""
    result = plan_pb2.Plan()

    backend = backend_selector.find_backend(BackendOptions(Backend.DUCKDB, False))
    backend.register_tpch()

    connection = backend.get_connection()
    proto_bytes = connection.get_substrait(query=sql).fetchone()[0]
    result.ParseFromString(proto_bytes)
    return result
