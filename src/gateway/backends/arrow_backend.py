# SPDX-License-Identifier: Apache-2.0
"""Provides access to Acero."""
from pathlib import Path

import pyarrow
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend


class ArrowBackend(Backend):
    """Provides access to send Acero Substrait plans."""

    # pylint: disable=import-outside-toplevel
    def execute(self, plan: plan_pb2.Plan) -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = pyarrow.substrait.run_query(plan_data)
        query_result = reader.read_all()
        return query_result

    def register_table(self, name: str, path: Path) -> None:
        """Registers the given table with the backend."""
        raise NotImplementedError()
