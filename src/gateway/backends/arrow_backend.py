# SPDX-License-Identifier: Apache-2.0
"""Provides access to Acero."""
from pathlib import Path

import pyarrow as pa
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend


class ArrowBackend(Backend):
    """Provides access to send Acero Substrait plans."""

    # pylint: disable=import-outside-toplevel
    def execute(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = pa.substrait.run_query(plan_data)
        return reader.read_all()

    def register_table(self, name: str, path: Path, file_format: str = 'parquet') -> None:
        """Register the given table with the backend."""
        raise NotImplementedError()
