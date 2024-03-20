# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from pathlib import Path

import duckdb
from substrait.gen.proto import plan_pb2


# pylint: disable=E1101,too-few-public-methods
class SqlConverter:
    """Converts SQL to a Substrait plan."""

    def convert_sql(self, sql: str) -> plan_pb2.Plan:
        """Converts SQL into a Substrait plan."""
        result = plan_pb2.Plan()
        con = duckdb.connect(config={'max_memory': '100GB',
                                     'temp_directory': str(Path('.').absolute())})
        con.install_extension('substrait')
        con.load_extension('substrait')

        con.execute("CREATE TABLE users AS SELECT * FROM 'users.parquet'")

        proto_bytes = con.get_substrait(query=sql).fetchone()[0]
        result.ParseFromString(proto_bytes)
        return result
