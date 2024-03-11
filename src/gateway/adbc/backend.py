# SPDX-License-Identifier: Apache-2.0
"""Will eventually provide client access to an ADBC backend."""
from pathlib import Path

import adbc_driver_duckdb.dbapi
import duckdb
import pyarrow
from pyarrow import substrait
from datafusion import SessionContext
from datafusion import substrait as ds

from substrait.gen.proto import plan_pb2

from gateway.adbc.backend_options import BackendOptions, Backend


# pylint: disable=fixme
class AdbcBackend:
    """Provides methods for contacting an ADBC backend via Substrait."""

    def __init__(self):
        pass

    def execute_with_duckdb_over_adbc(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against DuckDB using ADBC."""
        with adbc_driver_duckdb.dbapi.connect() as conn, conn.cursor() as cur:
            plan_data = plan.SerializeToString()
            cur.adbc_statement.set_substrait_plan(plan_data)
            tbl = cur.fetch_arrow_table()
            return tbl

    def execute_with_duckdb(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against DuckDB."""
        con = duckdb.connect(config={'max_memory': '100GB',
                                     'temp_directory': str(Path('.').absolute())})
        con.install_extension('substrait')
        con.load_extension('substrait')
        plan_data = plan.SerializeToString()
        query_result = con.from_substrait(proto=plan_data)
        df = query_result.df()
        return pyarrow.Table.from_pandas(df=df)

    def execute_with_arrow(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = substrait.run_query(plan_data)
        query_result = reader.read_all()
        return query_result

    def execute_with_datafusion(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Datafusion."""
        ctx = SessionContext()
        # TODO -- Handle registration by scanning and then rewriting the plan.
        ctx.register_parquet("demotable", 'artists.parquet')

        plan_data = plan.SerializeToString()
        substrait_plan = ds.substrait.serde.deserialize_bytes(plan_data)
        logical_plan = ds.substrait.consumer.from_substrait_plan(
            ctx, substrait_plan
        )

        # Create a DataFrame from a deserialized logical plan
        df_result = ctx.create_dataframe_from_logical_plan(logical_plan)
        return df_result.to_arrow_table()

    def execute(self, plan: 'plan_pb2.Plan', options: BackendOptions) -> pyarrow.lib.Table:
        """Executes the given Substrait plan."""
        match options.backend:
            case Backend.ARROW:
                return self.execute_with_arrow(plan)
            case Backend.DATAFUSION:
                return self.execute_with_datafusion(plan)
            case Backend.DUCKDB:
                if options.use_adbc:
                    return self.execute_with_duckdb_over_adbc(plan)
                return self.execute_with_duckdb(plan)
            case _:
                raise ValueError('unknown backend requested')
