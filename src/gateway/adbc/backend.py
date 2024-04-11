# SPDX-License-Identifier: Apache-2.0
"""Will eventually provide client access to an ADBC backend."""
from pathlib import Path

import adbc_driver_duckdb.dbapi
import duckdb
import pyarrow
from pyarrow import substrait

from substrait.gen.proto import plan_pb2

from gateway.adbc.backend_options import BackendOptions, Backend
from gateway.converter.rename_functions import RenameFunctions
from gateway.converter.replace_local_files import ReplaceLocalFilesWithNamedTable
from gateway.converter.sql_to_substrait import register_table, find_tpch


# pylint: disable=protected-access
def _import(handle):
    return pyarrow.RecordBatchReader._import_from_c(handle.address)


# pylint: disable=fixme
class AdbcBackend:
    """Provides methods for contacting an ADBC backend via Substrait."""

    def __init__(self):
        pass

    def execute_with_duckdb_over_adbc(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against DuckDB using ADBC."""
        with adbc_driver_duckdb.dbapi.connect() as conn, conn.cursor() as cur:
            cur.execute("LOAD substrait;")
            plan_data = plan.SerializeToString()
            cur.adbc_statement.set_substrait_plan(plan_data)
            res = cur.adbc_statement.execute_query()
            table = _import(res[0]).read_all()
            return table

    # pylint: disable=import-outside-toplevel
    def execute_with_datafusion(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Datafusion."""
        import datafusion.substrait

        ctx = datafusion.SessionContext()

        file_groups = ReplaceLocalFilesWithNamedTable().visit_plan(plan)
        registered_tables = set()
        for files in file_groups:
            table_name = files[0]
            for file in files[1]:
                if table_name not in registered_tables:
                    ctx.register_parquet(table_name, file)
                registered_tables.add(files[0])

        RenameFunctions().visit_plan(plan)

        try:
            plan_data = plan.SerializeToString()
            substrait_plan = datafusion.substrait.substrait.serde.deserialize_bytes(plan_data)
            logical_plan = datafusion.substrait.substrait.consumer.from_substrait_plan(
                ctx, substrait_plan
            )

            # Create a DataFrame from a deserialized logical plan.
            df_result = ctx.create_dataframe_from_logical_plan(logical_plan)
            for column_number, column_name in enumerate(df_result.schema().names):
                df_result = df_result.with_column_renamed(
                    column_name,
                    plan.relations[0].root.names[column_number]
                )
            return df_result.to_arrow_table()
        finally:
            for table_name in registered_tables:
                ctx.deregister_table(table_name)

    def execute_with_duckdb(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against DuckDB."""
        con = duckdb.connect(config={'max_memory': '100GB',
                                     "allow_unsigned_extensions": "true",
                                     'temp_directory': str(Path('.').absolute())})
        con.install_extension('substrait')
        con.load_extension('substrait')
        plan_data = plan.SerializeToString()

        # TODO -- Rely on the client to register their own named tables.
        tpch_location = find_tpch()
        register_table(con, 'customer', tpch_location / 'customer')
        register_table(con, 'lineitem', tpch_location / 'lineitem')
        register_table(con, 'nation', tpch_location / 'nation')
        register_table(con, 'orders', tpch_location / 'orders')
        register_table(con, 'part', tpch_location / 'part')
        register_table(con, 'partsupp', tpch_location / 'partsupp')
        register_table(con, 'region', tpch_location / 'region')
        register_table(con, 'supplier', tpch_location / 'supplier')

        try:
            query_result = con.from_substrait(proto=plan_data)
        except Exception as err:
            raise ValueError(f'DuckDB Execution Error: {err}') from err
        df = query_result.df()
        return pyarrow.Table.from_pandas(df=df)

    def execute_with_arrow(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = substrait.run_query(plan_data)
        query_result = reader.read_all()
        return query_result

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
