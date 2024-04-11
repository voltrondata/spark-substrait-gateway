# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from pathlib import Path
from typing import List

import duckdb
from substrait.gen.proto import plan_pb2


def _expand_location(location: Path | str) -> List[str]:
    """Expands the location of a file or directory into a list of files."""
    # TODO -- Handle more than just Parquet files (here and below).
    files = Path(location).resolve().glob('*.parquet')
    return sorted(str(f) for f in files)


def find_tpch() -> Path:
    """Finds the location of the TPCH dataset."""
    current_location = Path('.').resolve()
    while current_location != Path('/'):
        location = current_location / 'third_party' / 'tpch' / 'parquet'
        if location.exists():
            return location.resolve()
        current_location = current_location.parent
    raise ValueError('TPCH dataset not found')


def register_table(con: duckdb.DuckDBPyConnection, table_name, location: Path | str) -> None:
    files = _expand_location(location)
    if not files:
        raise ValueError(f"No parquet files found at {location}")
    files_str = ', '.join([f"'{f}'" for f in files])
    files_sql = f"CREATE OR REPLACE TABLE {table_name} AS FROM read_parquet([{files_str}])"

    con.execute(files_sql)


# pylint: disable=E1101,too-few-public-methods,fixme
def convert_sql(sql: str) -> plan_pb2.Plan:
    """Converts SQL into a Substrait plan."""
    result = plan_pb2.Plan()
    con = duckdb.connect(config={'max_memory': '100GB',
                                 'temp_directory': str(Path('.').resolve())})
    con.install_extension('substrait')
    con.load_extension('substrait')

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

    proto_bytes = con.get_substrait(query=sql).fetchone()[0]
    result.ParseFromString(proto_bytes)
    return result
