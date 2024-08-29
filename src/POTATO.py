import ibis
import pyarrow.parquet as pq
from ibis.expr.sql import parse_sql
from ibis_substrait.compiler.core import SubstraitCompiler

def main():
    sql = "SELECT COUNT(c_phone) FROM customer"
    table_schemas: dict[str, ibis.Schema] = {}

    r = pq.read_table(
        '/Users/davids/projects/voltrondata-spark-substrait-gateway/'
        'data/tpch/parquet/customer.parquet')
    table_schemas['customer'] = ibis.Schema.from_pyarrow(r.schema)

    expr = parse_sql(sql, catalog=table_schemas)
    compiler = SubstraitCompiler()
    print(compiler.compile(expr))


if __name__ == "__main__":
    main()
