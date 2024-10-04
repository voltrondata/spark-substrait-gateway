from typing import List

import pyarrow as pa


def _reapply_names_to_struct(struct: pa.StructArray, names: List[str]) -> pa.StructArray:
    return struct


def reapply_names(table: pa.Table, names: List[str]) -> pa.Table:
    new_arrays = []
    new_schema = []

    for column in iter(table.columns):
        # TODO: Rebuild the data.
        # TODO: Save the schema.
        pass

    new_schema = pa.schema(
        [
            pa.field("test_struct", pa.struct([
                pa.field("custid", pa.int64()),
                pa.field("custname", pa.string()),
            ])),
        ]
    )
    custid_array = table.columns[0].chunks[0].field(0)
    custname_array = table.columns[0].chunks[0].field(1)

    new_struct_array = pa.StructArray.from_arrays([custid_array, custname_array], names=['custid', 'custname'])
    new_table = pa.Table.from_arrays([new_struct_array], schema=new_schema)

    return new_table
