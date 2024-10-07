# SPDX-License-Identifier: Apache-2.0
"""Routines to manipulate arrow tables."""
import pyarrow as pa


def _reapply_names_to_type(array: pa.ChunkedArray, names: list[str]) -> (pa.Array, list[str]):
    new_arrays = []
    new_schema = []

    if array.type.num_fields > len(names):
        raise ValueError('Insufficient number of names provided to reapply names.')

    remaining_names = names
    if pa.types.is_list(array.type):
        raise NotImplementedError('Reapplying names to lists not yet supported')
    if pa.types.is_map(array.type):
        raise NotImplementedError('Reapplying names to maps not yet supported')
    if pa.types.is_struct(array.type):
        field_num = 0
        while field_num < array.type.num_fields:
            field = array.chunks[0].field(field_num)
            this_name = remaining_names.pop(0)

            new_array, remaining_names = _reapply_names_to_type(field, remaining_names)
            new_arrays.append(new_array)

            new_schema.append(pa.field(this_name, new_array.type))

            field_num += 1

        return pa.StructArray.from_arrays(new_arrays, fields=new_schema), remaining_names
    if array.type.num_fields != 0:
        raise ValueError(f'Unsupported complex type: {array.type}')
    return array, remaining_names


def reapply_names(table: pa.Table, names: list[str]) -> pa.Table:
    """Apply the provided names to the given table recursively."""
    new_arrays = []
    new_schema = []

    remaining_names = names
    for column in iter(table.columns):
        this_name = remaining_names.pop(0)

        new_array, remaining_names = _reapply_names_to_type(column, remaining_names)
        new_arrays.append(new_array)

        new_schema.append(pa.field(this_name, new_array.type))

    return pa.Table.from_arrays(new_arrays, schema=pa.schema(new_schema))
