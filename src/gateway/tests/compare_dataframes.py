# SPDX-License-Identifier: Apache-2.0
"""Routines for comparing dataframes."""
import datetime
from decimal import Decimal

from pyspark import Row
from pyspark.testing import assertDataFrameEqual


def have_same_schema(outcome: list[Row], expected: list[Row]):
    """Returns True if the two dataframes have the same schema."""
    return all(type(a) is type(b) for a, b in zip(outcome[0], expected[0], strict=False))


def align_schema(source_df: list[Row], schema_df: list[Row]):
    """Returns a copy of source_df with the fields changed to match schema_df."""
    schema = schema_df[0]

    if have_same_schema(source_df, schema_df):
        return source_df

    new_source_df = []
    for row in source_df:
        new_row = {}
        for field_name, field_value in schema.asDict().items():
            if type(row[field_name]) is not type(field_value):
                if isinstance(field_value, datetime.date):
                    if row[field_name] is None:
                        new_row[field_name] = None
                    else:
                        new_row[field_name] = row[field_name].date()
                elif isinstance(field_value, float):
                    if row[field_name] is None:
                        new_row[field_name] = None
                    else:
                        new_row[field_name] = float(row[field_name])
                else:
                    new_row[field_name] = row[field_name]
            else:
                new_row[field_name] = row[field_name]

        new_source_df.append(Row(**new_row))

    return new_source_df


def assert_dataframes_equal(outcome: list[Row], expected: list[Row]):
    """Asserts that two dataframes are equal ignoring column names and date formats."""
    # Create a copy of the dataframes to avoid modifying the original ones
    modified_outcome = align_schema(outcome, expected)

    assertDataFrameEqual(modified_outcome, expected, checkRowOrder=True, atol=1e-2)
