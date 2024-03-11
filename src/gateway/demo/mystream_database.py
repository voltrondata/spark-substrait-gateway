# SPDX-License-Identifier: Apache-2.0
"""Routines to create a fake mystream database for testing."""
from pathlib import Path

import pyarrow
from faker import Faker
from pyarrow import parquet

TABLE_SCHEMAS = {
    'users': pyarrow.schema([
        pyarrow.field('user_id', pyarrow.string(), False),
        pyarrow.field('name', pyarrow.string(), False),
        pyarrow.field('paid_for_service', pyarrow.bool_(), False),
    ], metadata={'user_id': 'A unique user id.', 'name': 'The user\'s name.',
                 'paid_for_service': 'Whether the user is considered up to date on payment.'}),
    'channels': pyarrow.schema([
        pyarrow.field('creator_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
        pyarrow.field('name', pyarrow.string(), False),
        pyarrow.field('primary_category', pyarrow.string(), True),
    ]),
    'subscriptions': pyarrow.schema([
        pyarrow.field('user_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
    ]),
    'streams': pyarrow.schema([
        pyarrow.field('stream_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
        pyarrow.field('name', pyarrow.string(), False),
    ]),
    'categories': pyarrow.schema([
        pyarrow.field('category_id', pyarrow.string(), False),
        pyarrow.field('name', pyarrow.string(), False),
        pyarrow.field('language', pyarrow.string(), False),
    ]),
    'watches': pyarrow.schema([
        pyarrow.field('user_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
        pyarrow.field('stream_id', pyarrow.string(), False),
        pyarrow.field('start_time', pyarrow.string(), False),
        pyarrow.field('end_time', pyarrow.string(), True),
    ]),
}


def get_mystream_schema(name: str) -> pyarrow.Schema:
    """Fetches the schema for the table with the requested name."""
    return TABLE_SCHEMAS[name]


# pylint: disable=fixme,line-too-long
def make_users_database():
    """Constructs the users table."""
    fake = Faker()
    rows = []
    # TODO -- Make the number of users, the uniqueness of userids, and the density of paid customers configurable.
    for _ in range(100):
        rows.append({'name': fake.name(),
                     'user_id': f'user{fake.unique.pyint(max_value=999999999):>09}',
                     'paid_for_service': fake.pybool(truth_probability=21)})
    table = pyarrow.Table.from_pylist(rows, schema=get_mystream_schema('users'))
    parquet.write_table(table, 'users.parquet', version='2.4', flavor='spark',
                        compression='NONE')


def create_mystream_database() -> Path:
    """Creates all the tables that make up the mystream database."""
    Faker.seed(9999)
    # Build all the tables in sorted order.
    make_users_database()
    return Path('users.parquet')


def delete_mystream_database() -> None:
    """Deletes all the tables related to the mystream database."""
    for table_name in TABLE_SCHEMAS:
        try:
            Path(table_name + '.parquet').unlink()
        except FileNotFoundError:
            # We don't care if the file doesn't exist.
            pass
