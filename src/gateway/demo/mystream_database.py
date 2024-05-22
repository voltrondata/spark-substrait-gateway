# SPDX-License-Identifier: Apache-2.0
"""Routines to create a fake mystream database for testing."""
import contextlib
import os.path
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

TABLE_SCHEMAS = {
    'users': pa.schema([
        pa.field('user_id', pa.string(), False),
        pa.field('name', pa.string(), False),
        pa.field('paid_for_service', pa.bool_(), False),
    ], metadata={'user_id': 'A unique user id.', 'name': 'The user\'s name.',
                 'paid_for_service': 'Whether the user is considered up to date on payment.'}),
    'channels': pa.schema([
        pa.field('creator_id', pa.string(), False),
        pa.field('channel_id', pa.string(), False),
        pa.field('channel_name', pa.string(), False),
        pa.field('primary_category', pa.string(), True),
    ]),
    'subscriptions': pa.schema([
        pa.field('subscription_id', pa.string(), False),
        pa.field('user_id', pa.string(), False),
        pa.field('channel_id', pa.string(), False),
    ]),
    'streams': pa.schema([
        pa.field('stream_id', pa.string(), False),
        pa.field('channel_id', pa.string(), False),
        pa.field('stream_name', pa.string(), False),
    ]),
    'categories': pa.schema([
        pa.field('category_id', pa.string(), False),
        pa.field('category_name', pa.string(), False),
        pa.field('language', pa.string(), True),
    ]),
    'watches': pa.schema([
        pa.field('watch_id', pa.string(), False),
        pa.field('user_id', pa.string(), False),
        pa.field('channel_id', pa.string(), False),
        pa.field('stream_id', pa.string(), False),
        pa.field('start_time', pa.string(), False),
        pa.field('end_time', pa.string(), True),
    ]),
}


def get_mystream_schema(name: str) -> pa.Schema:
    """Fetch the schema for the mystream table with the requested name."""
    return TABLE_SCHEMAS[name]


# pylint: disable=fixme
def make_users_database():
    """Construct the users table."""
    fake = Faker(['en_US'])
    # TODO -- Make the number and uniqueness of userids configurable.
    # TODO -- Make the density of paid customers configurable.
    if os.path.isfile('users.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('users')
    with pq.ParquetWriter('users.parquet', schema) as writer:
        for _ in range(100):
            user_name = fake.name()
            user_id = f'user{fake.unique.pyint(max_value=999999999):>09}'
            user_paid = fake.pybool(truth_probability=21)
            data = [
                pa.array([user_id]),
                pa.array([user_name]),
                pa.array([user_paid]),
            ]
            batch = pa.record_batch(data, schema=schema)
            writer.write_batch(batch)


def create_mystream_database() -> Path:
    """Create all the tables that make up the mystream database."""
    Faker.seed(9999)
    # Build all the tables in sorted order.
    make_users_database()
    return Path('users.parquet')


def delete_mystream_database() -> None:
    """Delete all the tables related to the mystream database."""
    for table_name in TABLE_SCHEMAS:
        with contextlib.suppress(FileNotFoundError):
            Path(table_name + '.parquet').unlink()
