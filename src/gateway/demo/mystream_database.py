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


def make_categories_database():
    fake = Faker(['en_US'])
    if os.path.isfile('categories.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('categories')
    with (pq.ParquetWriter('categories.parquet', schema) as writer):
        for _ in range(100):
            category_id = f'category{fake.unique.pyint(max_value=9999):>04}'
            category_name = ' '.join(fake.words(nb=2, unique=True)).title()
            category_language = fake.word()
            data = [
                pa.array([category_id]),
                pa.array([category_name]),
                pa.array([category_language]),
            ]
            batch = pa.record_batch(data, schema=schema)
            writer.write_batch(batch)


# pylint: disable=fixme
def make_channels_database():
    """Construct the channels table."""
    fake = Faker(['en_US'])
    if os.path.isfile('channels.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('channels')
    with pq.ParquetWriter('channels.parquet', schema) as writer:
        file = pq.ParquetFile('users.parquet')
        for batch in file.iter_batches():
            for user_id in batch[0]:
                is_creator = fake.pybool(truth_probability=10)
                if not is_creator:
                    continue

                num_channels = fake.random_int(min=1, max=5)
                for _ in range(num_channels):
                    data = [
                        pa.array([user_id]),
                        pa.array([f'channel{fake.unique.pyint(max_value=999999999):>09}']),
                        pa.array([' '.join(fake.words(nb=3))]),
                        pa.array([None]),  # TODO -- Generate categories.
                    ]
                    batch = pa.record_batch(data, schema=schema)
                    writer.write_batch(batch)


def create_mystream_database() -> None:
    """Create all the tables that make up the mystream database."""
    Faker.seed(9999)
    # Build all the tables in sorted order.
    make_users_database()
    make_categories_database()
    make_channels_database()


def delete_mystream_database() -> None:
    """Delete all the tables related to the mystream database."""
    for table_name in TABLE_SCHEMAS:
        with contextlib.suppress(FileNotFoundError):
            Path(table_name + '.parquet').unlink()
