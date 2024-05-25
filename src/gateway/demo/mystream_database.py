# SPDX-License-Identifier: Apache-2.0
"""Routines to create a fake mystream database for testing."""
import contextlib
import datetime
import os.path
from os import unlink
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

NUMBER_OF_USERS: int = 100
NUMBER_OF_CATEGORIES: int = 100

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
        pa.field('stream_id', pa.string(), False),
        pa.field('start_time', pa.timestamp('us'), False),
        pa.field('end_time', pa.timestamp('us'), True),
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
        for _ in range(NUMBER_OF_USERS):
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


def category_id(category_number: int) -> str | None:
    """Return the category id for the given category number."""
    if category_number is None:
        return None
    return f'category{category_number + 1:>04}'


def make_categories_database():
    fake = Faker(['en_US'])
    if os.path.isfile('categories.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('categories')
    with (pq.ParquetWriter('categories.parquet', schema) as writer):
        for category_number in range(NUMBER_OF_CATEGORIES):
            category_name = ' '.join(fake.words(nb=2, unique=True)).title()
            category_language = fake.word()
            data = [
                pa.array([category_id(category_number)]),
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
        users_file = pq.ParquetFile('users.parquet')
        for batch in users_file.iter_batches():
            for user_id in batch[0]:
                is_creator = fake.pybool(truth_probability=10)
                if not is_creator:
                    continue

                num_channels = fake.random_int(min=1, max=5)
                for _ in range(num_channels):
                    if fake.pybool(truth_probability=90):
                        category_number = fake.pyint(min_value=1, max_value=100)
                    else:
                        # Ten percent of channels have no category.
                        category_number = None
                    data = [
                        pa.array([user_id]),
                        pa.array([f'channel{fake.unique.pyint(max_value=999999999):>09}']),
                        pa.array([' '.join(fake.words(nb=3))]),
                        pa.array([category_id(category_number)]),
                    ]
                    batch = pa.record_batch(data, schema=schema)
                    writer.write_batch(batch)


def rows_from_parquet_file(filename: str):
    """Return the rows from a parquet file."""
    file = pq.ParquetFile(filename)
    for batch in file.iter_batches():
        for row in batch.to_pylist():
            yield row.values()


def sort_parquet_file(filename: str, index: int = 0):
    """Sort a parquet file by the specified column."""
    # TODO -- Make this scale to larger files.
    schema = pq.ParquetFile(filename).schema.to_arrow_schema()
    with pq.ParquetWriter('sorted_' + filename, schema,
                          sorting_columns=[pq.SortingColumn(0)]) as writer:
        for row in sorted(rows_from_parquet_file(filename), key=lambda x: list(x)[index]):
            data = []
            for value in row:
                data.append(pa.array([value]))
            batch = pa.record_batch(data, schema=schema)
            writer.write_batch(batch)
    unlink(filename)
    os.rename('sorted_' + filename, filename)


# pylint: disable=fixme
def make_subscriptions_database():
    """Construct the subscriptions table."""
    fake = Faker(['en_US'])
    if os.path.isfile('subscriptions.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('subscriptions')
    # First create an intermediate file with the channels and intended users to join against.
    temp_schema = pa.schema([
        pa.field('user_number', pa.int64(), False),
        pa.field('channel_id', pa.string(), False),
    ])
    with pq.ParquetWriter('temporary_subscriptions.parquet', temp_schema) as writer:
        channels_file = pq.ParquetFile('channels.parquet')
        for batch in channels_file.iter_batches():
            for channel_id in batch.column(1):
                num_subscriptions = fake.random_int(min=0, max=3)
                for _ in range(num_subscriptions):
                    data = [
                        pa.array([fake.random_int(min=0, max=NUMBER_OF_USERS - 1)]),
                        pa.array([channel_id]),
                    ]
                    batch = pa.record_batch(data, schema=temp_schema)
                    writer.write_batch(batch)
    # Sort the temporary file by user number.
    sort_parquet_file('temporary_subscriptions.parquet')
    # Now find the user id for each user number and finish writing the subscriptions table.
    users_iterator = enumerate(rows_from_parquet_file('users.parquet'))
    last_user_number, last_user_row = next(users_iterator)
    with pq.ParquetWriter('subscriptions.parquet', schema) as writer:
        for user_number, channel_id in rows_from_parquet_file('temporary_subscriptions.parquet'):
            while user_number > last_user_number:
                last_user_number, last_user_row = next(users_iterator)
            data = [
                pa.array([f'subscription{fake.unique.pyint(max_value=999999999):>09}']),
                pa.array([list(last_user_row)[0]]),
                pa.array([channel_id]),
            ]
            batch = pa.record_batch(data, schema=schema)
            writer.write_batch(batch)
    unlink('temporary_subscriptions.parquet')


def make_streams_database():
    """Construct the streams table."""
    fake = Faker(['en_US'])
    if os.path.isfile('streams.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('streams')
    with pq.ParquetWriter('streams.parquet', schema) as writer:
        channels_file = pq.ParquetFile('channels.parquet')
        for batch in channels_file.iter_batches():
            for channel_id in batch[1]:
                num_streams = fake.random_int(min=1, max=5)
                for _ in range(num_streams):
                    data = [
                        pa.array([f'stream{fake.unique.pyint(max_value=999999999):>09}']),
                        pa.array([channel_id]),
                        pa.array([' '.join(fake.words(nb=3))]),
                    ]
                    batch = pa.record_batch(data, schema=schema)
                    writer.write_batch(batch)


def make_watches_database():
    """Construct the watches table."""
    fake = Faker(['en_US'])
    if os.path.isfile('watches.parquet'):
        # The file already exists.
        return
    schema = get_mystream_schema('watches')
    with pq.ParquetWriter('watches.parquet', schema) as writer:
        streams_file = pq.ParquetFile('streams.parquet')
        for batch in streams_file.iter_batches():
            for stream_id in batch[0]:
                num_watches = fake.random_int(min=0, max=10)
                for _ in range(num_watches):
                    start_time = fake.date_time_between(start_date=datetime.datetime(2024, 5, 5),
                                                        end_date=datetime.datetime(2024, 5, 6))
                    duration = fake.pyint(min_value=0, max_value=14400000)
                    end_time = start_time + datetime.timedelta(milliseconds=duration)
                    if end_time > datetime.datetime(2024, 5, 6):
                        end_time = None
                    data = [
                        pa.array([f'watch{fake.unique.pyint(max_value=999999999):>09}']),
                        pa.array([f'user{fake.unique.pyint(max_value=999999999):>09}']),
                        pa.array([stream_id]),
                        pa.array([start_time]),
                        pa.array([end_time]),
                    ]
                    batch = pa.record_batch(data, schema=schema)
                    writer.write_batch(batch)


def create_mystream_database() -> None:
    """Create all the tables that make up the mystream database."""
    Faker.seed(9999)

    make_users_database()
    make_categories_database()
    make_channels_database()
    make_subscriptions_database()
    make_streams_database()
    make_watches_database()


def delete_mystream_database() -> None:
    """Delete all the tables related to the mystream database."""
    for table_name in TABLE_SCHEMAS:
        with contextlib.suppress(FileNotFoundError):
            Path(table_name + '.parquet').unlink()
