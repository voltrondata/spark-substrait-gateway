# SPDX-License-Identifier: Apache-2.0
"""Creates the data files used by the demos."""
from mystream_database import create_mystream_database, delete_mystream_database

if __name__ == '__main__':
    delete_mystream_database()
    create_mystream_database()
