# SPDX-License-Identifier: Apache-2.0
"""Configuration for pytest."""


def pytest_addoption(parser):
    """Adds the rebuild goldens command line option to pytest."""
    parser.addoption("--rebuild_goldens", action="store", default=False)
