import pytest


def pytest_addoption(parser):
    parser.addoption(f"--rebuild_goldens", action="store", default=False)
