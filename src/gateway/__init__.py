# SPDX-License-Identifier: Apache-2.0
"""The spark to substrait gateway server module."""

try:
    from ._version import __version__  # noqa: F401
except ImportError:
    pass

__spark_version__ = "v3.5.1"
__spark_hash__ = "fd86f85e181"
__minimum_spark_version__ = "3.5.0"
