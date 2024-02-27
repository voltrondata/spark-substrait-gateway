# SPDX-License-Identifier: Apache-2.0
try:
    from ._version import __version__  # noqa: F401
except ImportError:
    pass

__spark_version__ = "0.42.1"
__spark_hash__ = "4734478"
__minimum_spark_version__ = "0.30.0"
