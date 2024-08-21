# SPDX-License-Identifier: Apache-2.0
"""Configuration settings for the gateway."""

from pathlib import Path

# Constants
TLS_DIR = Path("tls")
DEFAULT_CERT_FILE = (TLS_DIR / "server.crt").as_posix()
DEFAULT_KEY_FILE = (TLS_DIR / "server.key").as_posix()
SERVER_PORT = 50051
