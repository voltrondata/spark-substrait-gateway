# SPDX-License-Identifier: Apache-2.0
import logging
import random
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from gateway.config import DEFAULT_CERT_FILE, DEFAULT_KEY_FILE

_LOGGER = logging.getLogger(__name__)


def _gen_cryptography():
    """Generate a self-signed certificate using the cryptography library."""

    # Generate RSA private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    # Generate X.509 certificate
    subject = x509.Name([
        x509.NameAttribute(oid=x509.NameOID.COMMON_NAME, value=socket.gethostname())
    ])

    builder = x509.CertificateBuilder().subject_name(
        name=subject
    ).issuer_name(
        name=subject
    ).public_key(
        key=private_key.public_key()
    ).serial_number(
        number=random.getrandbits(64)
    ).not_valid_before(
        time=datetime.now(tz=timezone.utc)
    ).not_valid_after(
        time=datetime.now(tz=timezone.utc) + timedelta(days=5 * 365)
    ).add_extension(
        extval=x509.SubjectAlternativeName([
            x509.DNSName(value=socket.gethostname()),
            x509.DNSName(value=f"*.{socket.gethostname()}"),
            x509.DNSName(value="localhost"),
            x509.DNSName(value="*.localhost"),
        ]),
        critical=False
    ).add_extension(
        extval=x509.BasicConstraints(ca=False, path_length=None),
        critical=True,
    )

    certificate = builder.sign(
        private_key=private_key, algorithm=hashes.SHA256(), backend=default_backend()
    )

    # Convert the certificate and private key to PEM format
    cert_pem = certificate.public_bytes(encoding=serialization.Encoding.PEM)
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )

    return cert_pem, private_key_pem


def gen_self_signed_cert():
    """Returns (cert, key) as ASCII PEM strings"""
    return _gen_cryptography()


def create_tls_keypair(cert_file: str = DEFAULT_CERT_FILE,
                       key_file: str = DEFAULT_KEY_FILE,
                       overwrite: bool = False
                       ):
    """Create a self-signed TLS key pair and write to disk."""
    cert_file_path = Path(cert_file)
    key_file_path = Path(key_file)

    if cert_file_path.exists() or key_file_path.exists():
        if not overwrite:
            raise RuntimeError(
                f"The TLS Cert file(s): '{cert_file_path.as_posix()}' or "
                f"'{key_file_path.as_posix()}' - exist - and overwrite is False, aborting."
            )

        cert_file_path.unlink(missing_ok=True)
        key_file_path.unlink(missing_ok=True)

    cert, key = gen_self_signed_cert()

    cert_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file=cert_file_path, mode="wb") as cert_file:
        cert_file.write(cert)

    key_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file=key_file_path, mode="wb") as key_file:
        key_file.write(key)

    _LOGGER.info(msg="Created TLS Key pair successfully.")
    _LOGGER.info(msg=f"Cert file path: {cert_file_path.as_posix()}")
    _LOGGER.info(msg=f"Key file path: {key_file_path.as_posix()}")


@click.command()
@click.option(
    "--cert-file",
    type=str,
    default=DEFAULT_CERT_FILE,
    required=True,
    help="The TLS certificate file to create."
)
@click.option(
    "--key-file",
    type=str,
    default=DEFAULT_KEY_FILE,
    required=True,
    help="The TLS key file to create."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the cert/key if they exist?"
)
def click_create_tls_keypair(cert_file: str,
                             key_file: str,
                             overwrite: bool
                             ):
    """Provides a click interface to create a self-signed TLS key pair."""
    create_tls_keypair(**locals())


if __name__ == '__main__':
    click_create_tls_keypair()
