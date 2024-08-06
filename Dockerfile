# SPDX-License-Identifier: Apache-2.0
FROM continuumio/miniconda3:24.5.0-0

# Update OS and install packages
RUN <<EOF
    apt-get update --yes
    apt-get dist-upgrade --yes
    apt-get install --yes \
      build-essential \
      curl \
      unzip \
      vim \
      zip
EOF

# Create an application user
RUN useradd app_user --create-home

ARG APP_DIR="/opt/spark_substrait_gateway"
RUN mkdir --parents ${APP_DIR} && \
    chown app_user:app_user ${APP_DIR}

USER app_user

WORKDIR ${APP_DIR}

COPY --chown=app_user:app_user . .

# Install Rust/Cargo - see: https://doc.rust-lang.org/cargo/getting-started/installation.html
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Setup a Python Virtual environment
RUN <<EOF
  set -eux
  # Setup the Python Conda environment
  . ${HOME}/.cargo/env
  conda init bash
  . ~/.bashrc
  conda env create -f environment.yml
EOF

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "spark-substrait-gateway-env", "/bin/bash", "-c"]

RUN pip install .

# Expose the gRPC port
EXPOSE 50051

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "spark-substrait-gateway-env", "python", "src/gateway/server.py"]
