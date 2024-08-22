#!/bin/bash
# This script starts the Spark Substrait Gateway demo server.
# It will create demo TPC-H (Scale Factor 1GB) data, and start the server.

set -e

if [ $(echo "${GENERATE_CLIENT_DEMO_DATA}" | tr '[:upper:]' '[:lower:]') == "true" ]; then
  echo "Generating client demo TPC-H data..."
  spark-substrait-create-client-demo-data
fi

spark-substrait-gateway-server
