#!/bin/bash

set -e

docker login
docker buildx build --tag prmoorevoltron/spark-substrait-gateway:latest --platform linux/amd64,linux/arm64 --push .
