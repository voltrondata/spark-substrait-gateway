#!/bin/bash

if [[ -z "$1" ]]; then
    echo "Usage: bash update.sh <version>" 1>&2
    echo ""
    echo "Example: bash update.sh v0.1.0" 1>&2
    exit 1
fi

SPARK_VERSION=$1

echo "Updating spark submodule..."
git submodule update --remote third_party/spark

DIR=$(cd "$(dirname "$0")" && pwd)
pushd "${DIR}"/third_party/spark/ || exit
git checkout "$SPARK_VERSION"
SPARK_HASH=$(git rev-parse --short HEAD)
popd || exit

VERSION=${VERSION//v/}

sed -i "s#__spark_hash__.*#__spark_hash__ = \"$SPARK_HASH\"#g" src/gateway/__init__.py
sed -i "s#__spark_version__.*#__spark_version__ = \"$SPARK_VERSION\"#g" src/gateway/__init__.py

./gen_proto.sh
