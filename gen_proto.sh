#!/usr/bin/env bash

set -eou pipefail

namespace=proto
submodule_dir=./third_party/spark
src_dir="$submodule_dir"/connector/connect/common/src/main/protobuf
dest_dir=./src

# Remove the old python protobuf files
rm -rf "$dest_dir/spark/connect"
mkdir -p "$dest_dir"

# Generate the new python protobuf files
ls "$src_dir"/spark/connect/*.proto | xargs python -m grpc_tools.protoc -I"$src_dir" --python_out="$dest_dir" --grpc_python_out="$dest_dir" --mypy_out="$dest_dir"

# Remove the grpc files with nothing in them.
ls "$dest_dir"/spark/connect/*grpc.py | grep -v base_pb2 | xargs rm
