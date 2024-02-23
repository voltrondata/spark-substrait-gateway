#!/usr/bin/env bash

set -eou pipefail

namespace=proto
submodule_dir=./third_party/spark
src_dir="$submodule_dir"/connector/connect/common/src/main/protobuf/spark/connect
tmp_dir=./buf_work_dir
dest_dir=./src/spark/connect

mkdir -p "$tmp_dir/spark"
cp -r "$src_dir" "$tmp_dir/spark"

# Remove the old python protobuf files
rm -rf "$dest_dir"
mkdir -p "$dest_dir"

# Generate the new python protobuf files
buf generate
protol --in-place --create-package --python-out "$dest_dir" buf

# Remove the temporary work dir
rm -rf "$tmp_dir"
