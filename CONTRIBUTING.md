# Getting Started
## Get the repo
Fork and clone the repo.
```
git clone --recursive https://github.com/<your-fork>/spark-substrait-gateway.git
cd spark-substrait-gateway
```

## Conda env
Create a conda environment with developer dependencies.
```
conda env create -f environment.yml
conda activate spark-substrait-gateway-env
```

## Update the third_party submodules locally
This might be necessary if you are updating an existing checkout.
```
git submodule sync --recursive
git submodule update --init --recursive
```


# Upgrade the spark submodule

## a) Automatic upgrade using the update script

Run the update script to upgrade the submodule and regenerate the protobuf stubs.

```
./update.sh <version>
```

## b) Manual upgrade

### Upgrade the Spark submodule

```
cd third_party/spark
git checkout <version>
cd -
git commit . -m "Use submodule <version>"
```

### Generate protocol buffers
Generate the protobuf files manually. Requires protobuf `v3.20.1`.
```
./gen_proto.sh
```


# Build
## Python package
Editable installation.
```
pip install -e .
```

# Test
Run tests in the project's root dir.
```
pytest
```
