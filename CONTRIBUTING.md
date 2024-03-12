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
