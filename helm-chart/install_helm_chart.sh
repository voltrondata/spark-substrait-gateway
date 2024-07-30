#!/bin/bash

set -e

kubectl config set-context --current --namespace=spark-substrait-gateway

helm upgrade demo \
     --install . \
     --namespace spark-substrait-gateway \
     --create-namespace \
     --values values.yaml
