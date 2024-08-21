# Spark to Substrait Gateway

**[Apache Spark](https://github.com/apache/spark)** is a popular unified analytics engine for
large-scale data processing. However being unified makes it difficult for it to participate in a
composable data system. That's where the Spark to Substrait gateway comes in. By implementing the
SparkConnect protocol a backend using Substrait can be seamlessly dropped in for Apache Spark.

That said, there are some caveats. The gateway doesn't yet support all features and capabilities of
SparkConnect. And for the features it does support it doesn't support them with the same semantics
that Spark does (because whatever backend you want to plug in likely uses different semantics.)   So
please test your workloads to ensure they behave as expected.

## Alternatives to the Gateway

There are other projects that also use Substrait to enhance Apache Spark.
The **[Gluten project](https://github.com/oap-project/gluten)** exposes
the pushdown Substrait used in pipelines (think the join free parts of a plan) to either Clickhouse
or Velox.

## Starting the Gateway

To use the gateway simply start the gateway server:

```commandline
python src/gateway/server.py
```

This will start the service on local port 50051 (this is not currently configurable).

## Connecting to the Gateway

Here's how to use PySpark to connect to the running gateway:

```python
from pyspark.sql import SparkSession

# Create a Spark Connect session to local port 50051.
spark = (SparkSession.builder
         .remote("sc://localhost:50051/;use_ssl=true")
         .getOrCreate()
         )

# Use the spark session normally.
```

You'll find that the [usage examples on the Spark website](https://spark.apache.org/docs/latest/spark-connect-overview.html#use-spark-connect-in-standalone-applications) also apply to the gateway without any modification.  You may also run the provided demo located at ```src/gateway/demo/client_demo.py```.
