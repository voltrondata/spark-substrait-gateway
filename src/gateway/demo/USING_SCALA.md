# Using Scala

The gateway is language agnostic so you can use any Spark Connect client with it.  The following
provides details on how to use Scala.

Note that User Defined Functions (UDFs) are not supported by the gateway.


## Against Spark

Use ```spark-shell``` with no options and pass in your Scala code.


## Against the Gateway

The only Spark Connect tool for command line use is spark-connect-repl.  After installing it
(it's not part of the Spark distribution) you can simply pass in the location of the gateway.
After it starts up pass in the same Scala code you used above.

```commandline
SPARK_REMOTE=sc://localhost:50051 spark-connect-repl 
```

## Sample Query (TPC-H query #1)


```commandline
var lineitem = spark.read.parquet("/Users/davids/projects/voltrondata-spark-substrait-gateway/third_party/tpch/parquet/lineitem")

var result = lineitem.filter($"l_shipdate" <= "1998-09-02").groupBy($"l_returnflag", $"l_linestatus").agg(sum($"l_quantity"), sum($"l_extendedprice"), sum($"l_extendedprice" * ($"l_discount" + -1) * -1), sum($"l_extendedprice" * ($"l_discount" + -1) * ($"l_tax" + 1) * -1), avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity")).sort($"l_returnflag", $"l_linestatus") 

result.show()
```
