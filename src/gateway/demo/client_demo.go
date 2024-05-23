package main

import (
	"flag"
	"log"

	"github.com/apache/spark-connect-go/v34/client/sql"
)

var (
	remote = flag.String("remote", "sc://localhost:50051",
		"the remote address of Spark Connect server to connect to")
)

func main() {
	flag.Parse()
	spark, err := sql.SparkSession.Builder.Remote(*remote).Build()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}
	defer spark.Stop()

	df, err := spark.Sql("select 'apple' as word, 123 as count union all select 'orange' as word, 456 as count")
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("DataFrame from sql: select 'apple' as word, 123 as count union all select 'orange' as word, 456 as count")
	err = df.Show(100, false)
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	rows, err := df.Collect()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	for _, row := range rows {
		log.Printf("Row: %v", row)
	}

	df, err = spark.Read().Format("parquet").
		Load("file://../../../third-party/tpch/parquet/customer")
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("DataFrame from reading parquet")
	df.Show(100, false)

	err = df.CreateTempView("view1", true, false)
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	df, err = spark.Sql("select count, word from view1 order by count")
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("DataFrame from sql: select count, word from view1 order by count")
	df.Show(100, false)
}
