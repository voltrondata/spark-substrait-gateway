package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/spark-connect-go/v34/client/sql"
)

var (
	remote = flag.String("remote", "sc://localhost:50051",
		"the remote address of Spark Connect server to connect to")
)

func find_source_root() string {
	current_location, _ := os.Getwd()
	source_root := current_location
	_, err := os.OpenFile(source_root+"/CONTRIBUTING.md", os.O_RDONLY, 0)
	for errors.Is(err, os.ErrNotExist) {
		if source_root == "/" || source_root == "" {
			return "/"
		}
		source_root = filepath.Clean(filepath.Join(source_root, ".."))
		_, err = os.OpenFile(source_root+"/CONTRIBUTING.md", os.O_RDONLY, 0)
	}
	return source_root
}

func main() {
	flag.Parse()
	spark, err := sql.SparkSession.Builder.Remote(*remote).Build()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}
	defer spark.Stop()

	df, err := spark.Read().Format("parquet").
		Load(fmt.Sprintf("%s/%s", find_source_root(), "users.parquet"))
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("DataFrame from reading parquet")
	df.Show(100, false)

	log.Printf("Creating temporary view")
	err = df.CreateTempView("users", true, false)
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("Executing SQL query")
	df, err = spark.Sql("select name, paid_for_service from users order by user_id")
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("DataFrame from sql: elect name, paid_for_service from users order by user_id")
	df.Show(100, false)

	log.Printf("Collecting results")
	rows, err := df.Collect()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("Showing rows")
	for _, row := range rows {
		log.Printf("Row: %v", row)
	}

}
