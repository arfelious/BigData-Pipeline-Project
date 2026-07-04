import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

def main():
    print("Initializing Spark Session...")
    # Initialize Spark Session
    # Using HDFS config fs.defaultFS to set NameNode address
    spark = SparkSession.builder \
        .appName("Olist-Ingestion") \
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    print("Spark Session created successfully.")

    raw_dir = Path("/app/data/raw")
    csv_files = list(raw_dir.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in /app/data/raw!")
        sys.exit(1)

    print(f"There are {len(csv_files)} CSV files to ingest.")

    for csv_file in csv_files:
        table_name = csv_file.stem
        hdfs_path = f"hdfs://namenode:9000/olist/{table_name}"
        print(f"\n--- Ingesting {csv_file.name} ---")
        print(f"Destination HDFS path: {hdfs_path}")
        
        try:
            print(f"Reading {csv_file.name} into DataFrame...")
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiLine", "true") \
                .option("escape", '"') \
                .csv(f"file://{csv_file.resolve()}")
            
            print(f"Row count: {df.count()}")
            print("Schema:")
            df.printSchema()

            print(f"Writing to HDFS in Parquet format...")
            df.write.mode("overwrite").parquet(hdfs_path)
            print(f"Successfully ingested {table_name}.")
        except Exception as e:
            print(f"Error ingesting {csv_file.name}: {e}", file=sys.stderr)
            sys.exit(1)

    print("\nAll datasets successfully ingested into HDFS in Parquet format.")

    spark.stop()

if __name__ == "__main__":
    main()
