import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Olist-Silver-Transformation") \
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Spark Session created successfully.")

    bronze_base = "hdfs://namenode:9000/olist"
    silver_base = "hdfs://namenode:9000/silver"

    # Define table mappings to align with dbt schema names
    table_mappings = {
        "olist_customers_dataset": "silver_customers",
        "olist_geolocation_dataset": "silver_geolocation",
        "olist_order_items_dataset": "silver_order_items",
        "olist_order_payments_dataset": "silver_order_payments",
        "olist_order_reviews_dataset": "silver_order_reviews",
        "olist_orders_dataset": "silver_orders",
        "olist_products_dataset": "silver_products",
        "olist_sellers_dataset": "silver_sellers"
    }

    # Ensure database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS default_silver")

    print("\n" + "="*60)
    print("STARTING SILVER TRANSFORMATIONS")
    print("="*60)

    # 1. Load translation table
    print("\nReading category translation table...")
    translation_path = f"{bronze_base}/product_category_name_translation"
    translation_df = spark.read.parquet(translation_path)
    
    print("Writing category translation to silver...")
    translation_df.write.mode("overwrite") \
        .option("path", f"{silver_base}/product_category_name_translation") \
        .saveAsTable("default_silver.product_category_name_translation")

    for table, target_name in table_mappings.items():
        bronze_path = f"{bronze_base}/{table}"
        silver_path = f"{silver_base}/{target_name}"
        
        print(f"\n--- Processing {table} -> {target_name} ---")
        print(f"Source Bronze path:      {bronze_path}")
        print(f"Destination Silver path: {silver_path}")

        try:
            # Read from Bronze
            df = spark.read.parquet(bronze_path)
            row_count_before = df.count()

            # Apply table-specific transformations
            if table == "olist_geolocation_dataset":
                print("Action: Dropping exact row-level duplicates...")
                df = df.dropDuplicates()

            elif table == "olist_orders_dataset":
                print("Action: Casting order date/time columns to Timestamp...")
                timestamp_cols = [
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
                for col_name in timestamp_cols:
                    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))

            elif table == "olist_order_items_dataset":
                print("Action: Casting shipping limit date to Timestamp...")
                df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))

            elif table == "olist_order_reviews_dataset":
                print("Action: Casting review date/time columns to Timestamp...")
                timestamp_cols = [
                    "review_creation_date",
                    "review_answer_timestamp"
                ]
                for col_name in timestamp_cols:
                    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))

            elif table == "olist_products_dataset":
                print("Action: Joining product category name translation...")
                # Left join with translations
                df = df.join(translation_df, on="product_category_name", how="left")

            else:
                print("Action: No schema transformations needed.")

            # Write to Silver HDFS and register in Metastore
            row_count_after = df.count()
            print(f"Rows before: {row_count_before} | Rows after: {row_count_after}")
            print("Writing to HDFS Silver layer and Hive catalog...")
            df.write.mode("overwrite") \
                .option("path", silver_path) \
                .saveAsTable(f"default_silver.{target_name}")
            print(f"Successfully processed {table}.")

        except Exception as e:
            print(f"Error processing {table}: {e}", file=sys.stderr)
            sys.exit(1)

    print("\n" + "="*60)
    print("SILVER TRANSFORMATIONS COMPLETE")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    main()
