import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Olist-Silver-Transformation") \
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    print("Spark Session created successfully.")

    bronze_base = "hdfs://namenode:9000/olist"
    silver_base = "hdfs://namenode:9000/silver"

    # Define all tables
    tables = [
        "olist_customers_dataset",
        "olist_geolocation_dataset",
        "olist_order_items_dataset",
        "olist_order_payments_dataset",
        "olist_order_reviews_dataset",
        "olist_orders_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "product_category_name_translation",
    ]

    print("\n" + "="*60)
    print("STARTING SILVER TRANSFORMATIONS")
    print("="*60)

    # 1. First, we load the translation table because we'll need it to enrich the products table
    print("\nReading category translation table...")
    translation_path = f"{bronze_base}/product_category_name_translation"
    translation_df = spark.read.parquet(translation_path)
    
    # Save category translation as-is to silver
    print("Writing category translation to silver...")
    translation_df.write.mode("overwrite").parquet(f"{silver_base}/product_category_name_translation")

    for table in tables:
        if table == "product_category_name_translation":
            continue  # Already handled

        bronze_path = f"{bronze_base}/{table}"
        silver_path = f"{silver_base}/{table}"
        
        print(f"\n--- Processing {table} ---")
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
                print("Action: No schema transformations needed (preserving schema).")

            # Write to Silver HDFS
            row_count_after = df.count()
            print(f"Rows before: {row_count_before} | Rows after: {row_count_after}")
            print("Writing to HDFS Silver layer in Parquet format...")
            df.write.mode("overwrite").parquet(silver_path)
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
