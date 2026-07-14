import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Olist-Duplicate-Check") \
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    print("Spark Session created successfully.")

    # Dictionary mapping each table to its expected primary/unique key(s)
    tables_to_check = {
        "silver_customers": ["customer_id"],
        "silver_geolocation": None,  # Check overall row duplicates (no primary key)
        "silver_order_items": ["order_id", "order_item_id"],
        "silver_order_payments": ["order_id", "payment_sequential"],
        "silver_order_reviews": ["review_id"],
        "silver_orders": ["order_id"],
        "silver_products": ["product_id"],
        "silver_sellers": ["seller_id"],
        "product_category_name_translation": ["product_category_name"],
    }

    print("\n" + "="*60)
    print("STARTING DUPLICATE ANALYSIS")
    print("="*60)

    for table, keys in tables_to_check.items():
        hdfs_path = f"hdfs://namenode:9000/silver/{table}"
        print(f"\nAnalyzing table: {table}")
        print(f"HDFS Path: {hdfs_path}")

        try:
            # Read parquet from HDFS
            df = spark.read.parquet(hdfs_path)
            
            # Row counts
            total_count = df.count()
            distinct_count = df.distinct().count()
            row_duplicates = total_count - distinct_count

            print(f"  Total Rows:           {total_count}")
            print(f"  Distinct Rows:        {distinct_count}")
            print(f"  Exact Duplicate Rows: {row_duplicates}")

            if keys:
                print(f"  Checking unique key constraint on: {keys}")
                # Group by keys to count occurrences
                key_counts = df.groupBy(keys).count()
                duplicate_keys_df = key_counts.filter("count > 1")
                duplicate_key_count = duplicate_keys_df.count()

                if duplicate_key_count > 0:
                    print(f"  ⚠ FOUND {duplicate_key_count} duplicated primary key values!")
                    print("  Sample duplicates:")
                    duplicate_keys_df.orderBy(F.desc("count")).show(5, truncate=False)
                else:
                    print("  No duplicate keys found. Primary key constraint holds.")
            else:
                print("No primary key specified for this table. Checked row-level duplicates only.")

        except Exception as e:
            print(f"Error analyzing {table}: {e}", file=sys.stderr)

    print("\n" + "="*60)
    print("Duplicate analysis complete")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    main()
