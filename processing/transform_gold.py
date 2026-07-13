import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    print("Initializing Spark Session for Gold Transformation...")
    spark = SparkSession.builder \
        .appName("Olist-Gold-Transformation") \
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Spark Session created successfully.")

    gold_base = "hdfs://namenode:9000/gold"

    print("\n" + "="*60)
    print("STARTING GOLD TRANSFORMATIONS (STAR SCHEMA)")
    print("="*60)

    try:
        # Load necessary Silver tables from the metastore catalog
        print("\nLoading Silver datasets from Hive Metastore...")
        customers_df = spark.read.table("default_silver.silver_customers")
        products_df = spark.read.table("default_silver.silver_products")
        sellers_df = spark.read.table("default_silver.silver_sellers")
        orders_df = spark.read.table("default_silver.silver_orders")
        order_items_df = spark.read.table("default_silver.silver_order_items")
        payments_df = spark.read.table("default_silver.silver_order_payments")
        reviews_df = spark.read.table("default_silver.silver_order_reviews")

        # Ensure database exists
        spark.sql("CREATE DATABASE IF NOT EXISTS default_gold")

        # -------------------------------------------------------------
        # DIMENSIONS
        # -------------------------------------------------------------

        # 1. dim_customers
        print("\nBuilding dim_customers...")
        dim_customers = customers_df.select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        ).distinct()

        # 2. dim_products
        print("Building dim_products...")
        # Silver product table has already joined english translations
        dim_products = products_df.select(
            "product_id",
            "product_category_name",
            F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")).alias("product_category_name_english")
        ).distinct()

        # 3. dim_sellers
        print("Building dim_sellers...")
        dim_sellers = sellers_df.select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        ).distinct()

        # 4. dim_date
        print("Building dim_date...")
        # Determine the date range from the orders dataset and generate sequence natively
        date_range_df = orders_df.select(
            F.coalesce(F.min(F.to_date("order_purchase_timestamp")), F.to_date(F.lit("2016-01-01"))).alias("min_date"),
            F.coalesce(F.max(F.to_date("order_purchase_timestamp")), F.to_date(F.lit("2018-12-31"))).alias("max_date")
        )

        dim_date = date_range_df.withColumn(
            "date",
            F.explode(F.sequence(F.col("min_date"), F.col("max_date"), F.expr("interval 1 day")))
        ).select("date")

        dim_date = dim_date.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")) \
            .withColumn("year", F.year("date")) \
            .withColumn("month", F.month("date")) \
            .withColumn("month_name", F.date_format("date", "MMMM")) \
            .withColumn("quarter", F.quarter("date")) \
            .withColumn("day_of_week", F.dayofweek("date")) \
            .withColumn("day_of_month", F.dayofmonth("date"))

        # -------------------------------------------------------------
        # FACTS
        # -------------------------------------------------------------

        # 1. fact_orders
        print("\nBuilding fact_orders...")
        fact_orders = orders_df.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("order_purchase_date_key"),
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            # Precalculate delivery time in days (floating point)
            ((F.col("order_delivered_customer_date").cast("long") - F.col("order_purchase_timestamp").cast("long")) / (24.0 * 3600.0)).alias("delivery_time_days")
        )

        # 2. fact_order_items
        print("Building fact_order_items...")
        # Needs customer_id and order_purchase_date_key from orders
        orders_lookup = orders_df.select(
            "order_id", 
            "customer_id", 
            F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("order_purchase_date_key")
        )
        fact_order_items = order_items_df.join(orders_lookup, on="order_id", how="inner") \
            .select(
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "customer_id",
                "order_purchase_date_key",
                "price",
                "freight_value"
            )

        # 3. fact_order_payments
        print("Building fact_order_payments...")
        fact_order_payments = payments_df.join(orders_lookup, on="order_id", how="inner") \
            .select(
                "order_id",
                "payment_sequential",
                "order_purchase_date_key",
                "payment_type",
                "payment_installments",
                "payment_value"
            )

        # 4. fact_order_reviews
        print("Building fact_order_reviews...")
        fact_order_reviews = reviews_df.select(
            "review_id",
            "order_id",
            "review_score",
            "review_creation_date"
        )

        gold_tables = {
            "dim_customers": dim_customers,
            "dim_products": dim_products,
            "dim_sellers": dim_sellers,
            "dim_date": dim_date,
            "fact_orders": fact_orders,
            "fact_order_items": fact_order_items,
            "fact_order_payments": fact_order_payments,
            "fact_order_reviews": fact_order_reviews
        }

        print("\nWriting tables to Gold layer HDFS...")
        for table_name, df in gold_tables.items():
            path = f"{gold_base}/{table_name}"
            print(f"  Writing {table_name} to {path}...")
            df.write.mode("overwrite") \
                .option("path", path) \
                .saveAsTable(f"default_gold.{table_name}")
            print(f"  Successfully wrote {table_name}.")

        print("\n" + "="*60)
        print("GOLD TRANSFORMATIONS (STAR SCHEMA) COMPLETE")
        print("="*60)

    except Exception as e:
        print(f"Error during Gold transformations: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
