import os
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Olist-Gold-Queries") \
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    gold_base = "hdfs://namenode:9000/gold"
    tables = [
        "dim_customers",
        "dim_products",
        "dim_sellers",
        "dim_date",
        "fact_orders",
        "fact_order_items",
        "fact_order_payments",
        "fact_order_reviews"
    ]

    print("Registering Spark temp views...")
    for t in tables:
        df = spark.read.parquet(f"{gold_base}/{t}")
        df.createOrReplaceTempView(t)

    queries = {
        "1. Monthly Revenue": """
            SELECT 
                d.year, 
                d.month, 
                d.month_name,
                ROUND(SUM(p.payment_value), 2) AS monthly_revenue
            FROM 
                fact_order_payments p
            JOIN 
                dim_date d ON p.order_purchase_date_key = d.date_key
            GROUP BY 
                d.year, d.month, d.month_name
            ORDER BY 
                d.year, d.month
        """,
        "2. Revenue by Product Category": """
            SELECT 
                p.product_category_name_english,
                ROUND(SUM(i.price), 2) AS category_revenue
            FROM 
                fact_order_items i
            JOIN 
                dim_products p ON i.product_id = p.product_id
            GROUP BY 
                p.product_category_name_english
            ORDER BY 
                category_revenue DESC
            LIMIT 10
        """,
        "3. Top-Performing Sellers": """
            SELECT 
                s.seller_id,
                s.seller_city,
                s.seller_state,
                ROUND(SUM(i.price), 2) AS total_sales
            FROM 
                fact_order_items i
            JOIN 
                dim_sellers s ON i.seller_id = s.seller_id
            GROUP BY 
                s.seller_id, s.seller_city, s.seller_state
            ORDER BY 
                total_sales DESC
            LIMIT 5
        """,
        "4. Sales by Customer State": """
            SELECT 
                c.customer_state,
                ROUND(SUM(i.price), 2) AS total_sales
            FROM 
                fact_order_items i
            JOIN 
                dim_customers c ON i.customer_id = c.customer_id
            GROUP BY 
                c.customer_state
            ORDER BY 
                total_sales DESC
            LIMIT 10
        """,
        "5. Average Delivery Time by State (Days)": """
            SELECT 
                c.customer_state,
                ROUND(AVG(o.delivery_time_days), 2) AS avg_delivery_time_days
            FROM 
                fact_orders o
            JOIN 
                dim_customers c ON o.customer_id = c.customer_id
            WHERE 
                o.delivery_time_days IS NOT NULL AND o.delivery_time_days > 0
            GROUP BY 
                c.customer_state
            ORDER BY 
                avg_delivery_time_days ASC
            LIMIT 10
        """,
        "6. Payment Method Trends": """
            SELECT 
                d.year,
                d.month,
                d.month_name,
                p.payment_type,
                COUNT(*) AS payment_count,
                ROUND(SUM(p.payment_value), 2) AS total_payment_value
            FROM 
                fact_order_payments p
            JOIN 
                dim_date d ON p.order_purchase_date_key = d.date_key
            GROUP BY 
                d.year, d.month, d.month_name, p.payment_type
            ORDER BY 
                d.year, d.month, total_payment_value DESC
        """,
        "7. Average Review Score by Category": """
            SELECT 
                p.product_category_name_english,
                ROUND(AVG(r.review_score), 2) AS avg_review_score,
                COUNT(r.review_score) AS review_count
            FROM 
                fact_order_reviews r
            JOIN 
                fact_order_items i ON r.order_id = i.order_id
            JOIN 
                dim_products p ON i.product_id = p.product_id
            GROUP BY 
                p.product_category_name_english
            ORDER BY 
                avg_review_score DESC, review_count DESC
        """
    }

    for name, q in queries.items():
        print(f"\n{'='*60}\nRunning Query: {name}\n{'='*60}")
        spark.sql(q).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
