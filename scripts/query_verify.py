import sys
from pyhive import hive

def run_query(cursor, query_name, sql):
    print(f"\n### {query_name}")
    print("```sql\n" + sql.strip() + "\n```\n")
    try:
        cursor.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Format as Markdown Table
        header = " | ".join(cols)
        sep = " | ".join(["---"] * len(cols))
        print(f"| {header} |")
        print(f"| {sep} |")
        for row in rows:
            row_str = " | ".join(str(val) for val in row)
            print(f"| {row_str} |")
    except Exception as e:
        print(f"Error running query '{query_name}': {e}", file=sys.stderr)

def main():
    print("# Query Verification Results")
    print("Connecting to Spark ThriftServer on localhost:10000...")
    try:
        conn = hive.connect(host='localhost', port=10000, username='root')
        cursor = conn.cursor()
        print("Connected successfully!\n")
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    # 1. Monthly Revenue
    monthly_rev_sql = """
    SELECT 
        d.year, 
        d.month_name, 
        ROUND(SUM(p.payment_value), 2) AS monthly_revenue
    FROM default_gold.fact_order_payments p
    JOIN default_gold.dim_date d ON p.order_purchase_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
    LIMIT 5
    """
    run_query(cursor, "Monthly Revenue (First 5 Months)", monthly_rev_sql)

    # 2. Revenue by Product Category
    rev_by_cat_sql = """
    SELECT 
        p.product_category_name_english, 
        ROUND(SUM(i.price), 2) AS total_revenue
    FROM default_gold.fact_order_items i
    JOIN default_gold.dim_products p ON i.product_id = p.product_id
    GROUP BY p.product_category_name_english
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    run_query(cursor, "Revenue by Product Category (Top 5 Categories)", rev_by_cat_sql)

    # 3. Top-Performing Sellers
    top_sellers_sql = """
    SELECT 
        s.seller_id, 
        s.seller_city, 
        s.seller_state, 
        ROUND(SUM(i.price), 2) AS total_sales
    FROM default_gold.fact_order_items i
    JOIN default_gold.dim_sellers s ON i.seller_id = s.seller_id
    GROUP BY s.seller_id, s.seller_city, s.seller_state
    ORDER BY total_sales DESC
    LIMIT 5
    """
    run_query(cursor, "Top-Performing Sellers (Top 5)", top_sellers_sql)

    # 4. Sales by Customer State
    sales_by_state_sql = """
    SELECT 
        c.customer_state, 
        ROUND(SUM(i.price), 2) AS total_sales
    FROM default_gold.fact_order_items i
    JOIN default_gold.dim_customers c ON i.customer_id = c.customer_id
    GROUP BY c.customer_state
    ORDER BY total_sales DESC
    LIMIT 5
    """
    run_query(cursor, "Sales by Customer State (Top 5)", sales_by_state_sql)

    # 5. Average Delivery Time by State
    avg_delivery_time_sql = """
    SELECT 
        c.customer_state, 
        ROUND(AVG(o.delivery_time_days), 2) AS avg_delivery_time_days
    FROM default_gold.fact_orders o
    JOIN default_gold.dim_customers c ON o.customer_id = c.customer_id
    WHERE o.delivery_time_days IS NOT NULL
    GROUP BY c.customer_state
    ORDER BY avg_delivery_time_days ASC
    LIMIT 5
    """
    run_query(cursor, "Average Delivery Time by State (Top 5 Fastest States)", avg_delivery_time_sql)

    # 6. Payment Method Trends
    payment_method_sql = """
    SELECT 
        d.year, 
        d.month,
        d.month_name,
        p.payment_type, 
        COUNT(*) AS tx_count, 
        ROUND(SUM(p.payment_value), 2) AS total_value
    FROM default_gold.fact_order_payments p
    JOIN default_gold.dim_date d ON p.order_purchase_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name, p.payment_type
    ORDER BY d.year, d.month, total_value DESC
    LIMIT 20
    """
    run_query(cursor, "Payment Method Trends (Top 20 Monthly Periods)", payment_method_sql)

    # 7. Average Review Score by Category
    avg_review_score_sql = """
    SELECT 
        p.product_category_name_english, 
        ROUND(AVG(r.review_score), 2) AS avg_review_score,
        COUNT(*) AS total_reviews
    FROM default_gold.fact_order_reviews r
    JOIN default_gold.fact_order_items i ON r.order_id = i.order_id
    JOIN default_gold.dim_products p ON i.product_id = p.product_id
    GROUP BY p.product_category_name_english
    ORDER BY avg_review_score DESC
    LIMIT 5
    """
    run_query(cursor, "Average Review Score by Category (Top 5 highest rated with review counts)", avg_review_score_sql)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
