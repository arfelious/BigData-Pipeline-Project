# Big Data Analytics Pipeline: Olist E-Commerce Project Report

---

## 1. Project Phases

### Phase 1: Infrastructure & Data Ingest
* **Used Services**: HDFS (storage node), Hive Metastore (schema catalog), Postgres (catalog database), Spark ThriftServer (query execution engine), and Apache Superset (dashboarding and visualization).
* **Unused Service (MinIO)**: MinIO was included in the template as an alternative object store, but was not used. In modern big data architectures, HDFS remains a more active and robust filesystem choice, whereas the Docker templates for MinIO are archive-level and no longer actively maintained in production contexts.
* **Ingestion**: Spark reads the 9 raw CSV files and writes Snappy-compressed Parquet files onto HDFS in a raw "Bronze" directory.

### Phase 2: Core Transformations & Analytics
* **Transformations**: PySpark scripts (`transform_silver.py` and `transform_gold.py`) clean and transform the raw data.
  - **Silver (Cleaning)**: Casts dates to `TIMESTAMP`, left-joins English product categories, and deduplicates the geolocation table.
  - **Gold (Star Schema)**: Resolves dimension lookup tables (`dim_customers`, `dim_products`, `dim_sellers`, `dim_date`) and fact tables (`fact_orders`, `fact_order_items`, `fact_order_payments`, `fact_order_reviews`).
* **Analytical Queries**: Formulates 7 queries to answer business questions (monthly revenue, category trends, top sellers, delivery times, payment types, and review scores).

### Phase 3: Orchestration & Modeling (dbt & Airflow)
* **Apache Airflow**: Automates pipeline execution.
* **dbt (Data Build Tool)**: Replaces custom transformation scripts with modular, testable, and SQL-native modeling.
* **Dynamic Engine Branching**: The DAG supports dynamic engine selection (dbt SQL or PySpark Python) via the Airflow Variable `transformation_engine`.

---

## 2. Apache Airflow Core Components

| Component | Role |
|---|---|
| **DAG** | Directed Acyclic Graph defining task dependency order in Python. |
| **Scheduler** | Continuously monitors and triggers tasks based on schedules and upstream states. |
| **Executor** | Runs tasks (e.g. `LocalExecutor` on the same host, `CeleryExecutor` on workers). |
| **Web Server** | Flask-based UI for monitoring and managing DAGs, logs, and variables. |
| **Metadata DB** | Relational database (Postgres) storing pipeline state and execution logs. |
| **Operator** | Template for tasks (e.g., `BashOperator`, `SimpleHttpOperator`). |
| **Task Instance** | A single execution of an Operator for a specific DAG run and execution date. |

---

## 3. Airflow DAG Workflow & Routing

### Topology
```
                     /──► [ transform_dbt ] ───\
[ ingest_to_bronze ] ──► [ choose_engine ]      ──► [ refresh_superset ]
                     \──► [ transform_pyspark ] ─/
```

* **Ingestion (`ingest_to_bronze`)**: `BashOperator` executing `spark-submit` to convert raw CSVs into Snappy-compressed Parquet on HDFS (`hdfs://namenode:9000/olist/`).
* **Routing (`choose_engine`)**: `BranchPythonOperator` selecting the transformation engine using the Airflow Variable `transformation_engine` (`dbt` or `pyspark`).
* **SQL Transformation (`transform_dbt`)**: `BashOperator` executing `dbt run --no-partial-parse` and `dbt test` to rebuild Silver views and Gold tables.
* **Python Transformation (`transform_pyspark`)**: `BashOperator` running `transform_silver.py` and `transform_gold.py` to clean and register tables via PySpark.
* **Cache Invalidation (`refresh_superset`)**: `BashOperator` invalidating chart caches via the Superset REST API using dynamic JWT and CSRF header tokens.

---

## 4. Medallion Layers & Schema Decisions

* **Bronze (Raw)**: Unmodified Parquet files on HDFS (`/olist`).
* **Silver (Clean & Validate)**:
  - `silver_geolocation`: Deduplicated via `DISTINCT` (eliminates ~260k duplicate rows).
  - `silver_orders`, `silver_order_items`, `silver_order_reviews`: Dates cast from `STRING` to `TIMESTAMP`.
  - `silver_products`: Left-joined with the English category translation table.
* **Gold (Business Aggregations)**:
  - Implements a Star Schema fact/dimension layout for high-performance querying in Superset.
  - Generates `dim_date` calendar table dynamically.

---

## 5. Key Implementation Details

### 5.1 Date Dimension Generation in Spark SQL
To generate a continuous calendar table dynamically, we use Spark's date sequence functions:
```sql
EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY))
```

### 5.2 Metadata Cache Invalidation
Whenever the ingestion task overwrites Bronze Parquet directories, we execute `REFRESH TABLE bronze.<name>` to invalidate Spark's internal file path cache, preventing `FileNotFoundException` errors during compilation.

### 5.3 Metastore Stability
To resolve HDFS concurrent write hangs when materializing physical Gold tables, we disabled the Hadoop file system cache via:
`--conf spark.hadoop.fs.hdfs.impl.disable.cache=true`

---

## 6. ETL vs ELT
This project uses **ELT** (Extract -> Load -> Transform):
1. **Raw Preservation**: original CSVs are loaded verbatim into Bronze HDFS, allowing transformations to be re-run from the source at any time.
2. **Compute Localization**: Spark and dbt execute transformations directly inside the database/metastore layer rather than a separate ETL server.

---

## 7. Star Schema & Business Queries

### Schema Map
* Fact tables: `fact_orders`, `fact_order_items`, `fact_order_payments`, `fact_order_reviews`
* Dimension tables: `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`

### SQL Analytical Queries

#### Q1: Monthly Revenue
```sql
SELECT d.year, d.month_name, ROUND(SUM(p.payment_value), 2) AS monthly_revenue
FROM default_gold.fact_order_payments p
JOIN default_gold.dim_date d ON p.order_purchase_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

#### Q2: Revenue by Product Category
```sql
SELECT p.product_category_name_english, ROUND(SUM(i.price), 2) AS total_revenue
FROM default_gold.fact_order_items i
JOIN default_gold.dim_products p ON i.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC;
```

#### Q3: Top-Performing Sellers
```sql
SELECT s.seller_id, s.seller_city, s.seller_state, ROUND(SUM(i.price), 2) AS total_sales
FROM default_gold.fact_order_items i
JOIN default_gold.dim_sellers s ON i.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY total_sales DESC;
```

#### Q4: Sales by Customer State
```sql
SELECT c.customer_state, ROUND(SUM(i.price), 2) AS total_sales
FROM default_gold.fact_order_items i
JOIN default_gold.dim_customers c ON i.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_sales DESC;
```

#### Q5: Average Delivery Time by State (Days)
```sql
SELECT c.customer_state, ROUND(AVG(o.delivery_time_days), 2) AS avg_delivery_time_days
FROM default_gold.fact_orders o
JOIN default_gold.dim_customers c ON o.customer_id = c.customer_id
WHERE o.delivery_time_days IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_time_days ASC;
```

#### Q6: Payment Method Trends
```sql
SELECT d.year, d.month, d.month_name, p.payment_type, COUNT(*) AS tx_count, ROUND(SUM(p.payment_value), 2) AS total_value
FROM default_gold.fact_order_payments p
JOIN default_gold.dim_date d ON p.order_purchase_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, p.payment_type
ORDER BY d.year, d.month, total_value DESC;
```

#### Q7: Average Review Score by Category
```sql
SELECT p.product_category_name_english, ROUND(AVG(r.review_score), 2) AS avg_review_score, COUNT(*) AS total_reviews
FROM default_gold.fact_order_reviews r
JOIN default_gold.fact_order_items i ON r.order_id = i.order_id
JOIN default_gold.dim_products p ON i.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY avg_review_score DESC, total_reviews DESC;
```
