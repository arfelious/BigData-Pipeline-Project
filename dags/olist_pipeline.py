"""
olist_pipeline.py
================================================================
Orchestrates the full data flow:
  1. ingest_to_bronze  : Spark job: reads CSVs, writes Parquet to HDFS (Bronze)
  2. transform_dbt     : dbt run: applies Silver + Gold SQL models via ThriftServer
  3. refresh_superset  : HTTP call: invalidates Superset chart cache so the
                         dashboards immediately reflect this specific pipeline run
                         (not stale results from before the run started).

Schedule: `0 2 * * *`, daily at 02:00 UTC.
  The pipeline is scheduled to run daily at 02:00 UTC to automate ingestion and transformation.
  As this is a static dataset, manual trigger could be configured via `schedule_interval=None`.

Catchup: `False` as only the latest run matters, and no historical backfills are needed.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "olist-team",
    "depends_on_past": False,
    # Retry once after a 5-minute wait before declaring failure
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Environment / path constants
# (These can be overridden via Airflow Variables without touching the DAG)
# ---------------------------------------------------------------------------
SPARK_MASTER   = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
APP_PATH       = os.getenv("SPARK_APP_PATH",   "/opt/airflow/app")
DBT_DIR        = os.getenv("DBT_PROJECT_DIR",  "/opt/airflow/dbt_project")
DBT_PROFILES   = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt_project")

# Superset connection
SUPERSET_URL   = os.getenv("SUPERSET_URL",  "http://superset:8088")
SUPERSET_USER  = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASS  = os.getenv("SUPERSET_PASS", "admin")

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="olist_pipeline",
    description="End-to-end Olist data pipeline: Bronze ingest → dbt Silver/Gold → Superset refresh",
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    # schedule_interval=None,          # Alternative: manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["olist", "spark", "dbt", "superset"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Bronze Ingestion via Spark
    # -----------------------------------------------------------------------
    # Operator choice: BashOperator wrapping spark-submit
    #   • SparkSubmitOperator is an alternative but requires the
    #     apache-airflow-providers-apache-spark package and a preconfigured
    #     Spark connection object in Airflow.  BashOperator keeps the
    #     dependency surface minimal and makes the exact command fully visible.
    #
    # Resource config:
    #   --executor-memory 2g    → each executor gets 2 GB heap
    #   --executor-cores 2      → 2 CPU cores per executor
    #   --num-executors 1       → one executor (single worker node)
    #   local[2] fallback       → if no cluster, run with 2 local threads
    #
    # Boundaries:
    #   INPUT  : CSV files at /app/data/raw/  (mounted volume)
    #   OUTPUT : HDFS hdfs://namenode:9000/olist/<table>  (Parquet, Bronze layer)
    # -----------------------------------------------------------------------
    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=(
            "docker exec spark-master /spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--executor-memory 2g "
            "--executor-cores 2 "
            "--num-executors 1 "
            "/app/processing/ingest.py"
        ),
        # Give Spark up to 30 minutes; ingestion of 9 CSVs is typically 5-10 min in practice
        execution_timeout=timedelta(minutes=30),
        # NOTE on write mode in ingest.py:
        #   Current mode: overwrite (rewrites all 9 Bronze tables from scratch on every run).
        #   This is correct for a static dataset (idempotent: same input always produces same output).
        #   In a streaming/incremental pipeline, this would have been used instead:
        #     df.write.mode("append")                         # append new records only
        #     df.write.partitionBy("year", "month")           # partition for efficient time-range reads
        #  and only CSV files that have not already been processed would be ingested
    )

    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # Task 2: Engine Selection and Silver + Gold Transformation via PySpark or dbt
    # -----------------------------------------------------------------------
    def choose_transformation_engine(**kwargs):
        engine = Variable.get("transformation_engine", default_var="dbt").strip().lower()
        if engine == "pyspark":
            return "transform_pyspark"
        return "transform_dbt"

    choose_engine = BranchPythonOperator(
        task_id="choose_transformation_engine",
        python_callable=choose_transformation_engine,
    )

    transform_pyspark = BashOperator(
        task_id="transform_pyspark",
        bash_command=(
            f"docker exec spark-master /spark/bin/spark-submit --master {SPARK_MASTER} /app/processing/transform_silver.py && "
            f"docker exec spark-master /spark/bin/spark-submit --master {SPARK_MASTER} /app/processing/transform_gold.py"
        ),
        # Allow up to 45 minutes for the full PySpark transformations
        execution_timeout=timedelta(minutes=45),
    )

    transform_dbt = BashOperator(
        task_id="transform_dbt",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES} --no-partial-parse && "
            f"dbt test --profiles-dir {DBT_PROFILES} --no-partial-parse"
        ),
        # Allow up to 45 minutes for the full dbt run across all models
        execution_timeout=timedelta(minutes=45),
    )

    # -----------------------------------------------------------------------
    # Task 3 : Superset Dashboard Cache Refresh
    # -----------------------------------------------------------------------
    # Operator choice: BashOperator with curl
    #   • Superset caches each chart's query result in Redis so dashboards
    #     load instantly for users without hitting Spark on every page open.
    #   • The cache has no automatic expiry tied to our pipeline: it persists
    #     until explicitly invalidated OR until a configured TTL expires.
    #   • Because this DAG is scheduled daily, the cache is invalidated
    #     each time the pipeline executes, ensuring dashboards display
    #     fresh data after the daily transformation completes.
    #
    # Implementation:
    #   • Two sequential HTTP calls are needed:
    #       1. POST /api/v1/security/login  → obtain a short-lived JWT token
    #       2. POST /api/v1/caches/invalidate → drop all cached chart results
    #   • SimpleHttpOperator is an alternative but passing the JWT token
    #     between two separate tasks requires XComs; a single BashOperator
    #     with curl keeps both steps in one atomic shell script.
    #
    # Boundaries:
    #   INPUT  : Superset REST API at SUPERSET_URL
    #   OUTPUT : All chart caches dropped; next dashboard visit re-queries Spark
    # -----------------------------------------------------------------------
    refresh_superset = BashOperator(
        task_id="refresh_superset",
        bash_command="""
            set -e
            COOKIE_FILE=$(mktemp)

            # 1. Login to get JWT access token and session cookie
            TOKEN=$(curl -s -f -c "$COOKIE_FILE" -X POST \
                -H "Content-Type: application/json" \
                -d '{"username":"{{ var.value.superset_user }}", "password":"{{ var.value.superset_pass }}", "provider":"db"}' \
                {{ var.value.superset_url }}/api/v1/security/login \
                | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

            # 2. Get CSRF token
            CSRF_TOKEN=$(curl -s -f -b "$COOKIE_FILE" -c "$COOKIE_FILE" \
                -H "Authorization: Bearer $TOKEN" \
                {{ var.value.superset_url }}/api/v1/security/csrf_token/ \
                | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")

            # 3. Invalidate caches for all datasets (using their UIDs)
            curl -s -f -X POST \
                -b "$COOKIE_FILE" \
                -H "Authorization: Bearer $TOKEN" \
                -H "X-CSRF-Token: $CSRF_TOKEN" \
                -H "Content-Type: application/json" \
                -d '{"datasource_uids": ["1__table", "2__table", "3__table", "4__table", "5__table", "6__table", "7__table", "8__table", "9__table", "10__table", "11__table", "12__table", "13__table", "14__table", "15__table", "16__table", "17__table", "18__table", "22__table", "24__table"]}' \
                {{ var.value.superset_url }}/api/v1/cachekey/invalidate

            rm -f "$COOKIE_FILE"
            echo "Superset cache refreshed successfully."
        """,
        # superset_url / superset_user / superset_pass must be set as Airflow Variables
        execution_timeout=timedelta(minutes=5),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # -----------------------------------------------------------------------
    # Alternative: Native Airflow HttpOperator cache refresh
    # -----------------------------------------------------------------------
    # (Suitable for production environments or non-static pipelines where
    #  XCom state passing is preferred and container security restricts CLI curl)
    #
    # import json
    #
    # get_superset_token = SimpleHttpOperator(
    #     task_id="get_superset_token",
    #     http_conn_id="superset_default",  # HTTP Connection preconfigured in Airflow
    #     endpoint="/api/v1/security/login",
    #     method="POST",
    #     headers={"Content-Type": "application/json"},
    #     data=json.dumps({
    #         "username": SUPERSET_USER,
    #         "password": SUPERSET_PASS,
    #         "provider": "db"
    #     }),
    #     response_filter=lambda response: response.json()["access_token"],
    #     do_xcom_push=True,
    # )
    #
    # invalidate_superset_cache = SimpleHttpOperator(
    #     task_id="invalidate_superset_cache",
    #     http_conn_id="superset_default",
    #     endpoint="/api/v1/caches/invalidate",
    #     method="POST",
    #     headers={
    #         "Content-Type": "application/json",
    #         "Authorization": "Bearer {{ ti.xcom_pull(task_ids='get_superset_token') }}"
    #     },
    #     data=json.dumps({"chart_ids": [], "dashboard_ids": []}),
    # )
    #
    # dbt_task >> get_superset_token >> invalidate_superset_cache
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # Task dependency chain
    # -----------------------------------------------------------------------
    ingest_to_bronze >> choose_engine
    choose_engine >> [transform_dbt, transform_pyspark] >> refresh_superset
