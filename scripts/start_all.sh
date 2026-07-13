#!/bin/bash
# start_all.sh: Startup orchestrator for all local services
set -e

# Make sure working directory is project root
cd "$(dirname "$0")/.."

echo "===================================================="
echo "Starting Olist Big Data Pipeline Environment..."
echo "===================================================="

# 1. Setup Network
echo -e "\n1. Setting up shared Docker network..."
bash scripts/setup_network.sh

# 2. Start HDFS
echo -e "\n2. Starting HDFS (namenode, datanode)..."
docker compose -f docker/docker-compose-hdfs.yml up -d

# 3. Start Hive Metastore
echo -e "\n3. Starting Hive Metastore & Metastore DB..."
docker compose -f docker/docker-compose-hive.yml up -d

# 4. Start Spark Cluster & ThriftServer
echo -e "\n4. Starting Spark Master, Worker & ThriftServer..."
docker compose -f docker/docker-compose-spark.yml up -d

# 5. Start Apache Superset
echo -e "\n5. Starting Apache Superset (DB, Redis, Web)..."
docker compose -f docker/docker-compose-superset.yml up -d

# 6. Start Apache Airflow
echo -e "\n6. Starting Apache Airflow (DB, Init, Scheduler, Web)..."
docker compose -f docker/docker-compose-airflow.yml up -d

echo -e "\n===================================================="
echo "Waiting for Spark ThriftServer to be ready on port 10000..."
echo "===================================================="

until nc -z localhost 10000; do
  sleep 3
done

echo "Spark ThriftServer is ready!"

echo -e "\n===================================================="
echo "All services started successfully!"
echo "----------------------------------------------------"
echo "- HDFS NameNode Web UI : http://localhost:9870"
echo "- Spark Master Web UI  : http://localhost:8080"
echo "- Apache Superset      : http://localhost:8088  (admin / admin)"
echo "- Apache Airflow Web UI: http://localhost:8090  (admin / admin)"
echo "===================================================="
echo "To execute the business queries, run: bash scripts/run_queries.sh"
echo "===================================================="
