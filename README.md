# Big Data Analytics Pipeline — Olist E-Commerce

A hands-on big data project built around the
[Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) —
~100,000 real orders from Brazil's largest online marketplace (2016–2018).

The project is developed in stages. Each stage adds a new layer to the pipeline. More stages will be added over time.

---

## Automated Startup & Query Execution

Convenient automation scripts have been provided to start up the entire pipeline environment and run the business analytical queries.

**1. Start the Stack (HDFS, Spark, Metastore, Superset, Airflow)**

```bash
bash scripts/start_all.sh
```

This script automatically creates the shared network, spins up the docker containers in the correct dependency order, and waits for the Spark ThriftServer to become healthy.

Once finished, the web interfaces are accessible at:
- **HDFS NameNode** : http://localhost:9870
- **Spark Master**  : http://localhost:8080
- **Apache Superset** : http://localhost:8088  (credentials: `admin` / `admin`)
- **Apache Airflow**  : http://localhost:8090  (credentials: `admin` / `admin`)

**2. Execute the Analytical queries**

To query the built Gold Star Schema on the Spark ThriftServer and answer all the Olist business analytical questions, run:

```bash
bash scripts/run_queries.sh
```

**3. Stop the stack**

To stop all running services and clean up resources, run:

```bash
docker compose -f docker/docker-compose-airflow.yml down
docker compose -f docker/docker-compose-superset.yml down
docker compose -f docker/docker-compose-spark.yml down
docker compose -f docker/docker-compose-hive.yml down
docker compose -f docker/docker-compose-hdfs.yml down
```

---

## 📌 Important Notes

### Submission
Each student must **fork or clone this repository**, implement their solution, and submit by **opening a Pull Request (PR) back to this repository** with their completed work. PRs are the only accepted submission method.

### Docker is Optional
The Docker Compose files and scripts provided in this repo are **starter code only** — a reference setup to help you get up and running quickly. You are **not required** to use Docker. Feel free to run HDFS, Spark, and Superset however you prefer (local install, cloud, a different container setup, etc.), as long as the pipeline works end-to-end.

---

## Architecture (Phase 1)

```
[Olist Dataset — 9 CSV Tables]
        |
        v
[Apache Spark]
  · Reads CSVs
  · Writes Parquet
        |
        v
[HDFS or MinIO]
        |
        v
[Apache Superset — Simple Charts]
```

---


## Phases

### ✅ Phase 1 — Ingest & Visualize

> **Current task**

- Download the Olist dataset (9 CSV tables).
- Import all CSVs into **HDFS or MinIO** in **Parquet format** using Apache Spark.
- Connect Apache Superset to the stored data and create a few simple charts/diagrams.

No advanced transformations are required for this phase.

---

### 🔜 Phase 2 — Coming Soon

Details will be announced.

---

## Docker Quick Start (Optional)

The following commands use the provided Docker Compose files as a starting point.

**1. Create the shared network**

```bash
# Linux / macOS
bash scripts/setup_network.sh

# Windows (PowerShell)
.\scripts\setup_network.ps1
```

**2. Start the services**

```bash
docker compose -f docker/docker-compose-hdfs.yml up -d
docker compose -f docker/docker-compose-spark.yml up -d
docker compose -f docker/docker-compose-superset.yml up -d
```

| Service         | URL                       | Credentials   |
|-----------------|---------------------------|---------------|
| HDFS NameNode   | http://localhost:9870     |               |
| Spark Master    | http://localhost:8080     |               |
| Superset        | http://localhost:8088     | admin / admin |

**3. Stop everything**

```bash
docker compose -f docker/docker-compose-superset.yml down
docker compose -f docker/docker-compose-spark.yml down
docker compose -f docker/docker-compose-hdfs.yml down
```
