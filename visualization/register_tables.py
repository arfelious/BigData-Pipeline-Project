"""
Registers all Olist Parquet tables on HDFS in Apache Superset via the Superset API.

1. Connect to the Spark ThriftServer and CREATE EXTERNAL TABLEs in the Hive metastore pointing at the HDFS Parquet directories.

2. Authenticate with Superset to obtain a JWT access token.

3. Create (or locate existing) a SparkSQL database connection in Superset pointing to the Spark ThriftServer (HiveServer2 on port 10000).

4. For every table in OLIST_TABLES, create a Dataset in Superset.



Env variables (defaults are from docker compose files):
    SUPERSET_URL        (base URL of Superset)                   (default: http://localhost:8088)
    SUPERSET_USER       (admin username)                         (default: admin)
    SUPERSET_PASS       (admin password)                         (default: admin)
    THRIFT_HOST         (ThriftServer host for Superset)         (default: spark-thriftserver)
    THRIFT_PORT         (ThriftServer port)                      (default: 10000)
    HDFS_BASE           (HDFS root path)                         (default: hdfs://namenode:9000/olist)
"""

import os
import sys
import time
import requests


def raise_for_status_verbose(r: requests.Response) -> None:
    """Like raise_for_status() but prints the response body first for debugging."""
    if not r.ok:
        print(f"HTTP {r.status_code} error from {r.url}", file=sys.stderr)
        print(f"Response body: {r.text[:2000]}", file=sys.stderr)
        r.raise_for_status()

# Configuration

SUPERSET_URL  = os.environ.get("SUPERSET_URL",  "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")

THRIFT_HOST        = os.environ.get("THRIFT_HOST",       "spark-thriftserver")
THRIFT_PORT        = int(os.environ.get("THRIFT_PORT", "10000"))

HDFS_BASE     = os.environ.get("HDFS_BASE",     "hdfs://namenode:9000/olist")

DB_NAME = "Olist Spark (HDFS)"

OLIST_TABLES = [
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

GOLD_TABLES = [
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_date",
    "fact_orders",
    "fact_order_items",
    "fact_order_payments",
    "fact_order_reviews",
]

# Helpers
def wait_for_superset(timeout = 120):
    """Block until Superset's /health endpoint returns 200."""
    deadline = time.time() + timeout
    print(f"Waiting for Superset at {SUPERSET_URL} …", flush=True)
    while time.time() < deadline:
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.status_code == 200:
                print("Superset is up.")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
    print("ERROR: Superset did not become healthy in time.", file=sys.stderr)
    sys.exit(1)


def create_hive_tables(tables_to_register, base_path):
    """
    Register every Parquet path in the Hive metastore via the Spark ThriftServer.
    """
    import subprocess
    import shutil

    print(f"\nRegistering Hive tables from {base_path} via ThriftServer at {THRIFT_HOST}:{THRIFT_PORT} …")

    # Build a self-contained Python snippet to run inside the superset container
    lines = [
        "from pyhive import hive",
        f"conn = hive.connect(host='{THRIFT_HOST}', port={THRIFT_PORT}, auth='NONE', database='default')",
        "cur = conn.cursor()",
    ]
    for table in tables_to_register:
        location = f"{base_path}/{table}"
        drop_ddl = f"DROP TABLE IF EXISTS `{table}`"
        ddl = f"CREATE TABLE `{table}` USING PARQUET LOCATION '{location}'"
        lines.append(f"cur.execute(\"{drop_ddl}\")")
        lines.append(f"cur.execute(\"{ddl}\")")
        lines.append(f"print('  [ok] {table}')")   
    lines += ["cur.close()", "conn.close()", "print('Hive tables ready.')"]
    script = "\n".join(lines)


    if shutil.which("docker") is None:
        print("WARNING: 'docker' not found — skipping Hive table creation.", file=sys.stderr)
        print("Run the DDL manually or ensure docker is on PATH.", file=sys.stderr)
        return

    result = subprocess.run(
        ["docker", "exec", "superset", "python", "-c", script],
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        print(f"ERROR creating Hive tables:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)



def get_token(session):
    """Authenticate and return a JWT access token."""
    payload = {
        "username": SUPERSET_USER,
        "password": SUPERSET_PASS,
        "provider":  "db",
        "refresh":   True,
    }
    r = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=payload)
    r.raise_for_status()
    token = r.json()["access_token"]
    print("Authentication successful.")
    return token


def get_csrf_token(session):
    """Fetch the CSRF token.
    The /api/v1/ REST endpoints are CSRF-exempt when using Bearer tokens.
    """
    r = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    raise_for_status_verbose(r)
    return r.json()["result"]


def find_existing_database(session, name):
    """Return the Superset database ID if a DB with *name* already exists."""
    r = session.get(f"{SUPERSET_URL}/api/v1/database/", params={"page_size": 100})
    raise_for_status_verbose(r)
    for db in r.json().get("result", []):
        if db.get("database_name") == name:
            db_id = db["id"]
            print(f"Found existing database '{name}' with id={db_id}.")
            return db_id
    return None


def create_database(session):
    """Create or update the SparkSQL / HiveServer2 database connection; return its ID."""
    existing_id = find_existing_database(session, DB_NAME)

    # SQLAlchemy URI for HiveServer2, authentication with NONE matches the ThriftServer
    sqlalchemy_uri = f"hive://{THRIFT_HOST}:{THRIFT_PORT}/default"

    payload = {
        "database_name":     DB_NAME,
        "sqlalchemy_uri":    sqlalchemy_uri,
        "expose_in_sqllab":  True,
        "allow_run_async":   False,
        "allow_ctas":        False,
        "allow_cvas":        False,
        "allow_dml":         False,
        "extra": '{"engine_params":{"connect_args":{"auth":"NONE"}}}',
    }

    if existing_id:
        r = session.put(f"{SUPERSET_URL}/api/v1/database/{existing_id}", json=payload)
        raise_for_status_verbose(r)
        print(f"Updated database '{DB_NAME}' (id={existing_id}) to disable async query execution.")
        return existing_id

    r = session.post(f"{SUPERSET_URL}/api/v1/database/", json=payload)
    raise_for_status_verbose(r)
    db_id = r.json()["id"]
    print(f"Created database '{DB_NAME}' with id={db_id}.")
    return db_id


def find_existing_dataset(session, db_id, table_name):
    """Return dataset ID if it already exists for this db + table."""
    r = session.get(f"{SUPERSET_URL}/api/v1/dataset/", params={"page_size": 100})
    raise_for_status_verbose(r)
    for ds in r.json().get("result", []):
        if ds.get("table_name") == table_name and ds.get("database", {}).get("id") == db_id:
            return ds["id"]
    return None


def refresh_dataset(session, ds_id, table_name):
    """Trigger a refresh of columns on the dataset to pick up new schema types."""
    r = session.put(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}/refresh")
    if r.status_code == 200:
        print(f"  [refresh] '{table_name}' columns synced.")
    else:
        print(f"  [warn] '{table_name}' refresh → HTTP {r.status_code}: {r.text}", file=sys.stderr)


def register_dataset(session, db_id, table_name):
    """Register a single Parquet table as a Superset Dataset."""
    existing_id = find_existing_dataset(session, db_id, table_name)
    if existing_id:
        print(f"  [skipped] '{table_name}' already registered (id={existing_id}).")
        refresh_dataset(session, existing_id, table_name)
        return

    payload = {
        "database":    db_id,
        "table_name":  table_name,
        "schema":      "default",
    }
    r = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload)
    if r.status_code in (200, 201):
        ds_id = r.json()["id"]
        print(f"  [ok] '{table_name}' registered (id={ds_id}).")
        refresh_dataset(session, ds_id, table_name)
    else:
        print(f"  [warn] '{table_name}' → HTTP {r.status_code}: {r.text}", file=sys.stderr)


def main():
    target = "all"
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ["silver", "gold", "all"]:
            target = arg
        else:
            print(f"Unknown target '{arg}'. Usage: python register_tables.py [silver|gold|all]")
            sys.exit(1)

    wait_for_superset()

    session = requests.Session()

    # 2. Authenticate with Superset
    token = get_token(session)
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    })

    # 3. Fetch and attach CSRF token 
    csrf = get_csrf_token(session)
    session.headers.update({"X-CSRFToken": csrf})

    # 4. Create or find the Spark database connection in Superset
    db_id = create_database(session)

    # Resolve jobs to run
    jobs = []
    if target in ["silver", "all"]:
        jobs.append((OLIST_TABLES, HDFS_BASE))
    if target in ["gold", "all"]:
        jobs.append((GOLD_TABLES, "hdfs://namenode:9000/gold"))

    for tables, base_path in jobs:
        # Register in Hive metastore
        create_hive_tables(tables, base_path)
        
        # Register in Superset
        print(f"\nRegistering {len(tables)} datasets from {base_path} in Superset …")
        for table in tables:
            register_dataset(session, db_id, table)

    print(f"\nDone!")


if __name__ == "__main__":
    main()