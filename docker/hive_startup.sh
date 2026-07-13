#!/usr/bin/env bash
set -e

echo "=== HIVE METASTORE STARTUP SCRIPT ==="
echo "Initializing Hive Metastore database schema..."
# Run schematool to initialize the Postgres metastore schema tables.
# The '|| true' ensures that if the schema already exists, the script
# continues and doesn't abort.
/opt/hive/bin/schematool -dbType postgres -initSchema || echo "Schema tables already exist, continuing."

echo "Starting Hive Metastore service on port 9083..."
exec /opt/hive/bin/hive --service metastore
