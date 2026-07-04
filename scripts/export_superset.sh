#!/bin/bash

# Exit on error
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Check if superset container is running
if ! docker ps --format '{{.Names}}' | grep -q '^superset$'; then
    echo "ERROR: The 'superset' container is not running!" >&2
    echo "Please start the services using: docker compose -f docker/docker-compose-superset.yml up -d" >&2
    exit 1
fi

echo "Exporting dashboards from Superset..."
docker exec superset superset export-dashboards -f /app/pythonpath/dashboards.zip

echo "Success! Export saved to: $PROJECT_ROOT/docker/superset/dashboards.zip"
