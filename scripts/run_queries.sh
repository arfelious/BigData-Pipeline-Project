#!/bin/bash
# run_queries.sh: Executes verification queries against Spark ThriftServer
set -e

# Make sure working directory is project root
cd "$(dirname "$0")/.."

echo "===================================================="
echo "Running Business Queries against Spark ThriftServer..."
echo "===================================================="

if [ -f ".venv/bin/python" ]; then
  .venv/bin/python scripts/query_verify.py
else
  python3 scripts/query_verify.py
fi

echo "===================================================="
echo "Queries completed successfully!"
echo "===================================================="
