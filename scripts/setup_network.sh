#!/bin/bash
# Create the shared Docker network used by all services.
# Run this once before starting any docker-compose stack.

set -e

NETWORK_NAME="bigdata-net"

if docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    echo "Docker network '$NETWORK_NAME' already exists; nothing to do."
else
    docker network create "$NETWORK_NAME"
    echo "Docker network '$NETWORK_NAME' created."
fi
