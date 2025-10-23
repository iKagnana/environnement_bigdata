#!/bin/bash
set -e

echo "Initializing Superset..."

# Upgrade database
superset db upgrade

# Create admin user (ignore error if already exists)
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin || echo "Admin user already exists"

# Load examples
superset load_examples || echo "Examples already loaded"

# Initialize Superset
superset init

echo "Starting Superset web server..."

# Start Superset
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger