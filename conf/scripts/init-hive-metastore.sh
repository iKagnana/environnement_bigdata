#!/bin/bash
set -e

echo "🔧 Waiting for PostgreSQL to be ready..."
until nc -z ${POSTGRES_HOST:-postgres} ${POSTGRES_PORT:-5432}; do
  echo "⏳ PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "✅ PostgreSQL is up"

# Installer postgresql-client si nécessaire
apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Afficher la configuration Hive
echo "📋 Hive configuration:"
cat /opt/hive/conf/hive-site.xml | grep -A 1 "ConnectionURL"

# Vérifier si le schéma existe déjà
echo "🔍 Checking if metastore schema already exists..."
SCHEMA_EXISTS=$(PGPASSWORD=${POSTGRES_PASSWORD:-postgres123} psql \
  -h ${POSTGRES_HOST:-postgres} \
  -p ${POSTGRES_PORT:-5432} \
  -U ${POSTGRES_USER:-postgres} \
  -d ${POSTGRES_DB:-metastore} \
  -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='TBLS';" 2>/dev/null || echo "0")

if [ "$SCHEMA_EXISTS" -gt "0" ]; then
  echo "✅ Metastore schema already exists, skipping initialization"
else
  echo "🔧 Initializing Hive Metastore schema with PostgreSQL..."
  
  # FORCER l'utilisation de PostgreSQL
  /opt/hive/bin/schematool \
    -dbType postgres \
    -initSchema \
    --verbose || {
    
    echo "⚠️  Schema tool returned an error, checking if tables were created..."
    
    TABLES_CREATED=$(PGPASSWORD=${POSTGRES_PASSWORD:-postgres123} psql \
      -h ${POSTGRES_HOST:-postgres} \
      -p ${POSTGRES_PORT:-5432} \
      -U ${POSTGRES_USER:-postgres} \
      -d ${POSTGRES_DB:-metastore} \
      -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';" 2>/dev/null || echo "0")
    
    if [ "$TABLES_CREATED" -gt "10" ]; then
      echo "✅ Schema tables created successfully ($TABLES_CREATED tables)"
    else
      echo "❌ Schema initialization failed - only $TABLES_CREATED tables created"
      exit 1
    fi
  }
fi

echo "🚀 Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore