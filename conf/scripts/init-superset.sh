#!/bin/bash
set -e

echo "🔧 Initializing Superset..."

# Attendre que Trino soit prêt
echo "⏳ Waiting for Trino to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0

until curl -sf http://trino:8080/v1/info > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "❌ Trino not ready after $MAX_RETRIES attempts"
        echo "⚠️  Continuing anyway..."
        break
    fi
    echo "Trino is unavailable - sleeping (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 5
done

if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
    echo "✅ Trino is ready"
fi

# Tester la connexion Python à Trino
echo "🔍 Testing Trino connection from Python..."
python3 << 'EOF'
try:
    from trino.dbapi import connect
    conn = connect(
        host='trino',
        port=8080,
        user='admin',
        catalog='hive'
    )
    cursor = conn.cursor()
    cursor.execute('SHOW CATALOGS')
    catalogs = cursor.fetchall()
    print(f'✅ Trino connection successful. Catalogs: {[c[0] for c in catalogs]}')
except Exception as e:
    print(f'⚠️  Trino connection failed: {e}')
    print('Continuing with Superset initialization...')
EOF

# Upgrade database
echo "📦 Upgrading Superset database..."
superset db upgrade

# Create admin user (ignore error if already exists)
echo "👤 Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin || echo "✅ Admin user already exists"

# Initialize Superset
echo "🔧 Initializing Superset..."
superset init

echo "🚀 Starting Superset web server..."

# Start Superset
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger