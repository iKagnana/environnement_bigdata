#!/bin/bash
set -e

echo "⏳ Waiting ${STARTUP_SLEEP:-15} seconds for services to be ready..."
sleep ${STARTUP_SLEEP:-15}

MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}
JOB_DIR=${JOB_DIR:-/opt/app/spark/jobs}
SPARK_HOME=${SPARK_HOME:-/opt/spark}

echo "🚀 Starting Spark jobs execution..."
echo "📍 Master URL: $MASTER_URL"
echo "📁 Job directory: $JOB_DIR"
echo "🏠 Spark Home: $SPARK_HOME"

# Liste des jobs à exécuter dans l'ordre
JOBS=(
    "01_convert_to_parquet.py"
    "02_create_star_table.py"
    "03_gold_jobs.py"
    "04_register_table_hive.py"
    "05_migrate_to_s3.py"
)

for job in "${JOBS[@]}"; do
    job_path="$JOB_DIR/$job"
    
    if [ -f "$job_path" ]; then
        echo ""
        echo "════════════════════════════════════════════"
        echo "🔄 Executing: $job"
        echo "════════════════════════════════════════════"
        
        $SPARK_HOME/bin/spark-submit \
            --master $MASTER_URL \
            --deploy-mode client \
            --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000} \
            --conf spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY:-minioadmin} \
            --conf spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY:-minioadmin123} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --packages io.delta:delta-core_2.12:2.4.0 \
            "$job_path"
        
        if [ $? -eq 0 ]; then
            echo "✅ $job completed successfully"
        else
            echo "❌ $job failed with exit code $?"
            exit 1
        fi
    else
        echo "⚠️  Job file not found: $job_path"
    fi
done

echo ""
echo "════════════════════════════════════════════"
echo "✅ All jobs completed successfully!"
echo "════════════════════════════════════════════"

# Garder le conteneur actif pour le debugging
echo "💤 Keeping container alive for monitoring..."
tail -f /dev/null