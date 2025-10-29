#!/usr/bin/env bash
set -euo pipefail

# Attendre que le Spark Master soit disponible sur le port 7077
echo "⏳ Attente du Spark Master sur le port 7077..."
max_retries=30
count=0
while [ $count -lt $max_retries ]; do
  if timeout 2 bash -c "</dev/tcp/spark-master/7077" 2>/dev/null; then
    echo "✅ Spark Master accessible sur le port 7077"
    break
  fi
  echo "   Tentative $((count+1))/$max_retries..."
  sleep 2
  count=$((count + 1))
done
if [ $count -ge $max_retries ]; then
  echo "❌ ERREUR: Spark Master non accessible après $((max_retries*2))s"
  exit 1
fi

# Attendre que le master ait au moins 1 worker enregistré (timeout ≈ 60s)
echo "⏳ Attente de l'enregistrement d'au moins 1 worker sur le master..."
max_retries=20
count=0
while [ $count -lt $max_retries ]; do
  if curl -s http://spark-master:8080/json/ 2>/dev/null | grep -q '"workers":[^[]*' && \
    ! curl -s http://spark-master:8080/json/ 2>/dev/null | grep -q '"workers":\[\]'; then
    echo "✅ Worker(s) enregistré(s)"
    break
  fi
  sleep 3
  count=$((count + 1))
done
if [ $count -ge $max_retries ]; then
  echo "⚠️ Timeout: pas de worker enregistré après $((max_retries*3))s, on continue quand même"
fi

SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
JOB_DIR="${JOB_DIR:-/opt/app/spark/jobs}"

# Liste des jars (optionnel)
JARS=$(ls /opt/jars/*.jar 2>/dev/null | paste -sd, - || echo "")

echo "🚀 Spark job runner"
echo " - Spark master: $SPARK_MASTER"
echo " - MinIO endpoint: $MINIO_ENDPOINT"
echo " - Job dir: $JOB_DIR"
echo " - JARS: ${JARS:-(none)}"

# If mc is installed, configure alias and ensure bucket exists (no-fail)
if command -v mc >/dev/null 2>&1; then
  echo "🔧 Configuring mc alias for MinIO"
  mc alias set minio "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4 --insecure 2>/dev/null || true
  # adjust bucket name as needed (here 'healthcare-data')
  mc ls minio/healthcare-data >/dev/null 2>&1 || mc mb --insecure minio/healthcare-data >/dev/null 2>&1 || true
fi

shopt -s nullglob
jobs=("$JOB_DIR"/*.py)
if [ ${#jobs[@]} -eq 0 ]; then
  echo "⚠️ Aucun job trouvé dans $JOB_DIR"
  exit 0
fi
for job in "${jobs[@]}"; do
  echo "----------------------------------------"
  echo "📄 Exécution du job: $job"
  set +e
  /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --total-executor-cores 1 \
    --conf "spark.driver.maxResultSize=512m" \
    --conf "spark.python.worker.memory=512m" \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    $( [ -n "$JARS" ] && echo --jars "$JARS" ) \
    "$job"
  rc=$?
  set -e
  if [ $rc -ne 0 ]; then
    echo "❌ Job failed: $job (exit $rc)"
    # continuer avec les autres jobs (ne pas exit pour batch complet)
  else
    echo "✅ Job terminé: $job"
  fi
done

echo "🎉 Tous les jobs traités"