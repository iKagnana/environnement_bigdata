import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _parse_endpoint(endpoint: str):
    """
    Retourne (host:port, secure_bool) depuis une URL comme http://minio:9000 ou minio:9000
    """
    from urllib.parse import urlparse
    if not endpoint:
        return "minio:9000", False
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    host = f"{parsed.hostname}:{parsed.port or 9000}"
    secure = parsed.scheme == "https"
    return host, secure

def create_spark_session():
    """Créer une session Spark configurée pour MinIO (s3a) et Delta Lake"""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

    host_port, secure = _parse_endpoint(endpoint)

    builder = SparkSession.builder \
        .appName("Create Delta Tables - Healthcare Data") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{host_port}" if not secure else f"https://{host_port}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if secure else "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    return builder.getOrCreate()

def main():
    """Fonction principale"""
    logger.info("🎉 Les tables Delta seront créées dynamiquement lors de l'insertion des données")
    logger.info("✅ Ce job n'est plus nécessaire - les schémas sont définis dans le code ETL")
    logger.info("ℹ️  Les tables Delta sont créées automatiquement avec .write.format('delta').save()")

if __name__ == "__main__":
    main()
