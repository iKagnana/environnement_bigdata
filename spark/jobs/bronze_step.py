import os
import sys
import logging
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _parse_endpoint(endpoint: str):
    """
    Retourne (host:port, secure_bool) depuis une URL comme http://minio:9000 ou minio:9000
    """
    if not endpoint:
        return "minio:9000", False
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    host = f"{parsed.hostname}:{parsed.port or 9000}"
    secure = parsed.scheme == "https"
    return host, secure


def create_spark_session():
    """Créer une session Spark configurée pour MinIO (s3a) en lisant les variables d'environnement"""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

    host_port, secure = _parse_endpoint(endpoint)

    builder = SparkSession.builder \
        .appName("Bronze Layer - Healthcare Data") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{host_port}" if not secure else f"https://{host_port}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if secure else "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Si besoin d'extensions Delta (assurez-vous d'avoir les jars)
    builder = builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return builder.getOrCreate()

def create_minio_bucket(bucket_name="healthcare-data"):
    """Créer le bucket MinIO si nécessaire (utilise python-minio). Silencieux en cas d'erreur réseau."""
    try:
        from minio import Minio
    except Exception as e:
        logger.warning("minio SDK non installé dans l'image : %s", e)
        return

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

    host_port, secure = _parse_endpoint(endpoint)
    # Minio client veut host:port sans scheme
    try:
        client = Minio(host_port, access_key=access_key, secret_key=secret_key, secure=secure)
        if not client.bucket_exists(bucket_name):
            logger.info("Création du bucket MinIO: %s", bucket_name)
            client.make_bucket(bucket_name)
        else:
            logger.info("Bucket MinIO déjà existant: %s", bucket_name)
    except Exception as e:
        logger.warning("Impossible de créer ou vérifier le bucket '%s' : %s", bucket_name, e)

def bronze_ingestion(spark, source_path, target_path, table_name):
    """
    Ingestion des données brutes vers la couche Bronze (format Parquet dans MinIO)
    """
    try:
        logger.info(f"Début de l'ingestion Bronze pour {table_name}")
        
        # Lecture des données sources
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .csv(source_path)
        
        # Ajout de métadonnées Bronze
        df_bronze = df \
            .withColumn("bronze_ingestion_timestamp", current_timestamp()) \
            .withColumn("bronze_source_file", lit(source_path)) \
            .withColumn("bronze_table_name", lit(table_name))
        
        # Écriture dans MinIO (S3A)
        logger.info(f"Sauvegarde dans MinIO: {target_path}")
        df_bronze.write \
            .mode("overwrite") \
            .parquet(target_path)
        
        logger.info(f"✅ Ingestion réussie pour {table_name} - {df_bronze.count()} lignes")
        
        # Affichage des statistiques
        print(f"=== BRONZE LAYER - {table_name} ===")
        print(f"📍 Localisation: {target_path}")
        print(f"📊 Nombre de lignes: {df_bronze.count()}")
        print(f"📊 Nombre de colonnes: {len(df_bronze.columns)}")
        print("📋 Schéma des données:")
        df_bronze.printSchema()
        print("👀 Aperçu des données:")
        df_bronze.show(5)
        
        return df_bronze
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'ingestion de {table_name}: {str(e)}")
        raise

def main():
    """Fonction principale pour l'ingestion Bronze"""
    # Créer le bucket MinIO
    create_minio_bucket()
    
    spark = create_spark_session()
    
    try:
        # Configuration des chemins MinIO (S3A)
        minio_base_path = "s3a://healthcare-data/bronze"
        
        # Tables à ingérer - CHEMINS MINIOS
        tables_config = {
            "hospitalisation": {
                "source": "../datas/DATA 2024/Hospitalisation/Hospitalisations.csv",
                "target": f"{minio_base_path}/hospitalisation/",  # ← MinIO path
                "table_name": "hospitalisation"
            }
        }
        
        logger.info("🚀 Début de l'ingestion Bronze vers MinIO")
        
        # Ingestion de chaque table
        for table_name, config in tables_config.items():
            try:
                bronze_ingestion(
                    spark=spark,
                    source_path=config["source"],
                    target_path=config["target"],
                    table_name=table_name
                )
            except Exception as e:
                logger.warning(f"⚠️ Échec de l'ingestion pour {table_name}: {str(e)}")
                continue
        
        logger.info("🎉 Ingestion Bronze terminée avec succès")
        
    except Exception as e:
        logger.error(f"💥 Erreur générale: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()