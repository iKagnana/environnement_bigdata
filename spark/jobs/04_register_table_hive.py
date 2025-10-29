import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Créer une session Spark configurée pour MinIO et Delta Lake"""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

    builder = SparkSession.builder \
        .appName("Register Delta Tables in Hive Metastore") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport()

    return builder.getOrCreate()

def register_tables_in_hive(spark):
    """Enregistrer toutes les tables Delta dans le metastore Hive"""
    logger.info("🚀 Enregistrement des tables Delta dans le metastore Hive...")

    # Créer la base de données healthcare
    spark.sql("CREATE DATABASE IF NOT EXISTS healthcare LOCATION 's3a://healthcare-data/gold'")
    spark.sql("USE healthcare")

    tables_config = [
        # Tables de dimensions
        ("dim_lieu", "s3a://healthcare-data/gold/dim_lieu"),
        ("dim_patient", "s3a://healthcare-data/gold/dim_patient"),
        ("dim_date", "s3a://healthcare-data/gold/dim_date"),
        ("dim_etablissement", "s3a://healthcare-data/gold/dim_etablissement"),
        ("dim_diagnostic", "s3a://healthcare-data/gold/dim_diagnostic"),
        ("dim_indicateur", "s3a://healthcare-data/gold/dim_indicateur"),
        ("dim_professionel", "s3a://healthcare-data/gold/dim_professionel"),
        
        # Tables de faits
        ("fait_deces", "s3a://healthcare-data/gold/fait_deces"),
        ("fait_hospitalisation", "s3a://healthcare-data/gold/fait_hospitalisation"),
        ("fait_satisfaction", "s3a://healthcare-data/gold/fait_satisfaction"),
        ("fait_consultations", "s3a://healthcare-data/gold/fait_consultations")
    ]

    success_count = 0
    failed_count = 0

    for table_name, table_path in tables_config:
        try:
            # Vérifier que la table Delta existe
            df = spark.read.format("delta").load(table_path)
            
            # Créer la table dans le metastore Hive
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{table_path}'
            """
            
            spark.sql(create_table_sql)
            logger.info(f"✅ Table {table_name} enregistrée dans le metastore")
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ Erreur pour la table {table_name}: {str(e)}")
            failed_count += 1

    # Vérifier l'enregistrement
    logger.info("\n🔍 Vérification des tables enregistrées:")
    spark.sql("SHOW DATABASES").show()
    spark.sql("SHOW TABLES IN healthcare").show()

    logger.info(f"✅ Tables enregistrées avec succès: {success_count}")
    logger.info(f"❌ Tables échouées: {failed_count}")

    return success_count, failed_count

def main():
    spark = None
    try:
        spark = create_spark_session()
        success_count, failed_count = register_tables_in_hive(spark)
        
        if failed_count > 0:
            import sys
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Erreur: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        import sys
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()