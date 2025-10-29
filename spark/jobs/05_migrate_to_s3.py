from pyspark.sql import SparkSession
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Créer une session Spark avec support s3a ET s3"""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
    
    spark = SparkSession.builder \
        .appName("MigrateDeltaToS3") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3.access.key", access_key) \
        .config("spark.hadoop.fs.s3.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3.path.style.access", "true") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

def migrate_table(spark, table_name):
    """Migrer une table Delta de s3a:// vers s3://"""
    source_path = f"s3a://healthcare-data/gold/{table_name}"
    target_path = f"s3://healthcare-data/gold/{table_name}"
    
    logger.info(f"📋 Migration de {table_name}...")
    logger.info(f"   Source: {source_path}")
    logger.info(f"   Cible:  {target_path}")
    
    try:
        # Lire depuis s3a://
        df = spark.read.format("delta").load(source_path)
        count = df.count()
        
        if count == 0:
            logger.warning(f"⚠️  {table_name} est vide, passage à la suivante")
            return False
        
        logger.info(f"   📊 {count} lignes à migrer")
        
        # Écrire vers s3:// en Delta
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)
        
        # Mettre à jour le Hive Metastore
        spark.sql(f"DROP TABLE IF EXISTS healthcare.{table_name}")
        spark.sql(f"""
            CREATE TABLE healthcare.{table_name}
            USING DELTA
            LOCATION '{target_path}'
        """)
        
        logger.info(f"✅ {table_name} migré avec succès ({count} lignes)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la migration de {table_name}: {e}")
        return False

def verify_migration(spark, table_name):
    """Vérifier qu'une table migrée est accessible"""
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM healthcare.{table_name}").first().cnt
        logger.info(f"   ✓ Vérification {table_name}: {count} lignes")
        return True
    except Exception as e:
        logger.error(f"   ✗ Erreur vérification {table_name}: {e}")
        return False

def main():
    logger.info("🚀 Démarrage de la migration Delta s3a:// → s3://")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    # Liste de toutes les tables à migrer (mêmes noms que dans 04_register_table_hive.py)
    tables = [
        # Tables de dimensions
        "dim_lieu",
        "dim_patient",
        "dim_date",
        "dim_etablissement",
        "dim_diagnostic",
        "dim_indicateur",
        "dim_professionel",
        
        # Tables de faits
        "fait_deces",
        "fait_hospitalisation",
        "fait_satisfaction",
        "fait_consultations"
    ]
    
    # Créer la base de données si nécessaire
    spark.sql("CREATE DATABASE IF NOT EXISTS healthcare")
    
    # Compteurs
    success_count = 0
    failed_count = 0
    
    # Migrer chaque table
    for table_name in tables:
        if migrate_table(spark, table_name):
            success_count += 1
        else:
            failed_count += 1
        logger.info("-" * 60)
    
    # Vérification finale
    logger.info("\n📊 Vérification finale des tables migrées:")
    logger.info("=" * 60)
    
    for table_name in tables:
        verify_migration(spark, table_name)
    
    # Afficher les tables disponibles
    logger.info("\n📋 Tables disponibles dans healthcare:")
    spark.sql("SHOW TABLES IN healthcare").show(truncate=False)
    
    # Résumé
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ Migration terminée:")
    logger.info(f"   • Tables réussies: {success_count}")
    logger.info(f"   • Tables échouées: {failed_count}")
    logger.info("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()