import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

def create_delta_table_safe(spark, table_name, schema, partition_by, path):
    """Créer une table Delta de manière sécurisée avec gestion d'erreur"""
    try:
        logger.info(f"Création de la table {table_name}...")
        # Créer un DataFrame vide avec le schéma - utiliser sparkContext directement
        # pour éviter les problèmes de sérialisation
        empty_rdd = spark.sparkContext.emptyRDD()
        df = spark.createDataFrame(empty_rdd, schema)
        writer = df.write.format("delta").mode("ignore").option("overwriteSchema", "true")
        if partition_by:
            if isinstance(partition_by, list):
                writer = writer.partitionBy(*partition_by)
            else:
                writer = writer.partitionBy(partition_by)
        writer.save(path)
        logger.info(f"___Table {table_name} créée avec succès___")
        return True
    except Exception as e:
        logger.error(f"___Erreur lors de la création de {table_name}: {str(e)}___")
        import traceback
        logger.error(traceback.format_exc())
        return False

def create_delta_tables(spark):
    """Créer toutes les tables Delta selon le schéma défini"""
    logger.info("___Création des tables Delta Healthcare...___")
    tables_config = []

    # Dimension des lieux
    tables_config.append({
        "name": "dim_lieu",
        "schema": StructType([
            StructField("lieu_id", IntegerType(), True),
            StructField("commune", StringType(), True),
            StructField("region", StringType(), True),
            StructField("iso", StringType(), True)
        ]),
        "partition_by": "region",
        "path": "s3a://healthcare-data/gold/dim_lieu"
    })

    # Dimension des patients
    tables_config.append({
        "name": "dim_patient",
        "schema": StructType([
            StructField("patient_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("sexe", StringType(), True),
            StructField("annee_naissance", IntegerType(), True),
            StructField("date_naissance", DateType(), True)
        ]),
        "partition_by": "annee_naissance",
        "path": "s3a://healthcare-data/gold/dim_patient"
    })

    # Dimension des dates
    tables_config.append({
        "name": "dim_date",
        "schema": StructType([
            StructField("date_id", IntegerType(), True),
            StructField("date_complete", DateType(), True),
            StructField("annee", IntegerType(), True),
            StructField("mois", IntegerType(), True),
            StructField("jour", IntegerType(), True),
            StructField("trimestre", IntegerType(), True),
            StructField("semestre", IntegerType(), True)
        ]),
        "partition_by": "annee",
        "path": "s3a://healthcare-data/gold/dim_date"
    })

    # Dimension des établissements
    tables_config.append({
        "name": "dim_etablissement",
        "schema": StructType([
            StructField("etablissement_id", StringType(), True),
            StructField("raison_sociale", StringType(), True),
            StructField("commune", StringType(), True),
            StructField("region", StringType(), True),
            StructField("finess", StringType(), True)
        ]),
        "partition_by": "region",
        "path": "s3a://healthcare-data/gold/dim_etablissement"
    })

    # Dimension des diagnostics
    tables_config.append({
        "name": "dim_diagnostic",
        "schema": StructType([
            StructField("diagnostic_id", StringType(), True),
            StructField("libelle_diagnostic", StringType(), True),
            StructField("categorie_diagnostic", StringType(), True)
        ]),
        "partition_by": "categorie_diagnostic",
        "path": "s3a://healthcare-data/gold/dim_diagnostic"
    })

    # Dimension des indicateurs
    tables_config.append({
        "name": "dim_indicateur",
        "schema": StructType([
            StructField("indicateur_id", IntegerType(), True),
            StructField("libelle_indicateur", StringType(), True)
        ]),
        "partition_by": "libelle_indicateur",
        "path": "s3a://healthcare-data/gold/dim_indicateur"
    })

    # Dimension des professionnels
    tables_config.append({
        "name": "dim_professionel",
        "schema": StructType([
            StructField("professionnel_id", StringType(), True),
            StructField("nom", StringType(), True),
            StructField("prenom", StringType(), True),
            StructField("civilite", StringType(), True),
            StructField("profession", StringType(), True),
            StructField("specialite", StringType(), True)
        ]),
        "partition_by": ["profession", "specialite"],
        "path": "s3a://healthcare-data/gold/dim_professionel"
    })

    # Fait décès
    tables_config.append({
        "name": "fait_deces",
        "schema": StructType([
            StructField("id_mort", IntegerType(), True),
            StructField("fk_date_deces", IntegerType(), True),
            StructField("fk_lieu_deces", IntegerType(), True),
            StructField("nb_deces", IntegerType(), True),
            StructField("annee", IntegerType(), True)
        ]),
        "partition_by": "annee",
        "path": "s3a://healthcare-data/gold/fait_deces"
    })

    # Fait hospitalisation
    tables_config.append({
        "name": "fait_hospitalisation",
        "schema": StructType([
            StructField("fk_patient", IntegerType(), True),
            StructField("fk_etablissement", StringType(), True),
            StructField("fk_diagnostic", StringType(), True),
            StructField("fk_date_entree", IntegerType(), True),
            StructField("duree_sejour_jours", IntegerType(), True),
            StructField("nb_hospitalisations", IntegerType(), True),
            StructField("annee", IntegerType(), True)
        ]),
        "partition_by": "annee",
        "path": "s3a://healthcare-data/gold/fait_hospitalisation"
    })

    # Fait satisfaction
    tables_config.append({
        "name": "fait_satisfaction",
        "schema": StructType([
            StructField("fk_etablissement", StringType(), True),
            StructField("fk_indicateur", IntegerType(), True),
            StructField("fk_date_mesure", IntegerType(), True),
            StructField("score_ajuste", FloatType(), True),
            StructField("nombre_reponses", IntegerType(), True),
            StructField("taux_participation", FloatType(), True)
        ]),
        "partition_by": "fk_indicateur",
        "path": "s3a://healthcare-data/gold/fait_satisfaction"
    })

    # Fait consultations
    tables_config.append({
        "name": "fait_consultations",
        "schema": StructType([
            StructField("fk_patient", IntegerType(), True),
            StructField("fk_professionnel", StringType(), True),
            StructField("fk_date_consultation", IntegerType(), True),
            StructField("fk_diagnostic", StringType(), True),
            StructField("fk_etablissement", StringType(), True),
            StructField("nb_consultations", IntegerType(), True),
            StructField("annee", IntegerType(), True)
        ]),
        "partition_by": "annee",
        "path": "s3a://healthcare-data/gold/fait_consultations"
    })

    # Création des tables
    success_count = 0
    failed_count = 0
    for table_config in tables_config:
        if create_delta_table_safe(
            spark,
            table_config["name"],
            table_config["schema"],
            table_config["partition_by"],
            table_config["path"]
        ):
            success_count += 1
        else:
            failed_count += 1

    logger.info("=" * 60)
    logger.info(f"___Tables créées avec succès: {success_count}___")
    logger.info(f"___Tables échouées: {failed_count}___")
    logger.info(f"___Total: {success_count + failed_count}___")
    logger.info("=" * 60)
    return success_count, failed_count

def verify_tables(spark):
    """Vérifier que toutes les tables ont été créées"""
    logger.info("___Vérification des tables créées...___")
    tables = [
        ("dim_lieu", "s3a://healthcare-data/gold/dim_lieu"),
        ("dim_patient", "s3a://healthcare-data/gold/dim_patient"),
        ("dim_date", "s3a://healthcare-data/gold/dim_date"),
        ("dim_etablissement", "s3a://healthcare-data/gold/dim_etablissement"),
        ("dim_diagnostic", "s3a://healthcare-data/gold/dim_diagnostic"),
        ("dim_indicateur", "s3a://healthcare-data/gold/dim_indicateur"),
        ("dim_professionel", "s3a://healthcare-data/gold/dim_professionel"),
        ("fait_deces", "s3a://healthcare-data/gold/fait_deces"),
        ("fait_hospitalisation", "s3a://healthcare-data/gold/fait_hospitalisation"),
        ("fait_satisfaction", "s3a://healthcare-data/gold/fait_satisfaction"),
        ("fait_consultations", "s3a://healthcare-data/gold/fait_consultations")
    ]
    verified_count = 0
    for table_name, table_path in tables:
        try:
            df = spark.read.format("delta").load(table_path)
            col_count = len(df.columns)
            row_count = df.count()
            logger.info(f"___Table {table_name}: {col_count} colonnes, {row_count} lignes___")
            verified_count += 1
        except Exception as e:
            logger.error(f"___Erreur pour la table {table_name}: {str(e)}___")
    logger.info(f"___Tables vérifiées: {verified_count}/{len(tables)}___")

def main():
    """Fonction principale pour créer les tables Delta"""
    spark = None
    try:
        logger.info("___Début de la création des tables Delta Healthcare...___")
        # Créer la session Spark
        spark = create_spark_session()
        # Créer toutes les tables
        success_count, failed_count = create_delta_tables(spark)
        # Vérifier les tables créées seulement si tout s'est bien passé
        if failed_count == 0:
            verify_tables(spark)
        logger.info("___Création des tables Delta terminée!___")
        # Retourner un code de sortie approprié
        if failed_count > 0:
            import sys
            sys.exit(1)
    except Exception as e:
        logger.error(f"___Erreur lors de la création des tables: {str(e)}___")
        import traceback
        logger.error(traceback.format_exc())
        import sys
        sys.exit(1)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except:
                pass

if __name__ == "__main__":
    main()
