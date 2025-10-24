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

def create_delta_tables(spark):
    """Créer toutes les tables Delta selon le schéma défini"""
    logger.info("🚀 Création des tables Delta Healthcare...")
    
    # =================================
    # TABLES DE DIMENSIONS
    # =================================
    
    # Dimension des lieux
    logger.info("Création de la table dim_lieu...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_lieu` (
            lieu_id INT COMMENT 'Identifiant du lieu',
            commune STRING COMMENT 'Nom de la commune',
            departement STRING COMMENT 'Code du département',
            region STRING COMMENT 'Région administrative',
            pays STRING COMMENT 'Pays'
        )
        USING DELTA
        PARTITIONED BY (region)
        LOCATION 's3a://healthcare-data/gold/dim_lieu'
    """)
    
    # Dimension des patients
    logger.info("Création de la table dim_patient...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_patient` (
            patient_id INT COMMENT 'Identifiant du patient',
            age INT COMMENT 'Âge du patient',
            sexe STRING COMMENT 'Sexe du patient',
            annee_naissance INT COMMENT 'Année de naissance', ## Utile pour la partition
            date_naissance DATE COMMENT 'Date de naissance'
        )
        USING DELTA
        PARTITIONED BY (annee_naissance)
        LOCATION 's3a://healthcare-data/gold/dim_patient'
    """)
    
    # Dimension des dates
    logger.info("Création de la table dim_date...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_date` (
            date_id INT COMMENT 'Identifiant de la date',
            date_complete DATE COMMENT 'Date complète',
            annee INT COMMENT 'Année',
            mois INT COMMENT 'Mois',
            jour INT COMMENT 'Jour',
            trimestre INT COMMENT 'Trimestre',
            semestre INT COMMENT 'Semestre'
        )   
        USING DELTA
        PARTITIONED BY (annee)
        LOCATION 's3a://healthcare-data/gold/dim_date'
    """)
    
    # Dimension des établissements de santé
    logger.info("Création de la table dim_etablissement...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_etablissement` (
            etablissement_id INT COMMENT 'Identifiant établissement',
            raison_sociale STRING COMMENT 'Raison sociale',
            adresse STRING COMMENT 'Adresse postale',
            commune STRING COMMENT 'Commune',
            code_postal STRING COMMENT 'Code postal',
            region STRING COMMENT 'Région'
        )
        USING DELTA
        PARTITIONED BY (region)
        LOCATION 's3a://healthcare-data/gold/dim_etablissement'
    """)
    
    # Dimension des diagnostics
    logger.info("Création de la table dim_diagnostic...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_diagnostic` (
            diagnostic_id STRING COMMENT 'Code diagnostic',
            libelle_diagnostic STRING COMMENT 'Libellé du diagnostic',
            categorie_diagnostic STRING COMMENT 'Catégorie du diagnostic'
        )   
        USING DELTA
        PARTITIONED BY (categorie_diagnostic)
        LOCATION 's3a://healthcare-data/gold/dim_diagnostic'
    """)
    
    # Dimension des indicateurs de satisfaction
    logger.info("Création de la table dim_indicateur...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_indicateur` (
            indicateur_id INT COMMENT 'Identifiant indicateur',
            libelle_indicateur STRING COMMENT 'Libellé de l'indicateur',
            categorie_indicateur STRING COMMENT 'Catégorie de l'indicateur'
        )
        USING DELTA
        PARTITIONED BY (categorie_indicateur)
        LOCATION 's3a://healthcare-data/gold/dim_indicateur'
    """)
    
    # Dimension des professionnels de santé
    logger.info("Création de la table dim_professionel...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/dim_professionel` (
            professionnel_id INT COMMENT 'Identifiant professionnel de santé',
            nom STRING COMMENT 'Nom',
            prenom STRING COMMENT 'Prénom',
            civilite STRING COMMENT 'Civilité',
            profession STRING COMMENT 'Profession',
            specialite STRING COMMENT 'Spécialité'
        )
        USING DELTA
        PARTITIONED BY (profession, specialite)
        LOCATION 's3a://healthcare-data/gold/dim_professionel'
    """)
    
    # =================================
    # TABLES DE FAITS
    # =================================
    
    # Table de faits des décès
    logger.info("Création de la table fait_deces...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/fait_deces` (
            fk_patient INT COMMENT 'Clé étrangère patient',
            fk_date_deces INT COMMENT 'Clé étrangère date de décès',
            fk_lieu_deces INT COMMENT 'Clé étrangère lieu de décès',
            fk_lieu_naissance INT COMMENT 'Clé étrangère lieu de naissance',
            nb_deces INT COMMENT 'Nombre de décès agrégé',
            annee INT COMMENT 'Année de partition'
        )   
        USING DELTA
        PARTITIONED BY (annee)
        LOCATION 's3a://healthcare-data/gold/fait_deces'
    """)
    
    # Table de faits des hospitalisations
    logger.info("Création de la table fait_hospitalisation...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/fait_hospitalisation` (
            fk_patient INT COMMENT 'Clé étrangère patient',
            fk_etablissement INT COMMENT 'Clé étrangère établissement',
            fk_diagnostic STRING COMMENT 'Clé étrangère diagnostic',
            fk_date_entree INT COMMENT 'Clé étrangère date d'entrée',
            duree_sejour_jours INT COMMENT 'Durée du séjour (en jours)',
            nb_hospitalisations INT COMMENT 'Nombre d'hospitalisations (agrégé)',
            annee INT COMMENT 'Année de partition'
        )
        USING DELTA
        PARTITIONED BY (annee)
        LOCATION 's3a://healthcare-data/gold/fait_hospitalisation'
    """)
    
    # Table de faits de la satisfaction des patients
    logger.info("Création de la table fait_satisfaction...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/fait_satisfaction` (
            fk_etablissement INT COMMENT 'Clé étrangère établissement',
            fk_indicateur INT COMMENT 'Clé étrangère indicateur',
            fk_date_mesure INT COMMENT 'Clé étrangère date de mesure',
            score_ajuste FLOAT COMMENT 'Score ajusté de satisfaction',
            nombre_reponses INT COMMENT 'Nombre total de réponses',
            taux_participation FLOAT COMMENT 'Taux de participation (%)',
            annee INT COMMENT 'Année de partition'
        )   
        USING DELTA
        PARTITIONED BY (annee)
        LOCATION 's3a://healthcare-data/gold/fait_satisfaction'
    """)
    
    # Table de faits des consultations
    logger.info("Création de la table fait_consultations...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`s3a://healthcare-data/gold/fait_consultations` (
            fk_patient INT COMMENT 'Clé étrangère patient',
            fk_professionnel INT COMMENT 'Clé étrangère professionnel de santé',
            fk_date_consultation INT COMMENT 'Clé étrangère date de consultation',
            fk_diagnostic STRING COMMENT 'Clé étrangère diagnostic',
            fk_etablissement INT COMMENT 'Clé étrangère établissement',
            duree_consultation_minutes INT COMMENT 'Durée moyenne des consultations (en minutes)',
            nb_consultations INT COMMENT 'Nombre total de consultations',
            annee INT COMMENT 'Année de partition'
        )   
        USING DELTA
        PARTITIONED BY (annee)
        LOCATION 's3a://healthcare-data/gold/fait_consultations'
    """)
    
    logger.info("✅ Toutes les tables Delta ont été créées avec succès!")

def verify_tables(spark):
    """Vérifier que toutes les tables ont été créées"""
    logger.info("🔍 Vérification des tables créées...")
    
    tables = [
        "delta.`s3a://healthcare-data/delta/dim_lieu`",
        "delta.`s3a://healthcare-data/delta/dim_patient`",
        "delta.`s3a://healthcare-data/delta/dim_date`",
        "delta.`s3a://healthcare-data/delta/dim_etablissement`",
        "delta.`s3a://healthcare-data/delta/dim_diagnostic`",
        "delta.`s3a://healthcare-data/delta/dim_indicateur`",
        "delta.`s3a://healthcare-data/delta/dim_professionel`",
        "delta.`s3a://healthcare-data/delta/fait_deces`",
        "delta.`s3a://healthcare-data/delta/fait_hospitalisation`",
        "delta.`s3a://healthcare-data/delta/fait_satisfaction`",
        "delta.`s3a://healthcare-data/delta/fait_consultations`"
    ]
    
    for table in tables:
        try:
            table_name = table.split("/")[-1].replace("`", "")
            result = spark.sql(f"DESCRIBE {table}")
            logger.info(f"✅ Table {table_name}: {result.count()} colonnes")
        except Exception as e:
            logger.error(f"❌ Erreur pour la table {table_name}: {str(e)}")

def main():
    """Fonction principale pour créer les tables Delta"""
    spark = create_spark_session()
    
    try:
        logger.info("🚀 Début de la création des tables Delta Healthcare...")
        
        # Créer toutes les tables
        create_delta_tables(spark)
        
        # Vérifier les tables créées
        verify_tables(spark)
        
        logger.info("🎉 Création des tables Delta terminée avec succès!")
        
    except Exception as e:
        logger.error(f"💥 Erreur lors de la création des tables: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()