import os
import sys
import logging
from urllib.parse import urlparse
import glob
from pathlib import Path
import re

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
    if not endpoint:
        return "minio:9000", False
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    host = f"{parsed.hostname}:{parsed.port or 9000}"
    secure = parsed.scheme == "https"
    return host, secure

def create_spark_session():
    """Créer une session Spark configurée pour MinIO (s3a)"""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

    host_port, secure = _parse_endpoint(endpoint)

    builder = SparkSession.builder \
        .appName("CSV to Parquet Converter - Healthcare Data") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{host_port}" if not secure else f"https://{host_port}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if secure else "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    return builder.getOrCreate()

def create_minio_bucket(bucket_name="healthcare-data"):
    """Créer le bucket MinIO si nécessaire"""
    try:
        from minio import Minio
    except Exception as e:
        logger.warning("minio SDK non installé dans l'image : %s", e)
        return

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

    host_port, secure = _parse_endpoint(endpoint)
    
    try:
        client = Minio(host_port, access_key=access_key, secret_key=secret_key, secure=secure)
        if not client.bucket_exists(bucket_name):
            logger.info("Création du bucket MinIO: %s", bucket_name)
            client.make_bucket(bucket_name)
        else:
            logger.info("Bucket MinIO déjà existant: %s", bucket_name)
    except Exception as e:
        logger.warning("Impossible de créer ou vérifier le bucket '%s' : %s", bucket_name, e)

def clean_table_name(table_key):
    """
    Nettoie et valide un nom de table pour éviter les erreurs PySpark
    
    Args:
        table_key (str): Nom de table brut
        
    Returns:
        str: Nom de table nettoyé et valide
    """
    if not table_key:
        return "unknown_table"
    
    # Convertir en string au cas où
    table_key = str(table_key).lower()
    
    # Remplacer les espaces et caractères spéciaux
    table_key = table_key.replace(" ", "_").replace("-", "_").replace(".", "_")
    
    # Enlever tous les caractères non alphanumériques sauf underscore
    table_key = re.sub(r'[^a-zA-Z0-9_]', '_', table_key)
    
    # Enlever les chiffres en début de nom
    table_key = re.sub(r'^[^a-zA-Z]+', '', table_key)
    
    # Remplacer les underscores multiples par un seul
    table_key = re.sub(r'_+', '_', table_key)
    
    # Enlever les underscores en début et fin
    table_key = table_key.strip('_')
    
    # S'assurer qu'on a un nom valide qui commence par une lettre
    if not table_key or not table_key[0].isalpha():
        table_key = f"table_{table_key}" if table_key else "unknown_table"
    
    return table_key

def discover_files_to_convert(base_path="/opt/app/datas", file_extension="csv"):
    """
    Découvre automatiquement tous les fichiers à convertir dans un dossier
    
    Args:
        base_path (str): Chemin de base pour la recherche
        file_extension (str): Extension des fichiers à chercher (csv, json, etc.)
    
    Returns:
        dict: Configuration pour chaque fichier trouvé
    """
    files_config = {}
    
    # Pattern de recherche unique et récursif
    search_pattern = f"{base_path}/**/*.{file_extension}"
    
    logger.info(f"🔍 Recherche des fichiers .{file_extension} dans {base_path}...")
    
    # Utiliser un set pour éviter les doublons
    files_found = set(glob.glob(search_pattern, recursive=True))
    
    logger.info(f"📊 {len(files_found)} fichiers uniques trouvés")
    
    for file_path in files_found:
        try:
            logger.info(f"🔍 Traitement: {file_path}")
            
            path_obj = Path(file_path)
            
            # Créer une clé unique basée sur la structure des dossiers
            relative_path = path_obj.relative_to(base_path)
            parent_dirs = relative_path.parent.parts
            file_name = path_obj.stem.lower()
            
            logger.info(f"   📁 Parent dirs: {parent_dirs}")
            logger.info(f"   📄 File name: {file_name}")
            
            # Créer un nom de table unique
            if parent_dirs:
                table_key = "_".join([str(d) for d in parent_dirs] + [file_name]).lower()
            else:
                table_key = file_name
            
            logger.info(f"   🔑 Table key (avant nettoyage): {table_key}")
            
            # Nettoyer le nom de table AVANT toute utilisation
            table_key = clean_table_name(table_key)
            
            logger.info(f"   ✨ Table key (après nettoyage): {table_key}")
            
            # Éviter les doublons
            original_key = table_key
            counter = 1
            while table_key in files_config:
                table_key = f"{original_key}_{counter}"
                counter += 1
                logger.info(f"   ⚠️ Doublon détecté, nouveau nom: {table_key}")
            
            # IMPORTANT: S'assurer que table_key est bien une string
            table_key = str(table_key)
            
            # Calculer la taille du fichier de manière sécurisée
            try:
                file_size_bytes = os.path.getsize(file_path)
                # Utiliser le round natif de Python (pas celui de PySpark)
                file_size_mb = __builtins__.round(file_size_bytes / (1024 * 1024), 2)
            except (OSError, IOError) as size_error:
                logger.warning(f"⚠️ Impossible de calculer la taille de {file_path}: {size_error}")
                file_size_mb = 0.0
            
            # Construire les chemins en s'assurant que tout est string
            source_path_str = str(file_path)
            target_path_str = f"s3a://healthcare-data/silver/{table_key}/"
            
            files_config[table_key] = {
                "source_path": source_path_str,
                "target_path": target_path_str,
                "table_name": table_key,
                "original_path": source_path_str,
                "file_size_mb": file_size_mb
            }
            
            logger.info(f"✅ Fichier enregistré: {table_key} ({file_size_mb} MB)")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement de {file_path}: {str(e)}")
            import traceback
            logger.error(f"Traceback complet:\n{traceback.format_exc()}")
            continue
    
    if not files_config:
        logger.warning("⚠️ Aucun fichier trouvé. Diagnostic:")
        for root, dirs, files in os.walk(base_path):
            matching_files = [f for f in files if f.endswith(f'.{file_extension}')]
            if matching_files:
                logger.warning(f"   📁 {root}: {matching_files}")
    
    logger.info(f"📦 Total de fichiers configurés: {len(files_config)}")
    return files_config

def convert_file_to_parquet(spark, source_path, target_path, table_name, file_format="csv"):
    """
    Convertit un fichier source en format Parquet et le sauvegarde dans MinIO
    
    Args:
        spark: SparkSession
        source_path (str): Chemin du fichier source
        target_path (str): Chemin cible dans MinIO
        table_name (str): Nom de la table
        file_format (str): Format du fichier source (csv, json, parquet, etc.)
    
    Returns:
        bool: True si conversion réussie, False sinon
    """
    try:
        # Convertir tous les paramètres en string de manière défensive
        source_path = str(source_path) if source_path is not None else ""
        target_path = str(target_path) if target_path is not None else ""
        table_name = str(table_name) if table_name is not None else "unknown_table"
        file_format = str(file_format) if file_format is not None else "csv"
        
        logger.info(f"🔄 Début de la conversion pour {table_name}")
        logger.info(f"   Source: {source_path}")
        logger.info(f"   Cible: {target_path}")
        
        # Vérifier l'existence du fichier source
        if not os.path.exists(source_path):
            logger.error(f"❌ Fichier source non trouvé: {source_path}")
            return False
        
        # Configuration de lecture selon le format
        if file_format.lower() == "csv":
            try:
                df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("multiline", "true") \
                    .option("escape", '"') \
                    .option("quote", '"') \
                    .option("ignoreLeadingWhiteSpace", "true") \
                    .option("ignoreTrailingWhiteSpace", "true") \
                    .csv(source_path)
            except Exception as read_error:
                logger.warning(f"⚠️ Erreur avec inferSchema, tentative sans: {str(read_error)[:100]}...")
                # Fallback sans inférence de schéma
                df = spark.read \
                    .option("header", "true") \
                    .option("multiline", "true") \
                    .option("escape", '"') \
                    .option("quote", '"') \
                    .csv(source_path)
        
        elif file_format.lower() == "json":
            df = spark.read \
                .option("multiline", "true") \
                .json(source_path)
        
        elif file_format.lower() == "parquet":
            df = spark.read.parquet(source_path)
        
        else:
            logger.error(f"❌ Format non supporté: {file_format}")
            return False
        
        # Ajout de métadonnées de traçabilité avec gestion d'erreurs
        try:
            df_with_metadata = df \
                .withColumn("conversion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(source_path)) \
                .withColumn("table_name", lit(table_name))
        except Exception as metadata_error:
            logger.warning(f"⚠️ Erreur lors de l'ajout des métadonnées pour {table_name}: {metadata_error}")
            # Fallback sans métadonnées
            df_with_metadata = df
        
        # Vérifier que le DataFrame n'est pas vide
        try:
            row_count = df_with_metadata.count()
            if row_count == 0:
                logger.warning(f"⚠️ Le fichier {table_name} est vide")
                return False
        except Exception as count_error:
            logger.warning(f"⚠️ Impossible de compter les lignes de {table_name}: {count_error}")
            row_count = -1
        
        # Optimisation : repartitionnement si nécessaire
        if row_count > 0:
            if row_count > 1000000:  # Plus d'1M de lignes
                df_with_metadata = df_with_metadata.repartition(8)
            elif row_count > 100000:  # Plus de 100K lignes
                df_with_metadata = df_with_metadata.repartition(4)
            else:
                df_with_metadata = df_with_metadata.coalesce(1)
        
        # Sauvegarde en Parquet dans MinIO
        logger.info(f"💾 Sauvegarde en cours vers MinIO...")
        df_with_metadata.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(target_path)
        
        # Statistiques
        column_count = len(df_with_metadata.columns)
        logger.info(f"✅ Conversion réussie pour {table_name}")
        logger.info(f"   📊 {row_count:,} lignes, {column_count} colonnes")
        logger.info(f"   📍 Localisation: {target_path}")
        
        # Aperçu des données (limité pour éviter les erreurs d'affichage)
        logger.info("👀 Aperçu des données:")
        try:
            df_with_metadata.show(3, truncate=True)
        except Exception as show_error:
            logger.warning(f"⚠️ Impossible d'afficher l'aperçu: {str(show_error)[:50]}...")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la conversion de {str(table_name)}: {str(e)}")
        import traceback
        logger.debug(f"Traceback complet: {traceback.format_exc()}")
        return False

def convert_folder_to_parquet(source_folder="/opt/app/datas", file_extension="csv", 
                            target_bucket="healthcare-data"):
    """
    Fonction principale pour convertir tous les fichiers d'un dossier en Parquet
    
    Args:
        source_folder (str): Dossier source contenant les fichiers
        file_extension (str): Extension des fichiers à convertir
        target_bucket (str): Nom du bucket MinIO cible
    
    Returns:
        dict: Statistiques de conversion
    """
    
    # Créer le bucket MinIO
    create_minio_bucket(target_bucket)
    
    # Créer la session Spark
    spark = create_spark_session()
    
    stats = {
        "total_files": 0,
        "successful_conversions": 0,
        "failed_conversions": 0,
        "files_processed": []
    }
    
    try:
        # Découvrir tous les fichiers à convertir
        files_config = discover_files_to_convert(source_folder, file_extension)
        stats["total_files"] = len(files_config)
        
        if not files_config:
            logger.error("❌ Aucun fichier à convertir trouvé")
            return stats
        
        logger.info(f"🚀 Début de la conversion de {len(files_config)} fichiers {file_extension.upper()} vers Parquet")
        
        # Convertir chaque fichier
        for table_name, config in files_config.items():
            logger.info(f"📄 Traitement de {table_name} ({stats['successful_conversions'] + stats['failed_conversions'] + 1}/{stats['total_files']})")
            
            success = convert_file_to_parquet(
                spark=spark,
                source_path=config["source_path"],
                target_path=config["target_path"],
                table_name=table_name,
                file_format=file_extension
            )
            
            if success:
                stats["successful_conversions"] += 1
                stats["files_processed"].append({
                    "table": table_name,
                    "status": "SUCCESS",
                    "source": config["source_path"],
                    "target": config["target_path"],
                    "size_mb": config["file_size_mb"]
                })
            else:
                stats["failed_conversions"] += 1
                stats["files_processed"].append({
                    "table": table_name,
                    "status": "FAILED",
                    "source": config["source_path"],
                    "error": "Conversion failed"
                })
        
        # Résumé final
        logger.info("=" * 50)
        logger.info("🎉 RÉSUMÉ DE LA CONVERSION")
        logger.info("=" * 50)
        logger.info(f"📊 Total de fichiers: {stats['total_files']}")
        logger.info(f"✅ Conversions réussies: {stats['successful_conversions']}")
        logger.info(f"❌ Conversions échouées: {stats['failed_conversions']}")
        
        if stats['total_files'] > 0:
            success_rate = (stats['successful_conversions']/stats['total_files']*100)
            logger.info(f"📈 Taux de réussite: {success_rate:.1f}%")
        
        # Détail des fichiers traités
        for file_info in stats["files_processed"]:
            status_icon = "✅" if file_info["status"] == "SUCCESS" else "❌"
            size_info = f"{file_info.get('size_mb', 'N/A')} MB" if file_info["status"] == "SUCCESS" else "Failed"
            logger.info(f"{status_icon} {file_info['table']}: {size_info}")
        
        return stats
        
    except Exception as e:
        logger.error(f"💥 Erreur générale: {str(e)}")
        stats["failed_conversions"] = stats["total_files"]
        return stats
    finally:
        if spark:
            spark.stop()

def main():
    """Point d'entrée principal"""
    logger.info("🚀 Démarrage du convertisseur CSV vers Parquet")
    
    # Configuration (peut être modifiée selon vos besoins)
    source_folder = os.environ.get("SOURCE_FOLDER", "/opt/app/datas")
    file_extension = os.environ.get("FILE_EXTENSION", "csv")
    target_bucket = os.environ.get("TARGET_BUCKET", "healthcare-data")
    
    logger.info(f"📁 Dossier source: {source_folder}")
    logger.info(f"📄 Extension recherchée: {file_extension}")
    logger.info(f"🪣 Bucket cible: {target_bucket}")
    
    # Lancer la conversion
    stats = convert_folder_to_parquet(
        source_folder=source_folder,
        file_extension=file_extension,
        target_bucket=target_bucket
    )
    
    # Sortir avec le code approprié
    if stats["failed_conversions"] == 0 and stats["successful_conversions"] > 0:
        logger.info("🎉 Toutes les conversions ont réussi!")
        sys.exit(0)
    elif stats["total_files"] == 0:
        logger.warning("⚠️ Aucun fichier trouvé à convertir")
        sys.exit(0)
    else:
        logger.error(f"⚠️ {stats['failed_conversions']} conversions ont échoué sur {stats['total_files']}")
        sys.exit(1)

if __name__ == "__main__":
    main()