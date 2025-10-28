# Importation des bibliothèques
import logging 
from pyspark.sql import SparkSession
from pyspark.sql import Row, types as T
from pyspark.sql import functions as F
from pyspark.sql.functions import ( 
    col, year, month, dayofmonth,
    monotonically_increasing_id, lit, datediff,
    current_date, floor, months_between ) 

# Configuration du logging 
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

# Chargement des tables silver
def load_silver_tables(spark):
    logger.info("____Chargement des tables Silver depuis MinIO...____")
    base_path = "s3a://healthcare-data/silver/"
    paths = {
        "consultations":            f"{base_path}silver_consultation_consultation",
        "diagnostic":               f"{base_path}silver_consultation_diagnostic",
        "patient":                  f"{base_path}silver_consultation_patient",
        "deces":                    f"{base_path}silver_deces_deces",
        "activite_professionel":    f"{base_path}silver_etablissement_sante_activite_professionnel_sante",
        "etablissement_sante":      f"{base_path}silver_etablissement_sante_etablissement_sante",
        "professionnel_sante":      f"{base_path}silver_etablissement_sante_professionnel_sante",
        "hospitalisation":          f"{base_path}silver_hospitalisation_hospitalisation",
        "region":                   f"{base_path}silver_localisation_regions",
        "communes":                 f"{base_path}silver_localisation_communes",
        "satisfaction_esatis":      f"{base_path}silver_satisfaction_esatis48h_2020",
        "satisfaction_esatisca":    f"{base_path}silver_satisfaction_esatisca_2020",
    }
    tables = {}
    for name, path in paths.items():
        try:
            df = spark.read.parquet(path)
            tables[name] = df
            logger.info(f"___Table Silver chargée : {name}___")
        except Exception as e:
            logger.warning(f"___Table absente ou illisible : {name} ({path}) — {str(e)}____")
    return tables

# Ecriture des DataFrames au format Delta dans S3
def write_delta(df, path, partition_col):
    writer = df.write.format("delta").mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
        writer.save(path)
        logger.info(f"___Table Delta écrite : {path}____")

# Construction des tables de dimensions
def build_dimension_lieu(t):
    logger.info("____Création de la dimension des lieux...____")
    dim_lieu = (
        t["communes"]
        .join(
            t["region"].select("code_region", "region"),
            on="code_region",
            how="left"
        )
        .select(
            monotonically_increasing_id().cast("integer").alias("lieu_id"),
            col("code_commune").alias("commune"),
            col("region")
        )
        .distinct()
    )
    write_delta(dim_lieu, "s3a://healthcare-data/gold/dim_lieu", "region")

def build_dimension_patient(t):
    logger.info("____Création de la dimension des patients...____")
    dim_patient = (
        t["patient"]
        .select(
            col("id_patient").cast("int").alias("patient_id"),
            col("sexe"),
            col("date_naissance"),
            floor(months_between(current_date(), col("date_naissance")) / 12).cast("int").alias("annee_naissance")
        )
        .distinct()
    )
    write_delta(dim_patient, "s3a://healthcare-data/gold/dim_patient", "annee_naissance")

def build_dimension_date(t):
    logger.info("____Création de la dimension des dates...____")
    # Extraction des colonnes de dates depuis les différentes sources
    df_hosp = t["hospitalisation"].select("date")
    df_consult = t["consultations"].select("date")
    df_deces = t["deces"].select(F.col("date_deces").alias("date"))
    df_patient = t["patient"].select(F.col("date_naissance").alias("date"))
    # Union de toutes les dates disponibles
    df_dates = df_hosp.union(df_consult).union(df_deces).union(df_patient).distinct()
    # Construction de la dimension date
    dim_date = (
        df_dates
        .withColumnRenamed("date", "date_complete")
        .withColumn("date_id", 
                    F.year("date_complete") * 10000 + 
                    F.month("date_complete") * 100 + 
                    F.dayofmonth("date_complete"))
        .withColumn("annee", F.year("date_complete"))
        .withColumn("mois", F.month("date_complete"))
        .withColumn("jour", F.dayofmonth("date_complete"))
        .withColumn("trimestre", F.quarter("date_complete"))
        .withColumn("semestre", F.when(F.month("date_complete") <= 6, F.lit(1)).otherwise(F.lit(2)))
        .dropDuplicates(["date_id"])
    )
    schema = T.StructType([
        T.StructField("date_complete", T.DateType(), True),
        T.StructField("date_id", T.IntegerType(), True),
        T.StructField("annee", T.IntegerType(), True),
        T.StructField("mois", T.IntegerType(), True),
        T.StructField("jour", T.IntegerType(), True),
        T.StructField("trimestre", T.IntegerType(), True),
        T.StructField("semestre", T.IntegerType(), True)
    ])
    # Création d'une ligne avec tous les champs NULL sauf annee = 2020 pour la satisfaction
    data_null_2020 = [(None, None, 2020, None, None, None, None)]
    spark = t["hospitalisation"].sparkSession
    df_null_2020 = spark.createDataFrame(data_null_2020, schema=schema)
    dim_date = dim_date.unionByName(df_null_2020)
    write_delta(dim_date, "s3a://healthcare-data/gold/dim_date", "annee")

def build_dimension_etablissement(t):
    logger.info("____Création de la dimension des établissements...____")
    dim_etablissement = (
        t["etablissement_sante"]
        .join(
            t["communes"].select("code_commune", "code_region"),
            on="code_commune",
            how="left"
        )
        .join(
            t["region"].select("code_region", "region"),
            on="code_region",
            how="left"
        )
        .select(
            col("id_etablissement").cast("integer").alias("etablissement_id"),
            col("raison_sociale").alias("raison_sociale"),
            col("code_commune").alias("commune"),
            col("region")
        )
        .distinct()
    )
    write_delta(dim_etablissement, "s3a://healthcare-data/gold/dim_etablissement", "region")

def build_dimension_diagnostic(t):
    logger.info("____Création de la dimension des diagnostics...____")
    dim_diagnostic = (
        t["consultations"]
        .select("code_diag", "motif")
        .join(
            t["diagnostic"].select("code_diag", "diagnostic"),
            on="code_diag",
            how="left"
        )
        .select(
            col("code_diag").alias("diagnostic_id"),
            col("diagnostic").alias("libelle_diagnostic"),
            col("motif").alias("categorie_diagnostic")
        )
        .distinct()
    )
    write_delta(dim_diagnostic, "s3a://healthcare-data/gold/dim_diagnostic", "categorie_diagnostic")

def build_dimension_professionnel(t):
    logger.info("____Création de la dimension des professionnels...____")
    dim_professionel = (
        t["professionnel_sante"]
        .select(
            col("id_prof_sante").cast("int").alias("professionnel_id"),
            "nom", 
            "prenom", 
            "civilite", 
            "profession", 
            "specialite"
        )
    )
    write_delta(dim_professionel, "s3a://healthcare-data/gold/dim_professionel", ["profession", "specialite"])

def build_dimension_indicateur(t):
    logger.info("____Création de la dimension des indicateurs...____")
    # Création manuelle des indicateurs basés sur les tables de satisfaction
    indicateurs = [
        Row(indicateur_id=1, libelle_indicateur="esatis48h"),
        Row(indicateur_id=2, libelle_indicateur="esatisca")
    ]
    spark = t["satisfaction_esatis"].sparkSession
    # Définition explicite du schéma pour garantir le type LongType
    schema = T.StructType([
        T.StructField("indicateur_id", T.IntegerType(), False),
        T.StructField("libelle_indicateur", T.StringType(), False)
    ])
    dim_indicateur = spark.createDataFrame(indicateurs, schema=schema)
    write_delta(dim_indicateur, "s3a://healthcare-data/gold/dim_indicateur", "libelle_indicateur")

def build_faits_deces(t):
    logger.info("____Création du fait des décès...____")
    fait_deces = (
        t["deces"]
        .withColumn("id_mort", monotonically_increasing_id().cast("int"))
        .withColumn("fk_date_deces", (year("date_deces")*10000 + month("date_deces")*100 + dayofmonth("date_deces")))
        .withColumn("fk_lieu_deces", col("code_commune").cast("int"))
        .withColumn("nb_deces", lit(1))
        .withColumn("annee", year("date_deces"))
        .select("id_mort","fk_date_deces", "fk_lieu_deces", "nb_deces", "annee")
    )
    write_delta(fait_deces, "s3a://healthcare-data/gold/fait_deces", "annee")

def build_faits_hospitalisation(t):
    logger.info("____Création du fait des hospitalisations...____")
    fait_hospitalisation = (
        t["hospitalisation"]
        .withColumn("fk_patient", col("id_patient"))
        .withColumn("fk_etablissement", col("id_etablissement").cast("int"))
        .withColumn("fk_diagnostic", col("code_diag"))
        .withColumn("fk_date_entree", (year("date")*10000 + month("date")*100 + dayofmonth("date")))
        .withColumn("duree_sejour_jours", col("duree"))
        .withColumn("nb_hospitalisations", lit(1))
        .withColumn("annee", year("date"))
        .select("fk_patient","fk_etablissement","fk_diagnostic","fk_date_entree","duree_sejour_jours","nb_hospitalisations","annee")
    )
    write_delta(fait_hospitalisation, "s3a://healthcare-data/gold/fait_hospitalisation", "annee")

def build_faits_satisfaction(t):
    logger.info("____Création du fait des satisfactions...____")
    spark = t["satisfaction_esatis"].sparkSession
    # Chargement des dimensions
    dim_indicateur = spark.read.format("delta").load("s3a://healthcare-data/gold/dim_indicateur")
    dim_date = spark.read.format("delta").load("s3a://healthcare-data/gold/dim_date")
    dim_etablissement = spark.read.format("delta").load("s3a://healthcare-data/gold/dim_etablissement")
    # Récupération du seul date_id de 2020
    date_2020_id = dim_date.filter(F.col("annee") == 2020).select("date_id").first()["date_id"]
    # Table réduite pour la FK établissement
    dim_etablissement_fk = dim_etablissement.select(
        F.col("etablissement_id"), F.col("raison_sociale")
    )
    # Transformation esatis (48h)
    fait_satisfaction_esatis = (
        t["satisfaction_esatis"]
        .withColumn("libelle_indicateur", F.lit("esatis48h"))
        .join(dim_indicateur.select("indicateur_id", "libelle_indicateur"),
            on="libelle_indicateur", how="inner")
        .join(dim_etablissement_fk,
            t["satisfaction_esatis"]["nom_etablissement"] == dim_etablissement_fk["raison_sociale"],
            "left")
        .select(
            F.col("etablissement_id").alias("fk_etablissement"),
            F.col("indicateur_id").alias("fk_indicateur"),
            F.lit(date_2020_id).cast("int").alias("fk_date_mesure"),
            F.col("score_all_rea_ajust").cast("float").alias("score_ajuste"),
            F.col("nb_rep_score_all_rea_ajust").alias("nombre_reponses"),
            F.col("taux_reco_brut").cast("float").alias("taux_participation")
        )
    )
    # Transformation esatisca (consultations anesthésie)
    fait_satisfaction_esatisca = (
        t["satisfaction_esatisca"]
        .withColumn("libelle_indicateur", F.lit("esatisca"))
        .join(dim_indicateur.select("indicateur_id", "libelle_indicateur"),
            on="libelle_indicateur", how="inner")
        .join(dim_etablissement_fk,
            t["satisfaction_esatisca"]["nom_etablissement"] == dim_etablissement_fk["raison_sociale"],
            "left")
        .select(
            F.col("etablissement_id").alias("fk_etablissement"),
            F.col("indicateur_id").alias("fk_indicateur"),
            F.lit(date_2020_id).cast("int").alias("fk_date_mesure"),
            F.col("score_all_ajust").cast("float").alias("score_ajuste"),
            F.col("nb_rep_score_all_ajust").alias("nombre_reponses"),
            F.col("taux_reco_brut").cast("float").alias("taux_participation")
        )
    )
    # Union propre et ajout de la partition
    fait_satisfaction = (
        fait_satisfaction_esatis
        .unionByName(fait_satisfaction_esatisca)
    )
    write_delta(fait_satisfaction, "s3a://healthcare-data/gold/fait_satisfaction", "fk_indicateur")

def build_faits_consultation(t):
    logger.info("____Création du fait des consultations...____")
    fait_consultations = (
        t["consultations"]
        .join(t["professionnel_sante"], "id_prof_sante", "left")
        .join(t["activite_professionel"], "id_prof_sante", "left")
        .withColumn("fk_patient", col("id_patient"))
        .withColumn("fk_professionnel", col("id_prof_sante").cast("int"))
        .withColumn("fk_date_consultation", (year("date")*10000 + month("date")*100 + dayofmonth("date")))
        .withColumn("fk_diagnostic", col("code_diag"))
        .withColumn("fk_etablissement", col("id_etablissement").cast("int"))
        .withColumn("nb_consultations", lit(1))
        .withColumn("annee", year("date"))
        .select("fk_patient","fk_professionnel","fk_date_consultation","fk_diagnostic","fk_etablissement","nb_consultations","annee")
    )
    write_delta(fait_consultations, "s3a://healthcare-data/gold/fait_consultations", "annee")

# Fonction principale
def main():
    spark = SparkSession.builder.appName("Healthcare Gold Tables").getOrCreate()
    logger.info("___Début du traitement Gold (MinIO)...___")
    t = load_silver_tables(spark)
    build_dimension_lieu(t)
    build_dimension_patient(t)
    build_dimension_date(t)
    build_dimension_etablissement(t)
    build_dimension_diagnostic(t)
    build_dimension_professionnel(t)
    build_dimension_indicateur(t)
    build_faits_deces(t)
    build_faits_hospitalisation(t)
    build_faits_satisfaction(t)
    build_faits_consultation(t)
    logger.info("___Traitement Gold terminé avec succès!___")
    spark.stop()

# Point d'entrée
if __name__ == "__main__":
    main()
