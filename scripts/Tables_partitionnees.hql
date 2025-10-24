---------------------------------
-- Tables de dimensions
---------------------------------

-- Dimension des lieux
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_lieu_partitionnee` (
    lieu_id INT COMMENT 'Identifiant du lieu',                              -- code_lieu_deces (deces.csv)
    commune STRING COMMENT 'Nom de la commune',                             -- code_commune (etablissement_sante.csv)
    departement STRING COMMENT 'Code du département',                       -- À enlever si inutile (optionnel)
    region STRING COMMENT 'Région administrative',                          -- region (ESATIS48H_MCO_recueil2017_donnees)
    pays STRING COMMENT 'Pays'                                              -- pays (etablissement_sante.csv)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 's3a://chu/delta/dim_lieu_partitionnee';

-- Dimension des patients
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_patient_partitionnee` (
    patient_id INT COMMENT 'Identifiant du patient',                            -- id_patient (Hospitalisations.csv), Patient.Id_patient
    sexe STRING COMMENT 'Sexe du patient',                                      -- Patient.Sexe
    date_naissance DATE COMMENT 'Date de naissance',                            -- Patient.Date
)
USING DELTA
PARTITIONED BY (date_naissance)
LOCATION 's3a://chu/delta/dim_patient_partitionnee';

-- Dimension des dates
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_date_partitionnee` (
    date_id INT COMMENT 'Identifiant de la date',                           -- Générer séquentiellement type YYYYMMDD
    date_complete DATE COMMENT 'Date complète',                             -- format 'YYYY-MM-DD', date_entree (Hospitalisation.csv), date_deces (deces.csv)
    annee INT COMMENT 'Année',                                              -- Extraire l'année
    mois INT COMMENT 'Mois',                                                -- Extraire le mois
    jour INT COMMENT 'Jour',                                                -- Extraire le jour
    trimestre INT COMMENT 'Trimestre',                                      -- Extraire le trimestre
    semestre INT COMMENT 'Semestre'                                         -- Extraire le semestre
)   
USING DELTA
PARTITIONED BY (annee)
LOCATION 's3a://chu/delta/dim_date_partitionnee';

-- Dimension des établissements de santé
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_etablissement_partitionnee` (
    etablissement_id INT COMMENT 'Identifiant établissement',                       -- siren_site (etablissement_sante.csv)
    raison_sociale STRING COMMENT 'Raison sociale',                                 -- raison_socialde_site (etablissement_sante.csv)
    adresse STRING COMMENT 'Adresse postale',                                       -- adresse (etablissement_sante.csv)
    commune STRING COMMENT 'Commune',                                               -- code_commune (etablissement_sante.csv)
    code_postal STRING COMMENT 'Code postal',                                       -- code_postal (etablissement_sante.csv)
    region STRING COMMENT 'Région'                                                  -- region (ESATIS48H_MCO_recueil2017_donnees)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 's3a://chu/delta/dim_etablissement_partitionnee';

-- Dimension des diagnostics
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_diagnostic_partitionnee` (
    diagnostic_id STRING COMMENT 'Code diagnostic',                             -- code_diagnostic (Hospitalisation.csv), Diagnostic.Code_diag
    libelle_diagnostic STRING COMMENT 'Libellé du diagnostic',                  -- suite_diagnostic_consultation (Hospitalisation.csv), Diagnostic.Diagnostic
    categorie_diagnostic STRING COMMENT 'Catégorie du diagnostic'               -- Consultation.Motif
)   
USING DELTA
PARTITIONED BY (categorie_diagnostic)
LOCATION 's3a://chu/delta/dim_diagnostic_partitionnee';

-- Dimension des indicateurs de satisfaction
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_indicateur_partitionnee` (
    indicateur_id INT COMMENT 'Identifiant indicateur',             -- NAME (Fichiers lexique)
    libelle_indicateur STRING COMMENT 'Libellé de l’indicateur',    -- LABEL (Fichiers lexique)
    categorie_indicateur STRING COMMENT 'Catégorie de l’indicateur' -- à construire en sélectionnant parmi les noms de fichiers lexique la chaîne entre "lexique-" et "-open"
)
USING DELTA
PARTITIONED BY (categorie_indicateur)
LOCATION 's3a://chu/delta/dim_indicateur_partitionnee';

-- Dimension des professionnels de santé
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/dim_professionel_partitionnee` (
    professionnel_id INT COMMENT 'Identifiant professionnel de santé',  -- identifiant (professionnel_sante.csv), Professionnel_de_sante.Identifiant
    nom STRING COMMENT 'Nom',                                           -- nom (professionnel_sante.csv), Professionnel_de_sante.Nom
    prenom STRING COMMENT 'Prénom',                                     -- prenom (professionnel_sante.csv), Professionnel_de_sante.Prenom
    civilite STRING COMMENT 'Civilité',                                 -- civilite (professionnel_sante.csv), Professionnel_de_sante.Civilite
    profession STRING COMMENT 'Profession',                             -- profession (professionnel_sante.csv), Specialites.Fonction
    specialite STRING COMMENT 'Spécialité'                              -- specialite (professionnel_sante.csv), Specialites.Specialite
)
USING DELTA
PARTITIONED BY (profession)
LOCATION 's3a://chu/delta/dim_professionel_partitionnee';

-----------------
-- Table de faits
-----------------

-- Table de faits des décès
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/fait_deces_partitionnee` (
    fk_patient INT COMMENT 'Clé étrangère patient',                 -- id_patient (Hospitalisation.csv)
    fk_date_deces INT COMMENT 'Clé étrangère date de décès',        -- date_deces (deces.csv)
    fk_lieu_deces INT COMMENT 'Clé étrangère lieu de décès',        -- code_lieu_deces (deces.csv)
    fk_lieu_naissance INT COMMENT 'Clé étrangère lieu de naissance',-- lieu_naissance (deces.csv)
    nb_deces INT COMMENT 'Nombre de décès agrégé'                   -- mesure permettant d'optimiser les analyses (un décès = 1)
)   
USING DELTA
PARTITIONED BY (annee)
LOCATION 's3a://chu/delta/fait_deces_partitionnee';

-- Table de faits des hospitalisations
Create TABLE IF NOT EXISTS delta.`s3a://chu/delta/fait_hospitalisation_partitionnee`(
    fk_patient INT COMMENT 'Clé étrangère patient',                         -- id_patient (Hospitalisation.csv)
    fk_etablissement INT COMMENT 'Clé étrangère établissement',             -- siren_site (etablissement_sante.csv)
    fk_diagnostic STRING COMMENT 'Clé étrangère diagnostic',                -- code_diagnostic (Hospitalisation.csv)
    fk_date_entree INT COMMENT 'Clé étrangère date d’entrée',               -- date_entree (Hospitalisation.csv)
    duree_sejour_jours INT COMMENT 'Durée du séjour (en jours)',            -- jour_hospitalisation (Hospitalisation.csv) 
    nb_hospitalisations INT COMMENT 'Nombre d’hospitalisations (agrégé)'    -- Calculer le nombre d'hospitalisations
)
USING DELTA
PARTITIONED BY (annee)
LOCATION 's3a://chu/delta/fait_hospitalisation_partitionnee';

-- Table de faits de la satisfaction des patients
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/fait_satisfaction_partitionnee` (
    fk_etablissement INT COMMENT 'Clé étrangère établissement',         -- siren_site (etablissement_sante.csv)
    fk_indicateur INT COMMENT 'Clé étrangère indicateur',               -- NAME (Fichiers lexique)
    fk_date_mesure INT COMMENT 'Clé étrangère date de mesure',          -- année de mesure à récupérer dans le nom du fichier (Fichiers lexique)
    score_ajuste FLOAT COMMENT 'Score ajusté de satisfaction',          -- score_all_rea_ajust (Fichiers donnees)
    nombre_reponses INT COMMENT 'Nombre total de réponses',             -- nb_rep_score_all_rea_ajust (Fichiers donnees)
    taux_participation FLOAT COMMENT 'Taux de participation (%)'        -- taux_reco_brut (Fichiers donnees)
)   
USING DELTA
PARTITIONED BY (annee)
LOCATION 's3a://chu/delta/fait_satisfaction_partitionnee';

-- Table de faits des consultations
CREATE TABLE IF NOT EXISTS delta.`s3a://chu/delta/fait_consultations_partitionnee` (
    fk_patient INT COMMENT 'Clé étrangère patient',                                         -- id_patient (Hospitalisation.csv)
    fk_professionnel INT COMMENT 'Clé étrangère professionnel de santé',                    -- identifiant (professionnel_sante.csv)
    fk_date_consultation INT COMMENT 'Clé étrangère date de consultation',                  -- Consultation.Date
    fk_diagnostic STRING COMMENT 'Clé étrangère diagnostic',                                -- code_diagnostic (Hospitalisation.csv)
    fk_etablissement INT COMMENT 'Clé étrangère établissement',                             -- siren_site (etablissement_sante.csv)
    duree_consultation_minutes INT COMMENT 'Durée moyenne des consultations (en minutes)',  -- Calculer à partir de Consultation.Heure_fin - Consultation.Heure_debut
    nb_consultations INT COMMENT 'Nombre total de consultations'                            -- Calculer le nombre de consultations
)   
USING DELTA
PARTITIONED BY (annee)
LOCATION 's3a://chu/delta/fait_consultations_partitionnee';
