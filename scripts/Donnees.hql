-----------------------------------------------
-- Chargement des données dans les tables Delta
-----------------------------------------------

-- dim_lieu
CREATE OR REPLACE TEMPORARY VIEW staging_lieu
USING parquet OPTIONS (path 's3a://chu/staging/lieu.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_lieu` SELECT * FROM staging_lieu;
INSERT INTO delta.`s3a://chu/delta/dim_lieu_partitionnee` SELECT * FROM staging_lieu;

-- dim_patient
CREATE OR REPLACE TEMPORARY VIEW staging_patient
USING parquet OPTIONS (path 's3a://chu/staging/patient.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_patient` SELECT * FROM staging_patient;
INSERT INTO delta.`s3a://chu/delta/dim_patient_partitionnee` SELECT * FROM staging_patient;

-- dim_date
CREATE OR REPLACE TEMPORARY VIEW staging_date
USING parquet OPTIONS (path 's3a://chu/staging/date.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_date` SELECT * FROM staging_date;
INSERT INTO delta.`s3a://chu/delta/dim_date_partitionnee` SELECT * FROM staging_date;

-- dim_etablissement
CREATE OR REPLACE TEMPORARY VIEW staging_etablissement
USING parquet OPTIONS (path 's3a://chu/staging/etablissement.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_etablissement` SELECT * FROM staging_etablissement;
INSERT INTO delta.`s3a://chu/delta/dim_etablissement_partitionnee` SELECT * FROM staging_etablissement;

-- dim_diagnostic
CREATE OR REPLACE TEMPORARY VIEW staging_diagnostic
USING parquet OPTIONS (path 's3a://chu/staging/diagnostic.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_diagnostic` SELECT * FROM staging_diagnostic;
INSERT INTO delta.`s3a://chu/delta/dim_diagnostic_partitionnee` SELECT * FROM staging_diagnostic;

-- dim_indicateur
CREATE OR REPLACE TEMPORARY VIEW staging_indicateur
USING parquet OPTIONS (path 's3a://chu/staging/indicateur.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_indicateur` SELECT * FROM staging_indicateur;
INSERT INTO delta.`s3a://chu/delta/dim_indicateur_partitionnee` SELECT * FROM staging_indicateur;

-- dim_professionel
CREATE OR REPLACE TEMPORARY VIEW staging_professionel
USING parquet OPTIONS (path 's3a://chu/staging/professionel.parquet');
INSERT INTO delta.`s3a://chu/delta/dim_professionel` SELECT * FROM staging_professionel;
INSERT INTO delta.`s3a://chu/delta/dim_professionel_partitionnee` SELECT * FROM staging_professionel;

-- fait_deces
CREATE OR REPLACE TEMPORARY VIEW staging_deces
USING parquet OPTIONS (path 's3a://chu/staging/fait_deces.parquet');
INSERT INTO delta.`s3a://chu/delta/fait_deces` SELECT * FROM staging_deces;
INSERT INTO delta.`s3a://chu/delta/fait_deces_partitionnee` SELECT * FROM staging_deces;

-- fait_hospitalisation
CREATE OR REPLACE TEMPORARY VIEW staging_hosp
USING parquet OPTIONS (path 's3a://chu/staging/fait_hospitalisation.parquet');
INSERT INTO delta.`s3a://chu/delta/fait_hospitalisation` SELECT * FROM staging_hosp;
INSERT INTO delta.`s3a://chu/delta/fait_hospitalisation_partitionnee` SELECT * FROM staging_hosp;

-- fait_satisfaction
CREATE OR REPLACE TEMPORARY VIEW staging_satisfaction
USING parquet OPTIONS (path 's3a://chu/staging/fait_satisfaction.parquet');
INSERT INTO delta.`s3a://chu/delta/fait_satisfaction` SELECT * FROM staging_satisfaction;
INSERT INTO delta.`s3a://chu/delta/fait_satisfaction_partitionnee` SELECT * FROM staging_satisfaction;

-- fait_consultations
CREATE OR REPLACE TEMPORARY VIEW staging_consult
USING parquet OPTIONS (path 's3a://chu/staging/fait_consultations.parquet');
INSERT INTO delta.`s3a://chu/delta/fait_consultations` SELECT * FROM staging_consult;
INSERT INTO delta.`s3a://chu/delta/fait_consultations_partitionnee` SELECT * FROM staging_consult;

------------------------------------------
-- Vérification de la présence des données
------------------------------------------
SELECT * FROM delta.`s3a://chu/delta/dim_lieu` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/dim_patient` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/dim_date` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/dim_etablissement` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/dim_diagnostic` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/dim_indicateur` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/dim_professionel` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/fait_deces` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/fait_hospitalisation` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/fait_satisfaction` LIMIT 100;

SELECT * FROM delta.`s3a://chu/delta/fait_consultations` LIMIT 100;