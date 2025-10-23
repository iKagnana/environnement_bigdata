---------------------------------
-- Requêtes analytiques demandées
---------------------------------

-- Taux de consultation des patients dans un établissement X sur une période Y
SELECT
    fk_etablissement,
    COUNT(DISTINCT fk_patient) / COUNT(*) * 100 AS taux_consultation
FROM delta.`s3a://chu/delta/fait_consultations`
WHERE annee BETWEEN 2020 AND 2023
    AND fk_etablissement = <id_etablissement>
GROUP BY fk_etablissement;

-- Taux de consultation par rapport à un diagnostic X sur une période Y
SELECT
    fk_diagnostic,
    COUNT(DISTINCT fk_patient) / COUNT(*) * 100 AS taux_consult_diag
FROM delta.`s3a://chu/delta/fait_consultations`
WHERE fk_diagnostic = '<code_diag>'
    AND annee BETWEEN 2020 AND 2023
GROUP BY fk_diagnostic;

-- Taux global d’hospitalisation sur une période Y
SELECT
    annee,
    COUNT(DISTINCT fk_patient) / COUNT(*) * 100 AS taux_hosp_global
FROM delta.`s3a://chu/delta/fait_hospitalisation`
WHERE annee BETWEEN 2020 AND 2023
GROUP BY annee;

-- Taux d’hospitalisation par diagnostic
SELECT
    fk_diagnostic,
    COUNT(DISTINCT fk_patient) / COUNT(*) * 100 AS taux_hosp_diagnostic
FROM delta.`s3a://chu/delta/fait_hospitalisation`
WHERE annee BETWEEN 2020 AND 2023
GROUP BY fk_diagnostic;

-- Taux d’hospitalisation par sexe et tranche d’âge
SELECT
    p.sexe,
    FLOOR(DATEDIFF(CURRENT_DATE(), p.date_naissance) / 365 / 10) * 10 AS tranche_age,
    COUNT(DISTINCT h.fk_patient) / COUNT(*) * 100 AS taux_hosp
FROM delta.`s3a://chu/delta/fait_hospitalisation` h
JOIN delta.`s3a://chu/delta/dim_patient` p ON h.fk_patient = p.patient_id
GROUP BY p.sexe, FLOOR(DATEDIFF(CURRENT_DATE(), p.date_naissance) / 365 / 10) * 10;

-- Taux de consultation par professionnel
SELECT
    fk_professionnel,
    COUNT(DISTINCT fk_patient) / COUNT(*) * 100 AS taux_consult_pro
FROM delta.`s3a://chu/delta/fait_consultations`
GROUP BY fk_professionnel;

-- Nombre de décès par région sur l'année 2019
SELECT
    l.region,
    COUNT(*) AS nb_deces
FROM delta.`s3a://chu/delta/fait_deces` d
JOIN delta.`s3a://chu/delta/dim_lieu` l ON d.fk_lieu_deces = l.lieu_id
WHERE annee = 2019
GROUP BY l.region;

-- Taux global de satisfaction par région sur 2020
SELECT
    e.region,
    AVG(score_ajuste) AS taux_satisfaction
FROM delta.`s3a://chu/delta/fait_satisfaction` s
JOIN delta.`s3a://chu/delta/dim_etablissement` e ON s.fk_etablissement = e.etablissement_id
WHERE s.annee = 2020
GROUP BY e.region;