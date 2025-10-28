-- Création de la base de données pour le metastore Hive
CREATE DATABASE metastore;

-- Création d'un utilisateur spécifique pour Hive (optionnel)
CREATE USER hive WITH PASSWORD 'hive123';
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
GRANT ALL PRIVILEGES ON DATABASE healthcare_data TO hive;

-- Donner les permissions nécessaires
\c metastore;
GRANT ALL ON SCHEMA public TO hive;
GRANT ALL ON ALL TABLES IN SCHEMA public TO hive;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO hive;

\c healthcare_data;
GRANT ALL ON SCHEMA public TO hive;
GRANT ALL ON ALL TABLES IN SCHEMA public TO hive;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO hive;