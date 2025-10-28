#!/bin/bash
set -e

echo "🚀 Initialisation du Hive Metastore..."

# Attendre que PostgreSQL soit prêt
echo "⏳ Attente de PostgreSQL..."
while ! nc -z ${POSTGRES_HOST} ${POSTGRES_PORT}; do
  sleep 1
done
echo "✅ PostgreSQL est prêt"

# Attendre un peu plus pour s'assurer que la base metastore est créée
sleep 10

# Configuration des variables d'environnement Hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_CONF_DIR=$HIVE_HOME/conf

# Initialiser le schéma de la base de données si nécessaire
echo "🔧 Initialisation du schéma Hive..."
if ! $HIVE_HOME/bin/schematool -dbType postgres -info > /dev/null 2>&1; then
    echo "📝 Création du schéma..."
    $HIVE_HOME/bin/schematool -dbType postgres -initSchema
else
    echo "✅ Le schéma existe déjà"
fi

echo "🚀 Démarrage du Hive Metastore..."
exec $HIVE_HOME/bin/hive --service metastore