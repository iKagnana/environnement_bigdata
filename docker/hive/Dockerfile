FROM apache/hive:3.1.3

USER root

# Copier le driver PostgreSQL depuis le dossier local
COPY docker/hive/jars/postgresql-42.6.0.jar /opt/hive/lib/postgresql-42.6.0.jar

# Installation des outils nécessaires
RUN apt-get update && apt-get install -y \
    procps \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Script d'initialisation
COPY conf/scripts/init-hive-metastore.sh /init-hive-metastore.sh
RUN chmod +x /init-hive-metastore.sh

EXPOSE 9083

ENTRYPOINT ["/init-hive-metastore.sh"]