FROM apache/hive:3.1.3

USER root

# Copier les JARs nécessaires depuis le dossier local
COPY docker/hive/jars/postgresql-42.6.0.jar /opt/hive/lib/postgresql-42.6.0.jar
COPY docker/hive/jars/hadoop-aws-3.3.4.jar /opt/hive/lib/hadoop-aws-3.3.4.jar
COPY docker/hive/jars/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar

# Installation des outils nécessaires
RUN apt-get update && apt-get install -y \
    procps \
    netcat \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Script d'initialisation
COPY conf/scripts/init-hive-metastore.sh /init-hive-metastore.sh

# IMPORTANT: Convertir les fins de ligne avec sed et donner les permissions
RUN sed -i 's/\r$//' /init-hive-metastore.sh && \
    chmod +x /init-hive-metastore.sh

EXPOSE 9083

ENTRYPOINT ["/init-hive-metastore.sh"]