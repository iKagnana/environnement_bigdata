FROM apache/hive:3.1.3

USER root

# Installation des outils nécessaires
RUN apt-get update && apt-get install -y \
    procps \
    netcat \
    postgresql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copier uniquement le driver PostgreSQL
COPY docker/hive/jars/postgresql-42.6.0.jar /opt/hive/lib/postgresql-42.6.0.jar

# IMPORTANT: Utiliser les versions compatibles avec Hive 3.1.3 (Hadoop 3.1.x)
# Télécharger hadoop-shaded-guava compatible
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar \
    -O /opt/hive/lib/hadoop-shaded-guava-1.1.1.jar

# Pour S3/MinIO, utiliser les versions Hadoop 3.1.0 (compatibles avec Hive 3.1.3)
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar \
    -O /opt/hive/lib/hadoop-aws-3.1.0.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
    -O /opt/hive/lib/aws-java-sdk-bundle-1.11.375.jar

# Supprimer les anciennes versions de guava qui causent des conflits
RUN rm -f /opt/hive/lib/guava-*.jar

# Script d'initialisation
COPY conf/scripts/init-hive-metastore.sh /init-hive-metastore.sh

# Convertir les fins de ligne et donner les permissions
RUN sed -i 's/\r$//' /init-hive-metastore.sh && \
    chmod +x /init-hive-metastore.sh

EXPOSE 9083

ENTRYPOINT ["/init-hive-metastore.sh"]