# Image de base avec Delta Lake pour le job runner
FROM apache/spark:3.4.1

USER root

# outils utiles
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    python3-pip \
  && rm -rf /var/lib/apt/lists/*

# Install MinIO client (mc) and python minio lib
RUN curl -fsSL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc \
  && chmod +x /usr/local/bin/mc \
  && pip3 install --no-cache-dir minio

# Répertoires
RUN mkdir -p /opt/app /opt/jars /opt/spark/scripts
WORKDIR /opt/app

# Télécharger Delta Lake JAR compatible avec Spark 3.4.1 et Scala 2.12
# Version 2.4.0 est compatible avec Spark 3.4.x
RUN curl -fsSL -o /opt/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
  && curl -fsSL -o /opt/spark/jars/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Copie datasets
COPY datas/ /opt/app/datas/

# Copier les jobs depuis le contexte (fallback si vous ne montez pas le repo)
COPY spark/jobs /opt/app/spark/jobs
# Copier le script d'exécution présent dans le contexte docker/spark/
COPY spark/jobs/run_jobs.sh /opt/spark/scripts/run_jobs.sh
RUN chmod +x /opt/spark/scripts/run_jobs.sh

# Variables par défaut (peuvent être surchargées par docker-compose)
ENV SPARK_MASTER_URL=spark://spark-master:7077 \
    MINIO_ENDPOINT=http://minio:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin123

# ENTRYPOINT uniquement pour le job runner
ENTRYPOINT ["/opt/spark/scripts/run_jobs.sh"]