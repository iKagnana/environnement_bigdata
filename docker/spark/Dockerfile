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

# Télécharger les JARs nécessaires pour S3A (compatibles avec Spark 3.4.1 = Hadoop 3.3.4)
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Copier les JARs Delta Lake depuis le dossier local jars/
COPY docker/spark/jars/*.jar /opt/spark/jars/

# Copie datasets
COPY datas/ /opt/app/datas/

# Copier les jobs depuis le contexte (fallback si vous ne montez pas le repo)
COPY spark/jobs /opt/app/spark/jobs

# Copier le script d'exécution présent dans le contexte docker/spark/
COPY spark/jobs/run_jobs.sh /opt/spark/scripts/run_jobs.sh

# Convertir les fins de ligne et donner les permissions
RUN sed -i 's/\r$//' /opt/spark/scripts/run_jobs.sh && \
    chmod +x /opt/spark/scripts/run_jobs.sh

# Variables par défaut (peuvent être surchargées par docker-compose)
ENV SPARK_MASTER_URL=spark://spark-master:7077 \
    MINIO_ENDPOINT=http://minio:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin123 \
    SPARK_HOME=/opt/spark

# ENTRYPOINT uniquement pour le job runner
ENTRYPOINT ["/opt/spark/scripts/run_jobs.sh"]