FROM apache/spark:3.4.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc \
  && chmod +x /usr/local/bin/mc \
  && pip3 install --no-cache-dir minio

# Télécharger les JARs AWS et Delta Lake
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
  && curl -fsSL -o /opt/spark/jars/hadoop-common-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
  && curl -fsSL -o /opt/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
  && curl -fsSL -o /opt/spark/jars/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

RUN mkdir -p /opt/app /opt/jars /opt/app/spark/jobs
WORKDIR /opt/app

COPY datas/ /opt/app/datas/
COPY spark/jobs /opt/app/spark/jobs
COPY spark/jobs/run_jobs.sh /opt/app/spark/jobs/run_jobs.sh
RUN chmod +x /opt/app/spark/jobs/run_jobs.sh

ENV SPARK_MASTER_URL=spark://spark-master:7077 \
    MINIO_ENDPOINT=http://minio:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin123

ENTRYPOINT ["/opt/app/spark/jobs/run_jobs.sh"]
