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

RUN mkdir -p /opt/app /opt/jars /opt/app/spark/jobs
WORKDIR /opt/app

COPY docker/spark/jars/*.jar /opt/spark/jars/
COPY datas/ /opt/app/datas/
COPY spark/jobs /opt/app/spark/jobs
COPY spark/jobs/run_jobs.sh /opt/app/spark/jobs/run_jobs.sh
RUN chmod +x /opt/app/spark/jobs/run_jobs.sh

ENV SPARK_MASTER_URL=spark://spark-master:7077 \
    MINIO_ENDPOINT=http://minio:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin123

ENTRYPOINT ["/opt/app/spark/jobs/run_jobs.sh"]
