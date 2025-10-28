FROM openjdk:8-jre-slim

ENV HADOOP_VERSION=3.3.4
ENV HIVE_VERSION=3.1.3

# Installation des outils nécessaires
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Installation de Hadoop
RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xzC /opt \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# Installation de Hive
RUN curl -fsSL https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    | tar -xzC /opt \
    && ln -s /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive

# Installation du driver PostgreSQL
RUN curl -fsSL -o /opt/hive/lib/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Variables d'environnement
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin

# Script d'initialisation
COPY conf/scripts/init-hive-metastore.sh /init-hive-metastore.sh
RUN chmod +x /init-hive-metastore.sh

EXPOSE 9083

CMD ["/init-hive-metastore.sh"]