FROM apache/spark:3.4.0

USER root

# Copier les JARs Hive nécessaires dans le conteneur
COPY ../../hive/jars/*.jar /opt/spark/jars/

# Copier le fichier hive-site.xml pour la configuration du metastore
COPY ../../conf/hive-site.xml /opt/spark/conf/hive-site.xml

# Exposer le port pour Spark Thrift Server
EXPOSE 10000

# Commande pour démarrer Spark Thrift Server
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"]