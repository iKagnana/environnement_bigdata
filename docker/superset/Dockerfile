FROM apache/superset:latest

USER root

# Installer les outils de compilation et les dépendances système
RUN apt-get update && apt-get install -y \
    build-essential \
    libsasl2-dev \
    libsasl2-modules \
    libsasl2-2 \
    gcc \
    g++ \
    python3-dev \
    libffi-dev \
    libssl-dev \
    pkg-config \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copier le script d'initialisation
COPY conf/scripts/init-superset.sh /app/init-superset.sh

# Corriger le format et les permissions
RUN dos2unix /app/init-superset.sh && chmod +x /app/init-superset.sh

# Mise à jour de pip
RUN pip install --upgrade pip

# Installer les drivers de base de données
RUN pip install --no-cache-dir psycopg2-binary==2.9.7

# Installer les composants Thrift/Hive
RUN pip install --no-cache-dir thrift==0.16.0
RUN pip install --no-cache-dir thrift-sasl==0.4.3
RUN pip install --no-cache-dir sasl==0.3.1 || pip install --no-cache-dir pure-sasl==0.6.2

# Installer PyHive avec toutes ses dépendances
RUN pip install --no-cache-dir pyhive[hive]==0.7.0 || \
    (pip install --no-cache-dir future python-dateutil && \
     pip install --no-cache-dir pyhive --no-deps)

# Installer Impyla comme alternative
RUN pip install --no-cache-dir impyla==0.18.0

# Créer le répertoire de configuration
RUN mkdir -p /app/pythonpath

# Remettre les permissions correctes
RUN chown -R superset:superset /app

# Revenir à l'utilisateur superset
USER superset

# Point d'entrée par défaut
ENTRYPOINT ["/app/init-superset.sh"]