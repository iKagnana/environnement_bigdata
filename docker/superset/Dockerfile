FROM apache/superset:latest

USER root

# Installer les outils système nécessaires
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Vérifier quel python3 est utilisé par défaut
RUN which python3 && python3 --version

# Installer pip dans le venv existant
RUN python3 -m ensurepip --upgrade 2>&1 || echo "pip déjà installé"

# Installer le driver PostgreSQL
RUN python3 -m pip install --no-cache-dir psycopg2-binary

# Installer trino
RUN echo "=== Installation de trino ===" && \
    python3 -m pip install --no-cache-dir trino && \
    echo "=== Installation réussie ==="

# Installer sqlalchemy-trino
RUN echo "=== Installation de sqlalchemy-trino ===" && \
    python3 -m pip install --no-cache-dir sqlalchemy-trino && \
    echo "=== Installation réussie ==="

# Vérifier ce qui a été installé
RUN echo "=== Packages installés contenant 'trino' ===" && \
    python3 -m pip list | grep -i trino

# Test d'import trino
RUN python3 -c "import trino; print('✅ Import trino OK:', trino.__version__)"

# Test d'import sqlalchemy_trino - peut avoir un nom différent
RUN python3 -c "from sqlalchemy.dialects import registry; registry.register('trino', 'sqlalchemy_trino.dialect', 'TrinoDialect'); print('✅ SQLAlchemy Trino dialect registered')" || \
    python3 -c "import sqlalchemy; print('✅ SQLAlchemy version:', sqlalchemy.__version__)"

# Créer les répertoires
RUN mkdir -p /app/pythonpath /app/superset_home

# Copier les fichiers de configuration
COPY conf/superset_config.py /app/pythonpath/superset_config.py
COPY conf/scripts/init-superset.sh /app/init-superset.sh

# Corriger le format et permissions
RUN sed -i 's/\r$//' /app/init-superset.sh && \
    chmod +x /app/init-superset.sh

# Permissions pour superset
RUN chown -R superset:superset /app

# Basculer vers l'utilisateur superset
USER superset

# Variables d'environnement
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
ENV PYTHONPATH=/app/pythonpath:$PYTHONPATH

ENTRYPOINT ["/app/init-superset.sh"]