# Utiliser une image Python officielle
FROM python:3.10-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers de dépendances
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source
COPY . .

# Créer les répertoires nécessaires
RUN mkdir -p /app/crypto_pipeline/data/visualizations && \
    mkdir -p /app/.dagster

# Exposer le port pour l'interface web de Dagster
EXPOSE 3000

# Commande pour lancer Dagster
CMD ["python", "-m", "dagster", "dev", "-m", "crypto_pipeline", "-p", "3000", "-h", "0.0.0.0"] 