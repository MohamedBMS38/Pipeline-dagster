FROM python:3.10-slim

WORKDIR /app

# Copier les fichiers du projet
COPY requirements.txt .
COPY .env.example .env
COPY README.md .
COPY crypto_pipeline/ ./crypto_pipeline/

# Créer les répertoires nécessaires
RUN mkdir -p ./crypto_pipeline/data

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Exposer le port pour le serveur Dagster
EXPOSE 3000

# Commande par défaut
CMD ["dagster", "dev", "-m", "crypto_pipeline"] 