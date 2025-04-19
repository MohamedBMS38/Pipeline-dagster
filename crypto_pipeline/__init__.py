"""
Pipeline de données pour l'extraction, le stockage et l'analyse des données de cryptomonnaies
via l'API CoinGecko en utilisant Dagster comme orchestrateur.
"""

# Exposer les définitions directement au niveau du module principal
from crypto_pipeline.definitions import defs 