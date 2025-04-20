"""
Jobs Dagster pour la pipeline de cryptomonnaies.
"""

from dagster import (
    define_asset_job,
    AssetSelection,
    job,
    graph,
    config_mapping,
    GraphDefinition
)
from datetime import datetime, timedelta
from typing import Any, Dict, List

# Importer les définitions de partitions depuis le module assets
from crypto_pipeline.assets.crypto_assets import DAILY_PARTITIONS, MONTHLY_PARTITIONS

# Job pour le chargement des métadonnées des cryptomonnaies
crypto_metadata_job = define_asset_job(
    name="crypto_metadata_job",
    description="[ÉTAPE 1] Job pour extraire et stocker les métadonnées des cryptomonnaies",
    selection=(AssetSelection.groups("extract") & AssetSelection.assets("crypto_coins_list")) |
              (AssetSelection.groups("load") & AssetSelection.assets("store_crypto_list")),
)

# Job pour les données de marché quotidiennes
crypto_market_data_job = define_asset_job(
    name="crypto_market_data_job",
    description="[ÉTAPE 2] Job pour extraire et stocker les données de marché quotidiennes",
    selection=(AssetSelection.groups("extract") & AssetSelection.assets("crypto_market_data")) |
              (AssetSelection.groups("load") & AssetSelection.assets("store_market_data")),
    partitions_def=DAILY_PARTITIONS,
)

# Job pour l'historique des prix
crypto_price_history_job = define_asset_job(
    name="crypto_price_history_job",
    description="[ÉTAPE 3] Job pour extraire et stocker l'historique des prix",
    selection=(AssetSelection.groups("extract") & AssetSelection.assets("crypto_price_history")) |
              (AssetSelection.groups("load") & AssetSelection.assets("store_price_history")),
    partitions_def=DAILY_PARTITIONS,
)

# Job pour l'analyse et la visualisation
crypto_analytics_job = define_asset_job(
    name="crypto_analytics_job",
    description="[ÉTAPE 4] Job pour analyser et visualiser les données de cryptomonnaies",
    selection=(AssetSelection.groups("transform") & AssetSelection.assets("crypto_price_trends")) |
              (AssetSelection.groups("visualize") & AssetSelection.assets("crypto_price_visualization")),
)

# Job pour le rapport mensuel
crypto_monthly_report_job = define_asset_job(
    name="crypto_monthly_report_job",
    description="[ÉTAPE 5] Job pour générer les rapports mensuels",
    selection=AssetSelection.groups("reporting"),
    partitions_def=MONTHLY_PARTITIONS,
) 