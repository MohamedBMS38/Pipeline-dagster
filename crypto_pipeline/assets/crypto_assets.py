"""
Assets Dagster pour les données de cryptomonnaies.
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from dagster import (
    asset, 
    AssetExecutionContext, 
    Output, 
    DailyPartitionsDefinition, 
    Definitions, 
    AssetsDefinition,
    MonthlyPartitionsDefinition,
    multi_asset,
    AssetIn,
    AssetOut
)
from typing import List, Dict, Any, Tuple

# Utiliser une date fixe au lieu de datetime.now() pour éviter des instances différentes
# IMPORTANT: Utiliser datetime.datetime et non datetime.date
START_DATE = datetime(2023, 1, 1)
# Définition des partitions quotidiennes depuis le 1er janvier 2023
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date=START_DATE)

# Définition des partitions mensuelles pour les rapports
MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date=datetime(2023, 1, 1))

# Liste des principales cryptomonnaies à suivre
TOP_CRYPTO_COINS = ["bitcoin", "ethereum", "solana", "binancecoin", "cardano", "polkadot", "dogecoin", "ripple", "avalanche-2", "tron"]

@asset(
    description="Extrait la liste complète des cryptomonnaies depuis l'API CoinGecko",
    group_name="extract",
    required_resource_keys={"coingecko_resource"},
    partitions_def=DAILY_PARTITIONS,
)
def crypto_coins_list(context: AssetExecutionContext) -> List[Dict[str, str]]:
    """
    Récupère la liste complète des cryptomonnaies depuis l'API CoinGecko.
    """
    # Vérifier si le job est exécuté avec des partitions
    try:
        partition_date = context.partition_key
        context.log.info(f"Extraction de la liste des cryptomonnaies pour la partition {partition_date}")
    except:
        # Si le run n'est pas partitionné, utiliser la date du jour
        partition_date = datetime.now().strftime("%Y-%m-%d")
        context.log.info(f"Run non partitionné. Extraction de la liste des cryptomonnaies pour la date {partition_date}")
        
    coins = context.resources.coingecko_resource.get_coin_list()
    context.log.info(f"Récupération de {len(coins)} cryptomonnaies")
    
    return coins

@asset(
    description="Stocke la liste des cryptomonnaies dans DuckDB",
    group_name="load",
    deps=["crypto_coins_list"],
    required_resource_keys={"duckdb_resource"},
    partitions_def=DAILY_PARTITIONS,
)
def store_crypto_list(context: AssetExecutionContext, crypto_coins_list) -> None:
    """
    Stocke la liste des cryptomonnaies dans la base de données DuckDB.
    """
    # Vérifier si le job est exécuté avec des partitions
    try:
        partition_date = context.partition_key
        context.log.info(f"Stockage de {len(crypto_coins_list)} cryptomonnaies dans la base de données pour la partition {partition_date}")
    except:
        # Si le run n'est pas partitionné, utiliser la date du jour
        partition_date = datetime.now().strftime("%Y-%m-%d")
        context.log.info(f"Run non partitionné. Stockage de {len(crypto_coins_list)} cryptomonnaies pour la date {partition_date}")
        
    # Créer les tables si elles n'existent pas
    context.resources.duckdb_resource.create_tables()
    
    # Stocker les données
    context.resources.duckdb_resource.store_coin_list(crypto_coins_list)

@asset(
    description="Extrait les données de marché pour les principales cryptomonnaies",
    group_name="extract",
    partitions_def=DAILY_PARTITIONS,
    required_resource_keys={"coingecko_resource"},
    deps=["store_crypto_list"],
)
def crypto_market_data(context: AssetExecutionContext) -> List[Dict[str, Any]]:
    """
    Récupère les données de marché pour les principales cryptomonnaies.
    """
    # Vérifier si le job est exécuté avec des partitions
    try:
        partition_date = context.partition_key
        context.log.info(f"Extraction des données de marché pour la partition {partition_date}")
    except:
        # Si le run n'est pas partitionné, utiliser la date du jour
        partition_date = datetime.now().strftime("%Y-%m-%d")
        context.log.info(f"Run non partitionné. Extraction des données de marché pour la date {partition_date}")
        
    market_data = context.resources.coingecko_resource.get_coin_market_data(TOP_CRYPTO_COINS)
    context.log.info(f"Récupération de {len(market_data)} entrées de données de marché")
    
    return market_data

@asset(
    description="Stocke les données de marché dans DuckDB",
    group_name="load",
    deps=["crypto_market_data"],
    required_resource_keys={"duckdb_resource"},
    partitions_def=DAILY_PARTITIONS,
)
def store_market_data(context: AssetExecutionContext, crypto_market_data) -> None:
    """
    Stocke les données de marché dans la base de données DuckDB.
    """
    # Vérifier si le job est exécuté avec des partitions
    try:
        partition_date = context.partition_key
        context.log.info(f"Stockage des données de marché pour la partition {partition_date}")
    except:
        # Si le run n'est pas partitionné, utiliser la date du jour
        partition_date = datetime.now().strftime("%Y-%m-%d")
        context.log.info(f"Run non partitionné. Stockage des données de marché pour la date {partition_date}")
        
    # Créer les tables si elles n'existent pas
    context.resources.duckdb_resource.create_tables()
    
    # Stocker les données
    context.resources.duckdb_resource.store_market_data(crypto_market_data)

@asset(
    description="Extrait l'historique des prix pour les principales cryptomonnaies",
    group_name="extract",
    deps=["store_market_data"],
    required_resource_keys={"coingecko_resource"},
)
def crypto_price_history(context: AssetExecutionContext) -> Dict[str, Dict[str, List[List[float]]]]:
    """
    Récupère l'historique des prix pour les principales cryptomonnaies.
    """
    context.log.info("Extraction de l'historique des prix")
    
    price_history = {}
    for coin_id in TOP_CRYPTO_COINS:
        history = context.resources.coingecko_resource.get_coin_price_history(coin_id)
        price_history[coin_id] = history
        context.log.info(f"Récupération de l'historique des prix pour {coin_id}")
    
    return price_history

@asset(
    description="Stocke l'historique des prix dans DuckDB",
    group_name="load",
    deps=["crypto_price_history"],
    required_resource_keys={"duckdb_resource"},
)
def store_price_history(context: AssetExecutionContext, crypto_price_history) -> None:
    """
    Stocke l'historique des prix dans la base de données DuckDB.
    """
    context.log.info(f"Stockage de l'historique des prix pour {len(crypto_price_history)} cryptomonnaies")
    
    # Créer les tables si elles n'existent pas
    context.resources.duckdb_resource.create_tables()
    
    # Stocker les données
    for coin_id, price_data in crypto_price_history.items():
        context.resources.duckdb_resource.store_price_history(coin_id, price_data)

@asset(
    description="Analyse des tendances de prix des cryptomonnaies",
    group_name="transform",
    deps=["store_market_data", "store_price_history"],
    required_resource_keys={"duckdb_resource"},
)
def crypto_price_trends(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Analyse les tendances de prix des principales cryptomonnaies.
    """
    context.log.info("Analyse des tendances de prix des cryptomonnaies")
    
    # Créer les tables si elles n'existent pas
    context.resources.duckdb_resource.create_tables()
    
    try:
        # Récupérer les données des 10 principales cryptomonnaies
        top_coins = context.resources.duckdb_resource.get_top_coins(10)
        context.log.info(f"Analyse des tendances de prix pour {len(top_coins)} cryptomonnaies")
        return top_coins
    except Exception as e:
        context.log.error(f"Erreur lors de l'analyse des tendances de prix: {e}")
        # Retourner un DataFrame vide en cas d'erreur
        return pd.DataFrame(columns=['id', 'name', 'price', 'market_cap', 'price_change_percentage_24h'])

@asset(
    description="Visualisation des tendances de prix",
    group_name="visualize",
    deps=["crypto_price_trends"],
    required_resource_keys={"duckdb_resource"},
)
def crypto_price_visualization(context: AssetExecutionContext, crypto_price_trends) -> str:
    """
    Génère une visualisation des tendances de prix des cryptomonnaies.
    """
    context.log.info("Génération de la visualisation des tendances de prix")
    
    # Créer un graphique
    plt.figure(figsize=(12, 8))
    plt.bar(crypto_price_trends['name'], crypto_price_trends['price_change_percentage_24h'])
    plt.title("Variation de prix sur 24h pour les principales cryptomonnaies")
    plt.xlabel("Cryptomonnaie")
    plt.ylabel("Variation en %")
    plt.xticks(rotation=45)
    plt.grid(True, axis='y')
    
    # Sauvegarder le graphique
    file_path = "crypto_pipeline/data/price_trends.png"
    plt.savefig(file_path)
    plt.close()
    
    context.log.info(f"Visualisation sauvegardée dans {file_path}")
    return file_path

# Définition multi-asset pour le rapport mensuel
@multi_asset(
    outs={
        "monthly_report_data": AssetOut(description="Données du rapport mensuel"),
        "monthly_report_visualization": AssetOut(description="Visualisation du rapport mensuel"),
    },
    deps={"market_data": AssetIn("store_market_data"), "price_history": AssetIn("store_price_history")},
    compute_kind="pandas",
    group_name="reporting",
    partitions_def=MONTHLY_PARTITIONS,
    required_resource_keys={"duckdb_resource"},
)
def monthly_crypto_report(context: AssetExecutionContext) -> Tuple[pd.DataFrame, str]:
    """
    Génère un rapport mensuel sur les performances des cryptomonnaies.
    """
    # Vérifier si le job est exécuté avec des partitions
    try:
        month = context.partition_key
        context.log.info(f"Génération du rapport mensuel pour la partition {month}")
    except:
        # Si le run n'est pas partitionné, utiliser le mois en cours
        month = datetime.now().strftime("%Y-%m")
        context.log.info(f"Run non partitionné. Génération du rapport mensuel pour {month}")
    
    # Créer les tables si elles n'existent pas
    context.resources.duckdb_resource.create_tables()
    
    try:
        # Récupérer les données pour les 10 principales cryptomonnaies
        top_coins = context.resources.duckdb_resource.get_top_coins(10)
        
        # Créer un graphique comparatif
        plt.figure(figsize=(12, 8))
        plt.bar(top_coins['name'], top_coins['price_change_percentage_24h'])
        plt.title(f"Variation de prix sur 24h pour les principales cryptomonnaies - {month}")
        plt.xlabel("Cryptomonnaie")
        plt.ylabel("Variation en %")
        plt.xticks(rotation=45)
        plt.grid(True, axis='y')
        
        # Sauvegarder le graphique
        file_path = f"crypto_pipeline/data/monthly_report_{month}.png"
        plt.savefig(file_path)
        plt.close()
        
        context.log.info(f"Rapport mensuel généré pour {month} et sauvegardé dans {file_path}")
        
        return Output(top_coins, "monthly_report_data"), Output(file_path, "monthly_report_visualization")
    except Exception as e:
        context.log.error(f"Erreur lors de la génération du rapport mensuel: {e}")
        empty_df = pd.DataFrame(columns=['id', 'name', 'price', 'market_cap', 'price_change_percentage_24h'])
        file_path = f"crypto_pipeline/data/monthly_report_error_{month}.txt"
        with open(file_path, 'w') as f:
            f.write(f"Erreur lors de la génération du rapport: {e}")
        return Output(empty_df, "monthly_report_data"), Output(file_path, "monthly_report_visualization") 