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
import time
import os

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
    group_name="extract",
    description="Extrait l'historique des prix pour les 4 principales cryptomonnaies",
    deps=["store_market_data"],
    partitions_def=DAILY_PARTITIONS,
    required_resource_keys={"coingecko_resource"},
)
def crypto_price_history(context) -> List[Dict[str, Any]]:
    """Extrait l'historique des prix pour les 4 principales cryptomonnaies."""
    # Liste des 4 principales cryptomonnaies à suivre
    COINS_TO_TRACK = ["bitcoin", "ethereum", "binancecoin", "cardano"]
    
    try:
        # Vérifier si le job est exécuté avec des partitions
        try:
            partition_date = context.partition_key
            context.log.info(f"Extraction de l'historique des prix pour la partition {partition_date}")
        except:
            # Si le run n'est pas partitionné, utiliser la date du jour
            partition_date = datetime.now().strftime("%Y-%m-%d")
            context.log.info(f"Run non partitionné. Extraction de l'historique des prix pour la date {partition_date}")
        
        context.log.info(f"Extraction de l'historique des prix pour {len(COINS_TO_TRACK)} cryptomonnaies")
        price_history = []
        
        for coin_id in COINS_TO_TRACK:
            try:
                # Ajouter un délai de 2 secondes entre chaque requête
                time.sleep(2)
                history = context.resources.coingecko_resource.get_coin_price_history(coin_id)
                price_history.append({
                    "coin_id": coin_id,
                    "history": history
                })
                context.log.info(f"Historique extrait pour {coin_id}")
            except Exception as e:
                if "429" in str(e):
                    context.log.warning(f"Limite de taux atteinte pour {coin_id}. Attente de 60 secondes...")
                    time.sleep(60)  # Attendre 60 secondes en cas de limite atteinte
                    try:
                        history = context.resources.coingecko_resource.get_coin_price_history(coin_id)
                        price_history.append({
                            "coin_id": coin_id,
                            "history": history
                        })
                        context.log.info(f"Historique extrait pour {coin_id} après attente")
                    except Exception as retry_e:
                        context.log.error(f"Échec de la récupération après attente pour {coin_id}: {str(retry_e)}")
                else:
                    context.log.error(f"Erreur lors de la récupération de l'historique des prix pour {coin_id}: {str(e)}")
                continue
        
        context.log.info(f"Extraction de l'historique terminée. {len(price_history)} cryptomonnaies traitées")
        return price_history
        
    except Exception as e:
        context.log.error(f"Erreur lors de l'extraction de l'historique des prix: {str(e)}")
        raise

@asset(
    description="Stocke l'historique des prix dans DuckDB",
    group_name="load",
    deps=["crypto_price_history"],
    required_resource_keys={"duckdb_resource"},
    partitions_def=DAILY_PARTITIONS,
)
def store_price_history(context: AssetExecutionContext, crypto_price_history) -> None:
    """
    Stocke l'historique des prix dans la base de données DuckDB.
    """
    try:
        # Récupérer la date de partition
        partition_date = context.partition_key
        context.log.info(f"Stockage de l'historique des prix pour la partition {partition_date}")
        
        # Créer les tables si elles n'existent pas
        context.resources.duckdb_resource.create_tables()
        
        # Stocker les données
        for coin_data in crypto_price_history:
            coin_id = coin_data["coin_id"]
            history = coin_data["history"]
            context.resources.duckdb_resource.store_price_history(coin_id, history)
            
        context.log.info(f"Stockage terminé pour la partition {partition_date}")
        
    except Exception as e:
        context.log.error(f"Erreur lors du stockage de l'historique des prix: {str(e)}")
        raise

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
    try:
        month = context.partition_key
        context.log.info(f"Génération du rapport mensuel pour la partition {month}")
    except:
        month = datetime.now().strftime("%Y-%m")
        context.log.info(f"Run non partitionné. Génération du rapport mensuel pour {month}")
    
    context.resources.duckdb_resource.create_tables()
    
    try:
        # Récupérer les données pour les 10 principales cryptomonnaies
        top_coins = context.resources.duckdb_resource.get_top_coins(10)
        
        # Créer une figure avec plusieurs sous-graphiques
        fig = plt.figure(figsize=(20, 15))
        
        # 1. Graphique des variations de prix sur 24h
        ax1 = plt.subplot(2, 2, 1)
        ax1.bar(top_coins['name'], top_coins['price_change_percentage_24h'])
        ax1.set_title(f"Variation de prix sur 24h - {month}")
        ax1.set_xlabel("Cryptomonnaie")
        ax1.set_ylabel("Variation en %")
        plt.xticks(rotation=45)
        ax1.grid(True, axis='y')
        
        # 2. Graphique de la capitalisation boursière
        ax2 = plt.subplot(2, 2, 2)
        ax2.pie(top_coins['market_cap'], labels=top_coins['name'], autopct='%1.1f%%')
        ax2.set_title(f"Répartition de la capitalisation boursière - {month}")
        
        # 3. Graphique des prix actuels
        ax3 = plt.subplot(2, 2, 3)
        ax3.bar(top_coins['name'], top_coins['price'])
        ax3.set_title(f"Prix actuels des cryptomonnaies - {month}")
        ax3.set_xlabel("Cryptomonnaie")
        ax3.set_ylabel("Prix en USD")
        plt.xticks(rotation=45)
        ax3.grid(True, axis='y')
        
        # 4. Graphique de l'évolution des prix sur le mois
        ax4 = plt.subplot(2, 2, 4)
        yearMonth = datetime.strptime(month, "%Y-%m-%d").strftime("%Y-%m")
        for coin_id in top_coins['id']:
            history = context.resources.duckdb_resource.get_coin_price_history(coin_id)
            if not history.empty:
                # Convertir les timestamps et filtrer par mois
                history['timestamp'] = pd.to_datetime(history['timestamp'])
                filtered_history = history[history['timestamp'].dt.strftime('%Y-%m') == yearMonth]

                # Vérifier si les données filtrées ne sont pas vides
                if not filtered_history.empty:
                    ax4.plot(filtered_history['timestamp'], filtered_history['price'], label=coin_id)
                else:
                    context.log.warning(f"Aucune donnée disponible pour {coin_id} sur le mois {yearMonth}")
            else:
                context.log.warning(f"Aucune donnée d'historique pour {coin_id}")
        ax4.set_title(f"Évolution des prix sur le mois - {yearMonth}")
        ax4.set_xlabel("Date")
        ax4.set_ylabel("Prix en USD")
        ax4.legend()
        ax4.grid(True)
        
        # Ajuster l'espacement entre les graphiques
        plt.tight_layout()
        

        # Sauvegarder le graphique
        file_path = f"crypto_pipeline/data/monthly_report_{month}.png"
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        context.log.info(f"Rapport mensuel généré pour {month} et sauvegardé dans {file_path}")
        
        return top_coins, file_path
    except Exception as e:
        context.log.error(f"Erreur lors de la génération du rapport mensuel: {e}")
        empty_df = pd.DataFrame(columns=['id', 'name', 'price', 'market_cap', 'price_change_percentage_24h'])
        file_path = f"crypto_pipeline/data/monthly_report_error_{month}.txt"
        with open(file_path, 'w') as f:
            f.write(f"Erreur lors de la génération du rapport: {e}")
        return empty_df, file_path 