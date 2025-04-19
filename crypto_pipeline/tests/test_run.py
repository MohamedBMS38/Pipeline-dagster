"""
Script de test pour tester les composants individuellement.
"""

import sys
import os
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Ajouter le chemin parent au sys.path pour pouvoir importer les modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from crypto_pipeline.resources.coingecko_resource import CoinGeckoResource
from crypto_pipeline.resources.duckdb_resource import DuckDBResource
from crypto_pipeline.utils.visualization import create_price_chart, create_market_overview

def test_coingecko_resource():
    """Teste la ressource CoinGecko pour s'assurer qu'elle peut se connecter à l'API."""
    print("\n=== Test de la ressource CoinGecko ===")
    
    try:
        resource = CoinGeckoResource()
        print("✅ Création de la ressource CoinGecko réussie")
        
        # Tester la récupération de la liste des cryptomonnaies
        coin_list = resource.get_coin_list()
        print(f"✅ Récupération de {len(coin_list)} cryptomonnaies réussie")
        print(f"Exemple de cryptomonnaie: {coin_list[0]}")
        
        # Tester la récupération des données de marché
        market_data = resource.get_coin_market_data(["bitcoin", "ethereum"])
        print(f"✅ Récupération des données de marché réussie pour {len(market_data)} cryptomonnaies")
        
        # Tester la récupération de l'historique des prix
        price_history = resource.get_coin_price_history("bitcoin")
        print(f"✅ Récupération de l'historique des prix réussie pour bitcoin ({len(price_history['prices'])} points)")
        
        return True, coin_list, market_data, price_history
    except Exception as e:
        print(f"❌ Erreur lors du test de la ressource CoinGecko: {e}")
        return False, None, None, None

def test_duckdb_resource(coin_list=None, market_data=None, price_history=None):
    """Teste la ressource DuckDB pour s'assurer qu'elle peut stocker les données."""
    print("\n=== Test de la ressource DuckDB ===")
    
    # Chemin temporaire pour les tests
    db_path = "crypto_pipeline/data/test.duckdb"
    
    # Suppression du fichier de test s'il existe déjà
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"✅ Suppression de l'ancien fichier de test {db_path} réussie")
        except Exception as e:
            print(f"❌ Erreur lors de la suppression de l'ancien fichier de test: {e}")
    
    try:
        resource = DuckDBResource(database_path=db_path)
        print("✅ Création de la ressource DuckDB réussie")
        
        # Tester la création des tables
        resource.create_tables()
        print("✅ Création des tables réussie")
        
        # Si des données sont fournies, les stocker
        if coin_list:
            resource.store_coin_list(coin_list[:10])  # Limiter à 10 cryptomonnaies pour le test
            print("✅ Stockage de la liste des cryptomonnaies réussie")
        
        if market_data:
            resource.store_market_data(market_data)
            print("✅ Stockage des données de marché réussie")
        
        if price_history:
            resource.store_price_history("bitcoin", price_history)
            print("✅ Stockage de l'historique des prix réussie")
        
        # Tester la récupération des données
        if coin_list and market_data:
            top_coins = resource.get_top_coins(5)
            print(f"✅ Récupération des top coins réussie: {len(top_coins)} cryptomonnaies")
            print(top_coins)
        
        return True
    except Exception as e:
        print(f"❌ Erreur lors du test de la ressource DuckDB: {e}")
        return False

def test_visualization(market_data=None, price_history=None):
    """Teste les fonctions de visualisation."""
    print("\n=== Test des fonctions de visualisation ===")
    
    try:
        # Créer des données de test si aucune n'est fournie
        if not market_data:
            market_data = [
                {
                    "id": "bitcoin",
                    "name": "Bitcoin",
                    "current_price": 50000,
                    "market_cap": 1000000000000,
                    "total_volume": 50000000000,
                    "price_change_percentage_24h": 2.5
                },
                {
                    "id": "ethereum",
                    "name": "Ethereum",
                    "current_price": 3000,
                    "market_cap": 400000000000,
                    "total_volume": 20000000000,
                    "price_change_percentage_24h": -1.8
                }
            ]
        
        # Préparation des données pour les graphiques
        df_market = pd.DataFrame(market_data)
        df_market = df_market.rename(columns={
            'current_price': 'price',
            'market_cap': 'market_cap',
            'total_volume': 'total_volume',
            'price_change_percentage_24h': 'price_change_percentage_24h'
        })
        
        # Créer un aperçu du marché
        vis_path = "crypto_pipeline/data/visualizations/test_market_overview.png"
        create_market_overview(df_market, output_path=vis_path)
        print(f"✅ Création de l'aperçu du marché réussie: {vis_path}")
        
        # Créer un graphique d'évolution des prix si des données d'historique sont disponibles
        if price_history and 'prices' in price_history:
            prices = price_history.get('prices', [])
            df_prices = pd.DataFrame(prices, columns=['timestamp', 'price'])
            df_prices['timestamp'] = pd.to_datetime(df_prices['timestamp'], unit='ms')
            
            vis_path = "crypto_pipeline/data/visualizations/test_price_chart.png"
            create_price_chart(df_prices, "Bitcoin", output_path=vis_path, show_volume=False)
            print(f"✅ Création du graphique d'évolution des prix réussie: {vis_path}")
        
        return True
    except Exception as e:
        print(f"❌ Erreur lors du test des fonctions de visualisation: {e}")
        return False

if __name__ == "__main__":
    print("=== Début des tests ===")
    
    # Tester la ressource CoinGecko
    success_coingecko, coin_list, market_data, price_history = test_coingecko_resource()
    
    # Tester la ressource DuckDB avec les données récupérées
    if success_coingecko:
        success_duckdb = test_duckdb_resource(coin_list, market_data, price_history)
    else:
        # Tester quand même DuckDB mais sans données
        success_duckdb = test_duckdb_resource()
    
    # Tester les visualisations
    success_viz = test_visualization(market_data, price_history)
    
    print("\n=== Résumé des tests ===")
    print(f"CoinGecko: {'✅ Success' if success_coingecko else '❌ Failed'}")
    print(f"DuckDB: {'✅ Success' if success_duckdb else '❌ Failed'}")
    print(f"Visualisations: {'✅ Success' if success_viz else '❌ Failed'}") 