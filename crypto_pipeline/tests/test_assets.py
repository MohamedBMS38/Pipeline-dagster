"""
Tests unitaires pour les assets.
"""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime
from dagster import build_op_context
from crypto_pipeline.assets.crypto_assets import (
    crypto_coins_list,
    store_crypto_list,
    crypto_market_data,
    store_market_data,
    crypto_price_history,
    store_price_history,
    crypto_price_trends,
    crypto_price_visualization
)

# Données de test
TEST_COIN_LIST = [
    {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
    {"id": "ethereum", "symbol": "eth", "name": "Ethereum"}
]

TEST_MARKET_DATA = [
    {
        "id": "bitcoin",
        "current_price": 50000,
        "market_cap": 1000000000000,
        "total_volume": 50000000000,
        "high_24h": 51000,
        "low_24h": 49000,
        "price_change_percentage_24h": 2.5
    },
    {
        "id": "ethereum",
        "current_price": 3000,
        "market_cap": 400000000000,
        "total_volume": 20000000000,
        "high_24h": 3100,
        "low_24h": 2900,
        "price_change_percentage_24h": 1.8
    }
]

TEST_PRICE_HISTORY = {
    "bitcoin": {
        "prices": [[1617753600000, 50000], [1617840000000, 52000]],
        "market_caps": [[1617753600000, 1000000000000], [1617840000000, 1050000000000]],
        "total_volumes": [[1617753600000, 50000000000], [1617840000000, 52000000000]]
    },
    "ethereum": {
        "prices": [[1617753600000, 3000], [1617840000000, 3100]],
        "market_caps": [[1617753600000, 400000000000], [1617840000000, 410000000000]],
        "total_volumes": [[1617753600000, 20000000000], [1617840000000, 21000000000]]
    }
}

TEST_PRICE_TRENDS = pd.DataFrame([
    {"id": "bitcoin", "name": "Bitcoin", "price": 50000, "market_cap": 1000000000000, "price_change_percentage_24h": 2.5},
    {"id": "ethereum", "name": "Ethereum", "price": 3000, "market_cap": 400000000000, "price_change_percentage_24h": 1.8}
])

# Tests pour les assets
def test_crypto_coins_list():
    """Teste l'asset crypto_coins_list"""
    # Création du mock pour la ressource CoinGecko
    coingecko_resource = MagicMock()
    coingecko_resource.get_coin_list.return_value = TEST_COIN_LIST
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    result = crypto_coins_list(context, coingecko_resource)
    
    # Vérifications
    coingecko_resource.get_coin_list.assert_called_once()
    assert result == TEST_COIN_LIST
    assert len(result) == 2
    assert result[0]["id"] == "bitcoin"

def test_store_crypto_list():
    """Teste l'asset store_crypto_list"""
    # Création des mocks
    duckdb_resource = MagicMock()
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    store_crypto_list(context, duckdb_resource, TEST_COIN_LIST)
    
    # Vérifications
    duckdb_resource.create_tables.assert_called_once()
    duckdb_resource.store_coin_list.assert_called_once_with(TEST_COIN_LIST)

def test_crypto_market_data():
    """Teste l'asset crypto_market_data"""
    # Création du mock pour la ressource CoinGecko
    coingecko_resource = MagicMock()
    coingecko_resource.get_coin_market_data.return_value = TEST_MARKET_DATA
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    result = crypto_market_data(context, coingecko_resource)
    
    # Vérifications
    coingecko_resource.get_coin_market_data.assert_called_once()
    assert result == TEST_MARKET_DATA
    assert len(result) == 2
    assert result[0]["id"] == "bitcoin"
    assert result[1]["id"] == "ethereum"

def test_store_market_data():
    """Teste l'asset store_market_data"""
    # Création des mocks
    duckdb_resource = MagicMock()
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    store_market_data(context, duckdb_resource, TEST_MARKET_DATA)
    
    # Vérifications
    duckdb_resource.store_market_data.assert_called_once_with(TEST_MARKET_DATA)

def test_crypto_price_history():
    """Teste l'asset crypto_price_history"""
    # Création du mock pour la ressource CoinGecko
    coingecko_resource = MagicMock()
    coingecko_resource.get_coin_price_history.side_effect = lambda coin_id: TEST_PRICE_HISTORY[coin_id]
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Patch pour remplacer la liste des cryptomonnaies suivies
    with patch('crypto_pipeline.assets.crypto_assets.TOP_CRYPTO_COINS', ["bitcoin", "ethereum"]):
        # Test de l'asset
        result = crypto_price_history(context, coingecko_resource)
    
    # Vérifications
    assert coingecko_resource.get_coin_price_history.call_count == 2
    assert "bitcoin" in result
    assert "ethereum" in result
    assert result["bitcoin"] == TEST_PRICE_HISTORY["bitcoin"]
    assert result["ethereum"] == TEST_PRICE_HISTORY["ethereum"]

def test_store_price_history():
    """Teste l'asset store_price_history"""
    # Création des mocks
    duckdb_resource = MagicMock()
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    store_price_history(context, duckdb_resource, TEST_PRICE_HISTORY)
    
    # Vérifications
    assert duckdb_resource.store_price_history.call_count == 2
    duckdb_resource.store_price_history.assert_any_call("bitcoin", TEST_PRICE_HISTORY["bitcoin"])
    duckdb_resource.store_price_history.assert_any_call("ethereum", TEST_PRICE_HISTORY["ethereum"])

def test_crypto_price_trends():
    """Teste l'asset crypto_price_trends"""
    # Création des mocks
    duckdb_resource = MagicMock()
    duckdb_resource.get_top_coins.return_value = TEST_PRICE_TRENDS
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    result = crypto_price_trends(context, duckdb_resource)
    
    # Vérifications
    duckdb_resource.get_top_coins.assert_called_once_with(10)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "bitcoin" in result["id"].values
    assert "ethereum" in result["id"].values

@patch('crypto_pipeline.assets.crypto_assets.plt')
def test_crypto_price_visualization(mock_plt):
    """Teste l'asset crypto_price_visualization"""
    # Création des mocks
    duckdb_resource = MagicMock()
    duckdb_resource.get_price_history_for_coin.return_value = pd.DataFrame({
        "timestamp": [datetime(2022, 1, 1), datetime(2022, 1, 2)],
        "price": [50000, 52000],
        "market_cap": [1000000000000, 1050000000000],
        "total_volume": [50000000000, 52000000000]
    })
    
    # Création du contexte d'exécution
    context = build_op_context()
    
    # Test de l'asset
    result = crypto_price_visualization(context, duckdb_resource, TEST_PRICE_TRENDS)
    
    # Vérifications
    assert duckdb_resource.get_price_history_for_coin.call_count > 0
    assert mock_plt.figure.call_count > 0
    assert mock_plt.savefig.call_count > 0
    assert result == "Visualisations créées avec succès" 