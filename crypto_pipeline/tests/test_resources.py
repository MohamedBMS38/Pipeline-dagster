"""
Tests unitaires pour les ressources.
"""

import pytest
import os
import pandas as pd
from unittest.mock import MagicMock, patch
from crypto_pipeline.resources.coingecko_resource import CoinGeckoResource
from crypto_pipeline.resources.duckdb_resource import DuckDBResource

# Tests pour CoinGeckoResource
def test_coingecko_resource_init():
    """Teste l'initialisation de la ressource CoinGecko"""
    resource = CoinGeckoResource()
    assert resource.base_url == "https://api.coingecko.com/api/v3"

@patch('crypto_pipeline.resources.coingecko_resource.requests.get')
def test_get_coin_list(mock_get):
    """Teste la méthode get_coin_list de CoinGeckoResource"""
    # Configuration du mock
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
        {"id": "ethereum", "symbol": "eth", "name": "Ethereum"}
    ]
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Test de la méthode
    resource = CoinGeckoResource()
    result = resource.get_coin_list()
    
    # Vérifications
    mock_get.assert_called_once_with("https://api.coingecko.com/api/v3/coins/list")
    assert len(result) == 2
    assert result[0]["id"] == "bitcoin"
    assert result[1]["name"] == "Ethereum"

@patch('crypto_pipeline.resources.coingecko_resource.requests.get')
def test_get_coin_market_data(mock_get):
    """Teste la méthode get_coin_market_data de CoinGeckoResource"""
    # Configuration du mock
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {
            "id": "bitcoin",
            "current_price": 50000,
            "market_cap": 1000000000000,
            "total_volume": 50000000000
        }
    ]
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Test de la méthode
    resource = CoinGeckoResource()
    result = resource.get_coin_market_data(["bitcoin"])
    
    # Vérifications
    mock_get.assert_called_once()
    assert len(result) == 1
    assert result[0]["id"] == "bitcoin"
    assert result[0]["current_price"] == 50000

@patch('crypto_pipeline.resources.coingecko_resource.requests.get')
def test_get_coin_price_history(mock_get):
    """Teste la méthode get_coin_price_history de CoinGeckoResource"""
    # Configuration du mock
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "prices": [[1617753600000, 50000], [1617840000000, 52000]],
        "market_caps": [[1617753600000, 1000000000000], [1617840000000, 1050000000000]],
        "total_volumes": [[1617753600000, 50000000000], [1617840000000, 52000000000]]
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Test de la méthode
    resource = CoinGeckoResource()
    result = resource.get_coin_price_history("bitcoin")
    
    # Vérifications
    mock_get.assert_called_once()
    assert len(result["prices"]) == 2
    assert result["prices"][0][1] == 50000
    assert result["prices"][1][1] == 52000

# Tests pour DuckDBResource
def test_duckdb_resource_init():
    """Teste l'initialisation de la ressource DuckDB"""
    resource = DuckDBResource(database_path="test.duckdb")
    assert resource.database_path == "test.duckdb"

@patch('crypto_pipeline.resources.duckdb_resource.duckdb.connect')
def test_create_tables(mock_connect):
    """Teste la méthode create_tables de DuckDBResource"""
    # Configuration du mock
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    
    # Test de la méthode
    resource = DuckDBResource(database_path="test.duckdb")
    resource.create_tables()
    
    # Vérifications
    mock_connect.assert_called_once_with("test.duckdb")
    assert mock_conn.execute.call_count == 3  # 3 tables créées

@patch('crypto_pipeline.resources.duckdb_resource.duckdb.connect')
def test_store_coin_list(mock_connect):
    """Teste la méthode store_coin_list de DuckDBResource"""
    # Configuration du mock
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    
    # Données de test
    test_data = [
        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
        {"id": "ethereum", "symbol": "eth", "name": "Ethereum"}
    ]
    
    # Test de la méthode
    resource = DuckDBResource(database_path="test.duckdb")
    resource.store_coin_list(test_data)
    
    # Vérifications
    mock_connect.assert_called_once_with("test.duckdb")
    mock_conn.execute.assert_called()  # La requête SQL est exécutée
    mock_conn.register.assert_called_once()  # Le DataFrame est enregistré

@patch('crypto_pipeline.resources.duckdb_resource.duckdb.connect')
def test_store_market_data(mock_connect):
    """Teste la méthode store_market_data de DuckDBResource"""
    # Configuration du mock
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    
    # Données de test
    test_data = [
        {
            "id": "bitcoin",
            "current_price": 50000,
            "market_cap": 1000000000000,
            "total_volume": 50000000000,
            "high_24h": 51000,
            "low_24h": 49000,
            "price_change_percentage_24h": 2.5
        }
    ]
    
    # Test de la méthode
    resource = DuckDBResource(database_path="test.duckdb")
    resource.store_market_data(test_data)
    
    # Vérifications
    mock_connect.assert_called_once_with("test.duckdb")
    mock_conn.execute.assert_called()  # La requête SQL est exécutée
    mock_conn.register.assert_called_once()  # Le DataFrame est enregistré

@patch('crypto_pipeline.resources.duckdb_resource.duckdb.connect')
def test_store_price_history(mock_connect):
    """Teste la méthode store_price_history de DuckDBResource"""
    # Configuration du mock
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    
    # Données de test
    test_data = {
        "prices": [[1617753600000, 50000], [1617840000000, 52000]],
        "market_caps": [[1617753600000, 1000000000000], [1617840000000, 1050000000000]],
        "total_volumes": [[1617753600000, 50000000000], [1617840000000, 52000000000]]
    }
    
    # Test de la méthode
    resource = DuckDBResource(database_path="test.duckdb")
    resource.store_price_history("bitcoin", test_data)
    
    # Vérifications
    mock_connect.assert_called_once_with("test.duckdb")
    mock_conn.execute.assert_called()  # La requête SQL est exécutée
    mock_conn.register.assert_called_once()  # Le DataFrame est enregistré 