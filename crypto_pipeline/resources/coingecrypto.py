"""
Client CoinGecko pour récupérer les données de cryptomonnaies.
"""

import requests
import time
from typing import List, Dict, Any

class CoinGeckoResource:
    def __init__(self):
        self.base_url = "https://api.coingeo.com/api/v3"
        self.rate_limit_delay = 2  # Attendre 2 secondes entre chaque requête
        self.max_retries = 3  # Nombre maximum de tentatives en cas d'erreur

    def _make_request(self, url: str, params: dict = None) -> dict:
        """
        Effectue une requête HTTP avec gestion des erreurs et des limites de taux.
        """
        for attempt in range(self.max_retries):
            try:
                # Attendre avant chaque requête pour respecter les limites
                time.sleep(self.rate_limit_delay)
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    if attempt < self.max_retries - 1:
                        # Augmenter le délai d'attente à chaque tentative
                        time.sleep((attempt + 1) * self.rate_limit_delay)
                        continue
                raise
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.rate_limit_delay)
                    continue
                raise

    def get_coin_list(self) -> List[Dict[str, Any]]:
        """
        Récupère la liste des cryptomonnaies disponibles.
        """
        try:
            endpoint = f"{self.base_url}/coins/list"
            return self._make_request(endpoint)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération de la liste des cryptomonnaies: {e}")
            raise

    def get_coin_market_data(self, coin_ids: list, vs_currency: str = "usd", days: int = 1) -> List[Dict[str, Any]]:
        """
        Récupère les données de marché pour une liste de cryptomonnaies.
        """
        try:
            endpoint = f"{self.base_url}/coins/markets"
            params = {
                "ids": ",".join(coin_ids) if isinstance(coin_ids, list) else coin_ids,
                "vs_currency": vs_currency,
                "days": days,
                "per_page": 250,
                "page": 1
            }
            return self._make_request(endpoint, params)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données de marché: {e}")
            raise

    def get_coin_price_history(self, coin_id: str, vs_currency: str = "usd", days: int = 30) -> Dict[str, Any]:
        """
        Récupère l'historique des prix pour une cryptomonnaie.
        """
        try:
            endpoint = f"{self.base_url}/coins/{coin_id}/market_chart"
            params = {
                "vs_currency": vs_currency,
                "days": days,
                "interval": "daily"
            }
            return self._make_request(endpoint, params)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération de l'historique des prix: {e}")
            raise 