"""
Ressource pour l'API CoinGecko.
"""

import requests
import time
from dagster import ConfigurableResource, get_dagster_logger
from typing import List, Dict, Any

logger = get_dagster_logger()

class CoinGeckoResource(ConfigurableResource):
    """
    Ressource pour interagir avec l'API CoinGecko.
    """
    base_url: str = "https://api.coingecko.com/api/v3"
    rate_limit_delay: int = 2  # Attendre 2 secondes entre chaque requête
    max_retries: int = 3  # Nombre maximum de tentatives en cas d'erreur
    
    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Effectue une requête HTTP avec gestion des erreurs et des limites de taux.
        """
        url = f"{self.base_url}{endpoint}"
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
                        wait_time = (attempt + 1) * self.rate_limit_delay
                        logger.warning(f"Rate limit atteint. Attente de {wait_time} secondes...")
                        time.sleep(wait_time)
                        continue
                logger.error(f"Erreur HTTP lors de la requête: {e}")
                raise
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Erreur de requête. Nouvelle tentative dans {self.rate_limit_delay} secondes...")
                    time.sleep(self.rate_limit_delay)
                    continue
                logger.error(f"Erreur lors de la requête: {e}")
                raise

    def get_coin_list(self) -> List[Dict[str, Any]]:
        """
        Récupère la liste des cryptomonnaies disponibles.
        """
        try:
            logger.info("Récupération de la liste des cryptomonnaies")
            return self._make_request("/coins/list")
        except Exception as e:
            logger.error(f"Erreur lors de la récupération de la liste des cryptomonnaies: {e}")
            raise

    def get_coin_market_data(self, coin_ids: list, vs_currency: str = "usd", days: int = 1) -> List[Dict[str, Any]]:
        """
        Récupère les données de marché pour une liste de cryptomonnaies.
        """
        try:
            if isinstance(coin_ids, list):
                coin_ids = ",".join(coin_ids)
            
            logger.info(f"Récupération des données de marché pour {coin_ids}")
            params = {
                "ids": coin_ids,
                "vs_currency": vs_currency,
                "days": days,
                "per_page": 250,
                "page": 1
            }
            return self._make_request("/coins/markets", params)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données de marché: {e}")
            raise

    def get_coin_price_history(self, coin_id: str, vs_currency: str = "usd", days: int = 30) -> Dict[str, Any]:
        """
        Récupère l'historique des prix pour une cryptomonnaie.
        """
        try:
            logger.info(f"Récupération de l'historique des prix pour {coin_id}")
            params = {
                "vs_currency": vs_currency,
                "days": days,
                "interval": "daily"
            }
            return self._make_request(f"/coins/{coin_id}/market_chart", params)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération de l'historique des prix: {e}")
            raise 