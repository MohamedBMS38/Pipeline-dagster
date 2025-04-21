"""
Ressource pour DuckDB.
"""

import os
import pandas as pd
import duckdb
from dagster import ConfigurableResource, get_dagster_logger
from typing import List, Dict, Any
from datetime import datetime

logger = get_dagster_logger()

class DuckDBResource(ConfigurableResource):
    """
    Ressource pour interagir avec DuckDB.
    """
    database_path: str = "crypto_pipeline/data/crypto.duckdb"
    
    def __post_init__(self):
        # Créer le répertoire de données s'il n'existe pas
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
    
    def _get_connection(self):
        """
        Établit une connexion à la base de données DuckDB.
        """
        return duckdb.connect(self.database_path)
    
    def create_tables(self):
        """
        Crée les tables nécessaires dans la base de données.
        """
        with self._get_connection() as conn:
            # Table pour les métadonnées des cryptomonnaies
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_metadata (
                    id VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    name VARCHAR,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Table pour les données de marché des cryptomonnaies
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_market_data (
                    id VARCHAR,
                    date DATE,
                    price DECIMAL(18, 8),
                    market_cap DECIMAL(24, 8),
                    total_volume DECIMAL(24, 8),
                    high_24h DECIMAL(18, 8),
                    low_24h DECIMAL(18, 8),
                    price_change_percentage_24h DECIMAL(10, 2),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id, date)
                )
            """)
            
            # Table pour l'historique des prix
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_price_history (
                    id VARCHAR,
                    timestamp TIMESTAMP,
                    price DECIMAL(18, 8),
                    market_cap DECIMAL(24, 8),
                    total_volume DECIMAL(24, 8),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id, timestamp)
                )
            """)
            
            logger.info("Tables créées avec succès")
    
    def store_coin_list(self, coins_data):
        """
        Stocke la liste des cryptomonnaies dans la base de données.
        
        Args:
            coins_data: Liste des cryptomonnaies à stocker.
        """
        df = pd.DataFrame(coins_data)
        
        with self._get_connection() as conn:
            # Vérifier si la table existe et la créer si nécessaire
            conn.execute("CREATE TABLE IF NOT EXISTS crypto_metadata (id VARCHAR PRIMARY KEY, symbol VARCHAR, name VARCHAR, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            
            # Conversion du DataFrame en table temporaire
            conn.register("temp_coins", df)
            
            # Insertion des données avec gestion des doublons
            conn.execute("""
                INSERT OR REPLACE INTO crypto_metadata (id, symbol, name, updated_at)
                SELECT id, symbol, name, CURRENT_TIMESTAMP FROM temp_coins
            """)
            
            logger.info(f"{len(df)} cryptomonnaies stockées dans la base de données")
    
    def store_market_data(self, market_data):
        """
        Stocke les données de marché dans la base de données.
        
        Args:
            market_data: Données de marché à stocker.
        """
        if not market_data:
            logger.warning("Aucune donnée de marché à stocker")
            return
            
        df = pd.DataFrame(market_data)
        
        # Formatage des colonnes pour correspondre à notre schéma
        df = df.rename(columns={
            'id': 'id',
            'current_price': 'price',
            'market_cap': 'market_cap',
            'total_volume': 'total_volume',
            'high_24h': 'high_24h',
            'low_24h': 'low_24h',
            'price_change_percentage_24h': 'price_change_percentage_24h'
        })
        
        # Ajouter la date du jour
        df['date'] = pd.Timestamp.now().date()
        
        # Sélectionner uniquement les colonnes nécessaires
        cols = ['id', 'date', 'price', 'market_cap', 'total_volume', 
                'high_24h', 'low_24h', 'price_change_percentage_24h']
        df = df[cols]
        
        with self._get_connection() as conn:
            # Vérifier si la table existe et la créer si nécessaire
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_market_data (
                    id VARCHAR,
                    date DATE,
                    price DECIMAL(18, 8),
                    market_cap DECIMAL(24, 8),
                    total_volume DECIMAL(24, 8),
                    high_24h DECIMAL(18, 8),
                    low_24h DECIMAL(18, 8),
                    price_change_percentage_24h DECIMAL(10, 2),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id, date)
                )
            """)
            
            # Conversion du DataFrame en table temporaire
            conn.register("temp_market_data", df)
            
            # Insertion des données avec gestion des doublons
            conn.execute("""
                INSERT OR REPLACE INTO crypto_market_data 
                    (id, date, price, market_cap, total_volume, high_24h, low_24h, price_change_percentage_24h, updated_at)
                SELECT 
                    id, date, price, market_cap, total_volume, high_24h, low_24h, price_change_percentage_24h, 
                    CURRENT_TIMESTAMP
                FROM temp_market_data
            """)
            
            logger.info(f"{len(df)} entrées de données de marché stockées dans la base de données")
    
    def store_price_history(self, coin_id, price_history):
        """
        Stocke l'historique des prix dans la base de données.
        
        Args:
            coin_id: Identifiant de la cryptomonnaie.
            price_history: Historique des prix à stocker.
        """
        if not price_history or not price_history.get('prices'):
            logger.warning(f"Aucun historique de prix à stocker pour {coin_id}")
            return
            
        # Préparation des données de prix
        prices = price_history.get('prices', [])
        market_caps = price_history.get('market_caps', [])
        total_volumes = price_history.get('total_volumes', [])
        
        # Vérifier que les listes ont la même longueur
        if len(prices) != len(market_caps) or len(prices) != len(total_volumes):
            logger.error("Les données d'historique n'ont pas la même longueur")
            return
            
        # Créer des DataFrames pour chaque type de données
        df_prices = pd.DataFrame(prices, columns=['timestamp', 'price'])
        df_market_caps = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
        df_total_volumes = pd.DataFrame(total_volumes, columns=['timestamp', 'total_volume'])
        
        # Convertir les timestamps en datetime
        df_prices['timestamp'] = pd.to_datetime(df_prices['timestamp'], unit='ms')
        
        # Fusionner les DataFrames
        df = df_prices.copy()
        df['market_cap'] = df_market_caps['market_cap']
        df['total_volume'] = df_total_volumes['total_volume']
        df['id'] = coin_id
        
        with self._get_connection() as conn:
            # Vérifier si la table existe et la créer si nécessaire
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_price_history (
                    id VARCHAR,
                    timestamp TIMESTAMP,
                    price DECIMAL(18, 8),
                    market_cap DECIMAL(24, 8),
                    total_volume DECIMAL(24, 8),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id, timestamp)
                )
            """)
            
            # Conversion du DataFrame en table temporaire
            conn.register("temp_price_history", df)
            
            # Insertion des données avec gestion des doublons
            conn.execute("""
                INSERT OR REPLACE INTO crypto_price_history 
                    (id, timestamp, price, market_cap, total_volume, updated_at)
                SELECT 
                    id, timestamp, price, market_cap, total_volume, CURRENT_TIMESTAMP
                FROM temp_price_history
            """)
            
            logger.info(f"{len(df)} entrées d'historique de prix stockées pour {coin_id}")
    
    def get_top_coins(self, limit=10):
        """
        Récupère les cryptomonnaies les plus importantes par capitalisation.
        
        Args:
            limit: Nombre de cryptomonnaies à récupérer.
        
        Returns:
            DataFrame pandas contenant les résultats.
        """
        with self._get_connection() as conn:
            # Vérifier si les tables existent
            tables_query = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
            tables = tables_query['name'].tolist() if not tables_query.empty else []
            
            # Si les tables nécessaires n'existent pas, retourner un DataFrame vide
            if 'crypto_market_data' not in tables or 'crypto_metadata' not in tables:
                logger.warning("Les tables crypto_market_data ou crypto_metadata n'existent pas encore")
                return pd.DataFrame(columns=['id', 'name', 'price', 'market_cap', 'price_change_percentage_24h'])
                
            # Vérifier si les tables contiennent des données
            count = conn.execute("SELECT COUNT(*) FROM crypto_market_data").fetchone()[0]
            if count == 0:
                logger.warning("La table crypto_market_data est vide")
                return pd.DataFrame(columns=['id', 'name', 'price', 'market_cap', 'price_change_percentage_24h'])
                
            try:
                result = conn.execute(f"""
                    SELECT m.id, meta.name, m.price, m.market_cap, m.price_change_percentage_24h
                    FROM crypto_market_data m
                    JOIN crypto_metadata meta ON m.id = meta.id
                    WHERE m.date = (SELECT MAX(date) FROM crypto_market_data)
                    ORDER BY m.market_cap DESC
                    LIMIT {limit}
                """).fetchdf()
                return result
            except Exception as e:
                logger.error(f"Erreur lors de la récupération des top coins : {e}")
                return pd.DataFrame(columns=['id', 'name', 'price', 'market_cap', 'price_change_percentage_24h'])
    
    def get_price_history_for_coin(self, coin_id, days=30):
        """
        Récupère l'historique des prix pour une cryptomonnaie.
        
        Args:
            coin_id: Identifiant de la cryptomonnaie.
            days: Nombre de jours d'historique à récupérer.
        
        Returns:
            DataFrame pandas contenant les résultats.
        """
        with self._get_connection() as conn:
            # Vérifier si les tables existent
            tables_query = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
            tables = tables_query['name'].tolist() if not tables_query.empty else []
            
            # Si la table nécessaire n'existe pas, retourner un DataFrame vide
            if 'crypto_price_history' not in tables:
                logger.warning("La table crypto_price_history n'existe pas encore")
                return pd.DataFrame(columns=['timestamp', 'price', 'market_cap', 'total_volume'])
                
            # Vérifier si la table contient des données pour cette crypto
            count = conn.execute(f"SELECT COUNT(*) FROM crypto_price_history WHERE id = '{coin_id}'").fetchone()[0]
            if count == 0:
                logger.warning(f"Pas de données d'historique pour {coin_id}")
                return pd.DataFrame(columns=['timestamp', 'price', 'market_cap', 'total_volume'])
                
            try:
                result = conn.execute(f"""
                    SELECT timestamp, price, market_cap, total_volume
                    FROM crypto_price_history
                    WHERE id = '{coin_id}'
                    AND timestamp >= CURRENT_DATE - INTERVAL '{days} days'
                    ORDER BY timestamp
                """).fetchdf()
                return result
            except Exception as e:
                logger.error(f"Erreur lors de la récupération de l'historique des prix pour {coin_id} : {e}")
                return pd.DataFrame(columns=['timestamp', 'price', 'market_cap', 'total_volume'])
    
    def get_coin_price_history(self, coin_id: str) -> pd.DataFrame:
        """
        Récupère l'historique des prix pour une cryptomonnaie.
        
        Args:
            coin_id: Identifiant de la cryptomonnaie.
        
        Returns:
            DataFrame pandas contenant les résultats.
        """
        try:
            with self._get_connection() as conn:
                # Vérifier si la table existe
                tables = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
                table_names = [t[0] for t in tables]
                
                if 'crypto_price_history' not in table_names:
                    logger.warning("La table crypto_price_history n'existe pas encore")
                    return pd.DataFrame(columns=['timestamp', 'price', 'market_cap', 'total_volume'])
                    
                # Vérifier si la table contient des données pour cette crypto
                count = conn.execute(
                    "SELECT COUNT(*) FROM crypto_price_history WHERE id = ?",
                    (coin_id,)
                ).fetchone()[0]
                
                if count == 0:
                    logger.warning(f"Pas de données d'historique pour {coin_id}")
                    return pd.DataFrame(columns=['timestamp', 'price', 'market_cap', 'total_volume'])
                    
                # Récupérer l'historique des prix
                result = conn.execute(
                    """
                    SELECT timestamp, price
                    FROM crypto_price_history
                    WHERE id = ?
                    ORDER BY timestamp ASC
                    """,
                    (coin_id,)
                ).df()
                
                return result
                
        except Exception as e:
            logger.error(f"Erreur lors de la récupération de l'historique des prix pour {coin_id} : {e}")
            return pd.DataFrame(columns=['timestamp', 'price', 'market_cap', 'total_volume'])