"""
Capteurs (sensors) Dagster pour la pipeline de cryptomonnaies.
"""

from dagster import (
    sensor,
    RunRequest,
    DefaultSensorStatus,
    SensorEvaluationContext,
    SensorResult,
    PartitionKeyRange
)
import os
from datetime import datetime, timedelta
from crypto_pipeline.jobs.crypto_jobs import (
    crypto_analytics_job,
    crypto_market_data_job
)
from crypto_pipeline.assets.crypto_assets import DAILY_PARTITIONS, START_DATE

# Capteur pour détecter les mouvements de prix importants et déclencher une analyse
@sensor(
    job=crypto_analytics_job,
    minimum_interval_seconds=60 * 30,  # Vérifier toutes les 30 minutes
    description="Capteur qui détecte les mouvements de prix importants et déclenche une analyse",
    default_status=DefaultSensorStatus.STOPPED,
)
def price_movement_sensor(context: SensorEvaluationContext):
    """
    Détecte les mouvements de prix importants sur les cryptomonnaies suivies.
    
    Ce capteur vérifie si des données de marché récentes ont été mises à jour et s'il y a
    des mouvements de prix significatifs (>5% dans les dernières 24 heures),
    puis déclenche un job d'analyse si nécessaire.
    """
    # Dans une implémentation réelle, on vérifierait la base de données pour les mouvements de prix
    # Pour cet exemple simple, on déclenche le job une fois par jour
    
    # Vérifier si le job a déjà été déclenché aujourd'hui
    last_run_time = context.last_run_key
    current_time = datetime.now().strftime("%Y-%m-%d")
    
    if last_run_time != current_time:
        context.log.info(f"Déclenchement de l'analyse des prix pour la journée {current_time}")
        return RunRequest(
            run_key=current_time,
            tags={"sensor": "price_movement", "reason": "daily_trigger", "date": current_time}
        )
    
    return None

# Capteur pour surveiller la disponibilité de nouvelles données sur l'API
@sensor(
    job=crypto_market_data_job,
    minimum_interval_seconds=60 * 15,  # Vérifier toutes les 15 minutes
    description="Capteur qui vérifie la disponibilité de nouvelles données sur l'API CoinGecko",
    default_status=DefaultSensorStatus.STOPPED,
)
def api_data_sensor(context: SensorEvaluationContext):
    """
    Vérifie la disponibilité de nouvelles données sur l'API CoinGecko.
    
    Ce capteur surveille les mises à jour de l'API et déclenche un job d'extraction de données
    si de nouvelles données sont disponibles (basé sur un simple intervalle de temps).
    """
    # Dans une implémentation réelle, on vérifierait réellement si l'API a de nouvelles données
    # Pour cet exemple simple, on déclenche le job toutes les heures
    
    last_run_time = context.last_run_key
    current_hour = datetime.now().strftime("%Y-%m-%d-%H")
    
    # Utiliser une partition qui existe dans notre définition
    # Nous utilisons la date de début de notre définition de partitions
    partition_key = START_DATE.strftime("%Y-%m-%d")
    
    if last_run_time != current_hour:
        context.log.info(f"Nouvelles données détectées sur l'API pour l'heure {current_hour}")
        
        # Utiliser une partition existante (premier jour de notre définition)
        return RunRequest(
            run_key=current_hour,
            tags={"sensor": "api_data", "reason": "hourly_update", "hour": current_hour},
            partition_key=partition_key  # Utiliser une partition qui existe de manière certaine
        )
    
    return None

# Capteur pour surveiller la création de fichiers de visualisation
@sensor(
    job=None,  # Ce capteur ne déclenche pas directement un job
    minimum_interval_seconds=60 * 60,  # Vérifier toutes les heures
    description="Capteur qui surveille la création de fichiers de visualisation",
    default_status=DefaultSensorStatus.STOPPED,
)
def visualization_files_sensor(context: SensorEvaluationContext):
    """
    Surveille la création de fichiers de visualisation.
    
    Ce capteur vérifie si de nouveaux fichiers de visualisation ont été créés et
    les enregistre pour référence (dans un cas réel, on pourrait envoyer des notifications).
    """
    visualizations_dir = "crypto_pipeline/data"
    detected_files = []
    
    # Vérifier si le répertoire existe
    if not os.path.exists(visualizations_dir):
        return None
    
    # Trouver tous les fichiers de visualisation créés dans les dernières 24 heures
    now = datetime.now()
    for file in os.listdir(visualizations_dir):
        if file.endswith(".png"):
            file_path = os.path.join(visualizations_dir, file)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            # Si le fichier a été créé/modifié dans les dernières 24 heures
            if now - file_mtime < timedelta(days=1):
                detected_files.append(file)
    
    if detected_files:
        context.log.info(f"Fichiers de visualisation détectés: {', '.join(detected_files)}")
        # Modifier pour envoyer un email ou une notification par exemple
    # Ce capteur ne déclenche pas de job, il enregistre simplement l'information
    return None 