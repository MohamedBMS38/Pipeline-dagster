"""
Schedules Dagster pour notre pipeline de cryptomonnaies.
"""

from dagster import (
    schedule,
    ScheduleDefinition,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    AssetSelection,
    RunRequest,
    SkipReason
)
from datetime import datetime, timedelta
from crypto_pipeline.jobs.crypto_jobs import (
    crypto_metadata_job,
    crypto_market_data_job,
    crypto_price_history_job,
    crypto_analytics_job,
    crypto_monthly_report_job
)

# Partitions quotidiennes pour les 6 derniers mois
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date=datetime.now() - timedelta(days=180))

# Partitions mensuelles à partir de 2023
MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date=datetime(2023, 1, 1))

# Schedule pour le job de métadonnées (désactivé)
metadata_schedule = ScheduleDefinition(
    name="metadata_schedule",
    cron_schedule="0 0 * * 0",  # Toutes les heures
    job=crypto_metadata_job,
    execution_timezone="Europe/Paris",
    should_execute=lambda context: False,  # Désactivé
)

# Schedule pour le job de données de marché (désactivé)
market_data_schedule = ScheduleDefinition(
    name="market_data_schedule",
    cron_schedule="0 * * * *",  # Toutes les heures
    job=crypto_market_data_job,
    execution_timezone="Europe/Paris",
    should_execute=lambda context: False,  # Désactivé
)

# Schedule pour le job d'historique des prix (désactivé)
price_history_schedule = ScheduleDefinition(
    name="price_history_schedule",
    cron_schedule="0 * * * *",  # Toutes les heures
    job=crypto_price_history_job,
    execution_timezone="Europe/Paris",
    should_execute=lambda context: False,  # Désactivé
)

# Schedule pour le job d'analyse (désactivé)
analytics_schedule = ScheduleDefinition(
    name="analytics_schedule",
    cron_schedule="0 * * * *",  # Toutes les heures
    job=crypto_analytics_job,
    execution_timezone="Europe/Paris",
    should_execute=lambda context: False,  # Désactivé
)

# Schedule pour le rapport mensuel (désactivé)
monthly_report_schedule = ScheduleDefinition(
    name="monthly_report_schedule",
    cron_schedule="0 0 1 * *",  # Le premier jour de chaque mois à minuit
    job=crypto_monthly_report_job,
    execution_timezone="Europe/Paris",
    should_execute=lambda context: False,  # Désactivé
) 