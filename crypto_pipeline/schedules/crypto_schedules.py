"""
Schedules Dagster pour notre pipeline de cryptomonnaies.
"""

from dagster import (
    schedule,
    ScheduleDefinition,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    AssetSelection
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

# Schedule pour mettre à jour les métadonnées (liste des cryptomonnaies) de manière hebdomadaire
weekly_metadata_update_schedule = ScheduleDefinition(
    job_name="crypto_metadata_job",
    cron_schedule="0 0 * * 0",  # tous les dimanches à minuit
    execution_timezone="Europe/Paris",
    description="Met à jour la liste des cryptomonnaies de manière hebdomadaire",
)

# Schedule pour mettre à jour les données de marché quotidiennement
market_data_update_schedule = ScheduleDefinition(
    job_name="crypto_market_data_job",
    cron_schedule="0 8 * * *",  # tous les jours à 8h du matin
    execution_timezone="Europe/Paris",
    description="Met à jour les données de marché quotidiennement",
)

# Schedule pour mettre à jour l'historique des prix quotidiennement
price_history_update_schedule = ScheduleDefinition(
    job_name="crypto_price_history_job",
    cron_schedule="0 9 * * *",  # tous les jours à 9h du matin
    execution_timezone="Europe/Paris",
    description="Met à jour l'historique des prix quotidiennement",
)

# Schedule pour mettre à jour les analyses quotidiennement
analytics_update_schedule = ScheduleDefinition(
    job_name="crypto_analytics_job",
    cron_schedule="0 10 * * *",  # tous les jours à 10h du matin
    execution_timezone="Europe/Paris",
    description="Met à jour les analyses quotidiennement",
)

# Schedule pour générer un rapport mensuel
monthly_report_schedule = ScheduleDefinition(
    job_name="crypto_monthly_report_job",
    cron_schedule="0 0 1 * *",  # le premier jour de chaque mois à minuit
    execution_timezone="Europe/Paris",
    description="Génère un rapport mensuel sur les performances des cryptomonnaies",
) 