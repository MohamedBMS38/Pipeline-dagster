"""
Définitions Dagster pour notre pipeline de cryptomonnaies.
"""

from dagster import Definitions, load_assets_from_modules
import os

from crypto_pipeline.resources.coingecko_resource import CoinGeckoResource
from crypto_pipeline.resources.duckdb_resource import DuckDBResource
from crypto_pipeline import assets, jobs, schedules, sensors

# Chargement des assets depuis le module assets
all_assets = load_assets_from_modules([assets])

# Définition des ressources
resources = {
    "coingecko_resource": CoinGeckoResource(),
    "duckdb_resource": DuckDBResource(
        database_path=os.environ.get("DUCKDB_PATH", "crypto_pipeline/data/crypto.duckdb")
    ),
}

# Définition de l'application Dagster
defs = Definitions(
    assets=all_assets,
    jobs=[
        jobs.crypto_metadata_job,
        jobs.crypto_market_data_job,
        jobs.crypto_price_history_job,
        jobs.crypto_analytics_job,
        jobs.crypto_monthly_report_job,
    ],
    schedules=[
        schedules.weekly_metadata_update_schedule,
        schedules.market_data_update_schedule,
        schedules.price_history_update_schedule,
        schedules.analytics_update_schedule,
        schedules.monthly_report_schedule,
    ],
    sensors=[
        sensors.price_movement_sensor,
        sensors.api_data_sensor,
        sensors.visualization_files_sensor,
    ],
    resources=resources,
) 