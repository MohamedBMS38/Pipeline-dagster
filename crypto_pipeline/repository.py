"""
Repository Dagster pour notre pipeline de cryptomonnaies.
"""

from dagster import repository, with_resources

from crypto_pipeline.definitions import defs, all_assets, resources
from crypto_pipeline.assets.crypto_assets import *

@repository
def crypto_repository():
    """Repository pour notre pipeline de cryptomonnaies."""
    # Convertir les listes en dictionnaires
    jobs_dict = {job.name: job for job in defs.jobs}
    schedules_dict = {schedule.name: schedule for schedule in defs.schedules}
    sensors_dict = {sensor.name: sensor for sensor in defs.sensors}
    
    return {
        "jobs": jobs_dict,
        "schedules": schedules_dict,
        "sensors": sensors_dict
    } 