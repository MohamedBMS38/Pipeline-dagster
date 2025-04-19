"""
Script pour tester l'exécution d'un job spécifique.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from dagster import execute_job

from crypto_pipeline.definitions import resources
from crypto_pipeline.jobs.crypto_jobs import crypto_metadata_job

def test_metadata_job():
    """Teste l'exécution du job crypto_metadata_job."""
    print("=== Test du job crypto_metadata_job ===")
    
    try:
        # Exécuter le job
        result = execute_job(
            crypto_metadata_job,
            resources=resources,
        )
        
        # Vérifier le résultat
        if result.success:
            print("✅ Exécution du job réussie")
            
            # Afficher les logs
            for event in result.get_job_logs():
                if event.is_dagster_event and event.dagster_event.is_asset_materialization:
                    print(f"✅ Asset matérialisé: {event.dagster_event.node_name}")
            
            return True
        else:
            print("❌ Échec de l'exécution du job")
            
            # Afficher les erreurs
            for event in result.get_job_logs():
                if event.is_dagster_event and event.dagster_event.is_step_failure:
                    print(f"❌ Erreur dans l'étape {event.dagster_event.node_name}: {event.dagster_event.step_exception}")
            
            return False
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution du job: {e}")
        return False

if __name__ == "__main__":
    success = test_metadata_job()
    print(f"\n=== Résultat du test ===")
    print(f"Job crypto_metadata_job: {'✅ Success' if success else '❌ Failed'}") 