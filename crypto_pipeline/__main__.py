"""
Point d'entrée principal pour la pipeline de données Dagster.
"""

from crypto_pipeline.definitions import defs, all_assets, resources

if __name__ == "__main__":
    # Pour faciliter l'exécution directe de ce script
    from dagster import materialize
    
    # Exécuter tous les assets
    result = materialize(
        assets=all_assets,
        resources=resources,
    )
    
    print(f"Exécution terminée avec statut: {result.success}") 