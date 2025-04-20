# Pipeline de Données Cryptomonnaies avec Dagster

Ce projet implémente un pipeline de données pour l'analyse des cryptomonnaies en utilisant Dagster comme orchestrateur. Il extrait des données de l'API CoinGecko, les stocke dans une base de données DuckDB, et génère des analyses et visualisations.

## Architecture du Projet

Le projet est structuré en plusieurs composants :

1. **Extraction** (`crypto_pipeline/assets/extract/`)
   - Récupération des métadonnées des cryptomonnaies
   - Extraction des données de marché
   - Historique des prix

2. **Stockage** (`crypto_pipeline/assets/load/`)
   - Stockage des données dans DuckDB
   - Gestion des partitions temporelles

3. **Transformation** (`crypto_pipeline/assets/transform/`)
   - Analyse des tendances de prix
   - Calcul des indicateurs techniques

4. **Visualisation** (`crypto_pipeline/assets/visualize/`)
   - Génération de graphiques
   - Rapports mensuels

5. **Orchestration** (`crypto_pipeline/`)
   - Jobs Dagster pour l'exécution des étapes
   - Schedules pour l'automatisation
   - Sensors pour la détection de changements

## Prérequis Système

- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)
- Git
- 500 Mo d'espace disque minimum
- Connexion Internet

## Installation

1. **Cloner le repository**
   ```bash
   git clone https://github.com/MohamedBMS38/Pipeline-dagster.git
   cd Pipeline-dagster
   ```

2. **Créer et activer un environnement virtuel**
   ```bash
   # Windows
   python -m venv venv
   .\venv\Scripts\activate

   # Linux/Mac
   python -m venv venv
   source venv/bin/activate
   ```

3. **Installer les dépendances**
   ```bash
   pip install -e .
   ```

4. **Configurer les variables d'environnement**
   ```bash
   # Copier le fichier d'exemple
   cp .env.example .env
   ```
   Modifier le fichier `.env` avec vos configurations :
   ```
   DUCKDB_PATH=crypto_pipeline/data/crypto.duckdb
   ```

## Utilisation

### Lancement de l'Interface Dagster

```bash
python -m dagster dev -m crypto_pipeline -p 3000
```

Accédez à l'interface web : http://localhost:3000

### Exécution des Jobs

Les jobs sont désactivés par défaut pour éviter une exécution automatique. Pour les exécuter :

1. **Job de Métadonnées** (Étape 1)
   - Description : Extrait la liste des cryptomonnaies
   - Exécution manuelle requise
   - Fréquence recommandée : Une fois par semaine

2. **Job de Données de Marché** (Étape 2)
   - Description : Récupère les données de marché actuelles
   - Exécution manuelle requise
   - Fréquence recommandée : Toutes les heures

3. **Job d'Historique des Prix** (Étape 3)
   - Description : Extrait l'historique des prix
   - Exécution manuelle requise
   - Fréquence recommandée : Toutes les heures

4. **Job d'Analyse** (Étape 4)
   - Description : Génère les analyses et visualisations
   - Exécution manuelle requise
   - Fréquence recommandée : Toutes les heures

5. **Job de Rapport Mensuel** (Étape 5)
   - Description : Génère le rapport mensuel
   - Exécution manuelle requise
   - Fréquence recommandée : Une fois par mois

### Ordre d'Exécution Recommandé

1. Lancer le job de métadonnées
2. Attendre sa complétion
3. Lancer le job de données de marché
4. Attendre sa complétion
5. Lancer le job d'historique des prix
6. Attendre sa complétion
7. Lancer le job d'analyse
8. Attendre sa complétion
9. Lancer le job de rapport mensuel (si nécessaire)

## Structure des Données

Les données sont stockées dans une base DuckDB avec les tables suivantes :

- `crypto_metadata` : Informations sur les cryptomonnaies
- `market_data` : Données de marché quotidiennes
- `price_history` : Historique des prix
- `price_trends` : Analyses des tendances

## Visualisations

Les visualisations sont générées dans le dossier `crypto_pipeline/data/` :
- `price_trends.png` : Graphique des tendances de prix
- `monthly_report_YYYY-MM.png` : Rapports mensuels

## Dépannage

Si vous rencontrez des problèmes :

1. **Erreur de Port**
   ```bash
   # Changer le port si 3000 est occupé
   python -m dagster dev -m crypto_pipeline -p 3001
   ```

2. **Erreur de Base de Données**
   - Vérifier que le chemin dans `.env` est correct
   - S'assurer que le dossier `crypto_pipeline/data/` existe

3. **Erreur d'API**
   - Vérifier la connexion Internet
   - S'assurer que l'API CoinGecko est accessible

## Tests Unitaires

Le projet inclut des tests unitaires pour assurer la fiabilité du code. Les tests sont organisés dans le dossier `crypto_pipeline/tests/`.

### Structure des Tests

```
crypto_pipeline/tests/
├── test_extract/          # Tests des fonctions d'extraction
├── test_load/             # Tests des fonctions de chargement
├── test_transform/        # Tests des fonctions de transformation
├── test_visualize/        # Tests des fonctions de visualisation
└── test_utils/            # Tests des fonctions utilitaires
```

### Exécution des Tests

1. **Lancer tous les tests**
   ```bash
   python -m pytest crypto_pipeline/tests/
   ```

2. **Lancer les tests d'un module spécifique**
   ```bash
   # Tests d'extraction
   python -m pytest crypto_pipeline/tests/test_extract/

   # Tests de chargement
   python -m pytest crypto_pipeline/tests/test_load/

   # Tests de transformation
   python -m pytest crypto_pipeline/tests/test_transform/

   # Tests de visualisation
   python -m pytest crypto_pipeline/tests/test_visualize/
   ```

3. **Lancer un test spécifique**
   ```bash
   python -m pytest crypto_pipeline/tests/test_extract/test_coingecko_api.py::test_get_crypto_list
   ```

4. **Générer un rapport de couverture**
   ```bash
   python -m pytest --cov=crypto_pipeline crypto_pipeline/tests/
   ```

### Types de Tests

1. **Tests d'API**
   - Vérification des appels à l'API CoinGecko
   - Tests des limites de taux
   - Gestion des erreurs

2. **Tests de Base de Données**
   - Création des tables
   - Insertion des données
   - Requêtes de sélection

3. **Tests de Transformation**
   - Calcul des indicateurs techniques
   - Analyse des tendances
   - Agrégation des données

4. **Tests de Visualisation**
   - Génération des graphiques
   - Format des fichiers
   - Contenu des visualisations

### Bonnes Pratiques

1. **Avant d'écrire un test**
   - Identifier la fonctionnalité à tester
   - Définir les cas de test
   - Préparer les données de test

2. **Pendant l'écriture du test**
   - Utiliser des fixtures pour les données de test
   - Tester les cas limites
   - Vérifier les erreurs attendues

3. **Après l'écriture du test**
   - Vérifier la couverture du code
   - Documenter les cas de test
   - Maintenir les tests à jour

### Exemple de Test

```python
def test_get_crypto_list():
    """Test de la récupération de la liste des cryptomonnaies."""
    # Préparation
    api = CoinGeckoAPI()
    
    # Exécution
    result = api.get_crypto_list()
    
    # Vérification
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(coin, dict) for coin in result)
```

## Contribution

Les contributions sont les bienvenues ! Pour contribuer :

1. Fork le projet
2. Créer une branche pour votre fonctionnalité
3. Commiter vos changements
4. Pousser vers la branche
5. Ouvrir une Pull Request

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.