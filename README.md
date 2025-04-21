# Pipeline de Données Cryptomonnaies avec Dagster

Ce projet implémente un pipeline de données pour l'analyse des cryptomonnaies en utilisant Dagster comme orchestrateur. Il extrait des données de l'API CoinGecko, les stocke dans une base de données DuckDB, et génère des analyses et visualisations.
Pour ce faire, nous avons utilisé les technologies suivantes :
- **Dagster** comme orchestrateur de flux de données
- **API CoinGecko** comme source de données sur les cryptomonnaies
- **DuckDB** comme base de données analytique légère
- **Pandas** pour la manipulation des données
- **Matplotlib** pour les visualisations

## Composants principaux

1. **Ressources** (`resources/`)
   - `CoinGeckoResource` : Interface avec l'API CoinGecko
   - `DuckDBResource` : Interface avec la base de données DuckDB

2. **Assets** (`assets/`)
   - Extraction des métadonnées des cryptomonnaies
   - Extraction des données de marché
   - Extraction de l'historique des prix
   - Stockage des données
   - Analyse des tendances
   - Visualisations

3. **Jobs** (`jobs/`)
   - Jobs spécifiques pour chaque étape du processus

4. **Schedules** (`schedules/`)
   - Exécution hebdomadaire pour les métadonnées
   - Exécution périodique pour les données de marché
   - Exécution périodique pour l'historique des prix
   - Execution périodique pour les analyses
   - Exécution mensuelle pour les rapports

5. **Sensors** (`sensors/`)
   - Détection des mouvements de prix significatifs
   - Surveillance de la disponibilité des données
   - Surveillance de la création de visualisations

6. **Utilis** (`utils/`)
   - Fonctions de visualisation avancées pour les tests

7. **Tests** (`tests/`)
   - Tests unitaires pour les ressources
   - Tests unitaires pour les assets
   - Tests unitaires pour les jobs
   - Tests unitaires pour les visualisations

## Fonctionnalités implémentées

- Extraction automatique des données de l'API CoinGecko
- Stockage efficace dans DuckDB avec schéma optimisé
- Analyse des tendances de prix des cryptomonnaies
- Visualisation comparative des performances
- Génération de rapports mensuels
- Orchestration complète avec partitionnement des données
- Conteneurisation avec Docker pour un déploiement facile

## Installation et Configuration

1. **Installation locale** : Suivez les instructions ci dessous
2. **Docker** : Utilisez `docker-compose up` depuis la racine du projet pour démarrer le projet dans un conteneur
3. **Interface Dagster** : Accédez à `http://localhost:3000` pour interagir avec la pipeline

## Prérequis Système

- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)
- Git
- 2 GO d'espace disque minimum
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
- `crypto_market_data` : Données de marché quotidiennes
- `crypto_price_history` : Historique des prix

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

### Exécution des Tests

```bash
python -m pytest crypto_pipeline/tests/
```
