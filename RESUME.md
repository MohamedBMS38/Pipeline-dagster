# Résumé du Projet de Pipeline de Données Crypto

## Ce que nous avons réalisé

Nous avons créé une pipeline de données complète pour collecter, stocker, analyser et visualiser des données de cryptomonnaies en utilisant les technologies suivantes :

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
   - Job complet pour exécuter toute la pipeline

4. **Planifications** (`schedules/`)
   - Exécution hebdomadaire pour les métadonnées
   - Exécution périodique pour les données de marché
   - Exécution quotidienne pour l'historique des prix
   - Exécution mensuelle pour les rapports

5. **Capteurs** (`sensors/`)
   - Détection des mouvements de prix significatifs
   - Surveillance de la disponibilité des données
   - Surveillance de la création de visualisations

6. **Utilitaires** (`utils/`)
   - Fonctions de visualisation avancées

7. **Tests** (`tests/`)
   - Tests unitaires pour les ressources
   - Tests unitaires pour les assets

## Fonctionnalités implémentées

- Extraction automatique des données de l'API CoinGecko
- Stockage efficace dans DuckDB avec schéma optimisé
- Analyse des tendances de prix des cryptomonnaies
- Visualisation comparative des performances
- Génération de rapports mensuels
- Orchestration complète avec partitionnement des données
- Tests unitaires pour garantir la fiabilité
- Conteneurisation avec Docker pour un déploiement facile

## Comment utiliser

1. **Installation locale** : Suivez les instructions dans le README.md
2. **Docker** : Utilisez `docker-compose up` pour démarrer le projet dans un conteneur
3. **Interface Dagster** : Accédez à `http://localhost:3000` pour interagir avec la pipeline

## Points forts du projet

- **Architecture modulaire** : Facile à étendre et à maintenir
- **Orchestration robuste** : Gestion des erreurs et des reprises
- **Visualisations informatives** : Aide à la prise de décision
- **Tests complets** : Assure la qualité du code
- **Documentation détaillée** : Facilite l'utilisation et la contribution

Ce projet répond pleinement aux exigences d'une pipeline de données ETL/ELT moderne utilisant Dagster comme orchestrateur central. 