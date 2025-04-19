# Pipeline de Données Crypto avec Dagster et CoinGecko

Ce projet implémente une pipeline de données complète pour extraire, stocker, transformer et visualiser des données de cryptomonnaies en utilisant l'API CoinGecko, DuckDB comme base de données et Dagster comme orchestrateur.

## 📋 Fonctionnalités

- **Extraction de données** depuis l'API CoinGecko
- **Stockage** des données dans DuckDB
- **Transformation** et analyse des données
- **Visualisation** des tendances de prix
- **Orchestration** avec Dagster (jobs, assets, schedules, sensors)
- **Tests unitaires** pour assurer la fiabilité

## 🛠️ Architecture du projet

```
crypto_pipeline/
├── assets/            # Assets Dagster (composants de données)
├── jobs/              # Jobs Dagster (définitions des workflows)
├── resources/         # Ressources Dagster (connexions externes)
├── schedules/         # Planifications d'exécution
├── sensors/           # Capteurs pour détecter des événements
├── tests/             # Tests unitaires
├── utils/             # Fonctions utilitaires
├── data/              # Données et visualisations générées
└── __main__.py        # Point d'entrée principal
```

## 📦 Installation

### Prérequis

- Python 3.8+
- pip (gestionnaire de paquets Python)

### 1. Créer un environnement virtuel

```bash
python -m venv venv
```

### 2. Activer l'environnement virtuel

#### Windows:
```bash
venv\Scripts\activate
```

#### macOS/Linux:
```bash
source venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install dagster dagster-webserver requests pandas duckdb python-dotenv matplotlib pytest pyarrow
```

## 🚀 Utilisation

### Démarrer le serveur Dagster

```bash
dagster dev
```

Ouvrez un navigateur et accédez à `http://localhost:3000` pour interagir avec l'interface Dagster.

### Exécuter la pipeline complète

```bash
python -m crypto_pipeline
```

### Exécuter un job spécifique

```bash
dagster job execute -f crypto_pipeline/__main__.py -a crypto_metadata_job
```

### Exécuter les tests

```bash
pytest crypto_pipeline/tests/
```

## 🔄 Pipeline de données

La pipeline comprend les étapes suivantes:

1. **Extraction** des données depuis l'API CoinGecko
   - Liste des cryptomonnaies
   - Données de marché actuelles
   - Historique des prix

2. **Stockage** dans DuckDB
   - Tables pour les métadonnées, données de marché et historique des prix

3. **Transformation** des données
   - Analyse des tendances de prix
   - Calculs statistiques

4. **Visualisation**
   - Graphiques d'évolution des prix
   - Comparaisons entre cryptomonnaies
   - Rapports mensuels

## ⏱️ Planification et automatisation

Le projet utilise les fonctionnalités d'orchestration de Dagster:

- **Schedules**: Exécution planifiée des jobs (quotidienne, hebdomadaire, mensuelle)
- **Sensors**: Déclenchement basé sur des événements (mouvement de prix, disponibilité de données)
- **Partitions**: Segmentation des données par jour/mois

## 📊 Visualisations

Les visualisations sont générées dans le répertoire `crypto_pipeline/data/` et comprennent:

- Graphiques d'évolution des prix
- Aperçu du marché des cryptomonnaies
- Rapports mensuels comparatifs

## 📌 Configuration

Vous pouvez configurer l'application via des variables d'environnement:

- `DUCKDB_PATH`: Chemin vers la base de données DuckDB (par défaut: `crypto_pipeline/data/crypto.duckdb`)

Créez un fichier `.env` à la racine du projet pour définir ces variables:

```
DUCKDB_PATH=chemin/vers/ma/base.duckdb
```

## 🔍 Dépannage

### API CoinGecko

L'API CoinGecko gratuite a des limites de taux. Si vous rencontrez des erreurs de limitation de taux, ralentissez vos requêtes ou envisagez d'utiliser une clé API payante.

### Problèmes de base de données

Si vous rencontrez des problèmes avec DuckDB, essayez de supprimer le fichier de base de données et laissez l'application le recréer.

## 🤝 Contribution

Les contributions sont les bienvenues! N'hésitez pas à ouvrir une issue ou soumettre une pull request.

## 📜 Licence

Ce projet est sous licence MIT.

---

Développé dans le cadre d'un projet éducatif sur l'ingénierie des données avec Dagster. 