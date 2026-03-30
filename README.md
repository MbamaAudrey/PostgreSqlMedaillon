# 🏦 Modern Data Lakehouse - Archer-Medaillon

Ce projet implémente une architecture de données de type **Lakehouse** utilisant l'approche **Medaillon** (Bronze, Silver, Gold). Il simule un environnement bancaire avec 10 semaines de données transactionnelles évolutives.

---

## 🏗️ Architecture Technique

Le projet repose sur une stack moderne et conteneurisée :
*   **Base de données** : PostgreSQL (Stockage multidimensionnel pour les 3 couches).
*   **Orchestration** : Apache Airflow (Automatisation du cycle de vie des données).
*   **Transformation** : Python & Pandas (Nettoyage et conversion monétaire).
*   **Infrastructure** : Docker & Docker Compose.

### Les Couches de Données
1.  **Bronze (Raw)** : Ingestion brute des fichiers CSV (Extraction directe de T24).
2.  **Silver (Cleaned)** : Nettoyage, typage, traitement des dates et conversion des montants selon la devise.
3.  **Gold (Curated)** : Modélisation en étoile (Faits et Dimensions) optimisée pour le reporting BI.

---

## 🚀 Guide de Démarrage Rapide

### 1. Prérequis
*   **Docker Desktop** (Mac/Windows/Linux)
*   **Python 3.10+** (Pour générer les données initiales)

### 2. Lancement de l'Infrastructure
```bash
docker-compose up -d
```
Accès aux services :
*   **Airflow UI** : [http://localhost:8081](http://localhost:8081) (Admin / gkcTemuWDKnTn27N)
*   **PostgreSQL** : localhost:5433 (Admin / admin)

### 3. Simulation des Données (10 Semaines)
Pour générer les données bancaires sur 10 semaines (croissance des clients/comptes et transactions massives) :
```bash
python3 data/create_data.py
```

### 4. Exécution du Pipeline (Manuel)
Si vous ne souhaitez pas attendre le déclenchement automatique d'Airflow (Minuit UTC) :
```bash
# Ingestion des fichiers data vers Bronze
python3 jobs/ingest_to_bronze.py

# Transformation Silver
python3 jobs/job_bronze_to_silver.py

# Modélisation Gold (Dimensions puis Faits)
python3 jobs/job_silver_to_gold_dimensions.py
python3 jobs/job_silver_to_gold_facts.py
```

---

## 📊 Indicateurs Clés (KPIs)
Le modèle de données Gold permet de suivre en temps réel :
*   **Fortune Totale** : Volume global consolidé en XAF.
*   **Volume de Transactions** : Nombre d'écritures bancaires et virements uniques.
*   **Historique** : Évolution temporelle des soldes de 2023 à aujourd'hui.

---


