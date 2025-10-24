# 🚦 Brief 1 — Analyse des accidents corporels de la circulation en France

## 🧭 Objectif du projet
Ce projet vise à analyser les données relatives aux **accidents corporels de la circulation en France**, afin de comprendre :
- Les causes principales des accidents
- Les zones et périodes les plus à risque
- Les profils de conducteurs les plus impliqués
- Les variables déterminantes pour la gravité des accidents

L’objectif final est de produire des visualisations claires et des modèles prédictifs permettant de mieux anticiper et prévenir les accidents.

---

## 📊 Parcours de traitement
1. **Ingestion & préparation**  
   - Connexion à la base de données (PostgreSQL via `docker-compose.yml`).  
   - Extraction des données brutes (`bronze.accidents_raw`) et normalisation des colonnes (`ingestion/transformation_accident.ipynb`).  
   - Fonctions de pipeline réutilisables centralisées dans `utils/etl.py` et `utils/utils.py`.
2. **Stockage**  
   - Création des tables cibles (schéma *silver*) avec les scripts SQL de `stockage/`.  
   - Insertion des données nettoyées via les notebooks ou les fonctions d’ETL.
3. **Analyse exploratoire**  
   - Notebooks d’EDA dans `analyse/` et ébauches de requêtes analytiques dans `analyse/analyses.sql`.  
   - Visualisations complémentaires conservées dans le dossier `EDA/`.
4. **Modélisation**  
   - Prototypes de modèles et expérimentations dans `modelisation/` (fait sur google drive).

---

## 🗂️ Arborescence du dépôt
```text
BRIEF_1_ACCIDENTS/
├─ analyse/                 # Requêtes SQL et notebooks d’analyse exploratoire
├─ EDA/                     # Visualisations et données intermédiaires
├─ ingestion/               # Notebooks de pipeline et transformations
├─ modelisation/            # Expérimentations de modèles prédictifs
├─ stockage/                # Scripts SQL pour les tables bronze/silver
├─ utils/                   # Connexion base, helpers ETL, mapping de colonnes
├─ docker-compose.yml       # Services PostgreSQL & dépendances
├─ requirements.txt         # Dépendances Python
└─ README.md
```

---

## 🛠️ Stack technique
- **Python 3.12**, `pandas`, `sqlalchemy`, `psycopg2` pour l’ingestion et le nettoyage.
- **Jupyter Notebooks** pour l’exploration et la documentation des traitements.
- **PostgreSQL** orchestré via Docker pour le stockage *bronze/silver*.
- Visualisation et analyses statistiques avec `matplotlib`, `seaborn`, `scikit-learn`, etc.


## 📌 Notes complémentaires
- Adapter les paramètres de connexion (host, port, utilisateurs) si la configuration Docker diffère.  
- Les notebooks contiennent des cellules de contrôle pour vérifier la qualité des données (doublons, valeurs manquantes, incohérences).

Bonne exploration !
