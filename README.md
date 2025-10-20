# 🚦 Brief 1 — Analyse des accidents corporels de la circulation en France

## 🧭 Objectif du projet
Ce projet vise à analyser les données relatives aux **accidents corporels de la circulation en France**, afin de comprendre :
- Les causes principales des accidents
- Les zones et périodes les plus à risque
- Les profils de conducteurs les plus impliqués
- Les variables déterminantes pour la gravité des accidents

L’objectif final est de produire des visualisations claires et des modèles prédictifs permettant de mieux anticiper et prévenir les accidents.

---

## 📁 Structure du projet

```text
BRIEF_1_ACCIDENTS/
├─ analyse/            # Notebooks & scripts d’analyse exploratoire (EDA, viz)
├─ EDA/
│  └─ data/            # Jeux de données locaux (⚠️ non versionnés)
├─ ingestion/          # Import, nettoyage, typage des données
├─ modelisation/       # Entraînement, évaluation des modèles
├─ stockage/           # SQL/Parquet, IO & persistance
└─ .gitignore          # Ignore datasets/outputs lourds
