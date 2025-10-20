# 🚦 Brief 1 — Analyse des accidents corporels de la circulation en France

## 🧭 Objectif du projet
Ce projet vise à analyser les données relatives aux **accidents corporels de la circulation en France**, afin de comprendre :
- Les causes principales des accidents
- Les zones et périodes les plus à risque
- Les profils de conducteurs les plus impliqués
- Les variables déterminantes pour la gravité des accidents

L’objectif final est de produire des visualisations claires et des modèles prédictifs permettant de mieux anticiper et prévenir les accidents.

---

## 🗂️ Structure du projet
BRIEF_1_ACCIDENTS/
│
├── analyse/
│   └── Notebooks et scripts d’analyse exploratoire (EDA avancée, visualisations, indicateurs statistiques)
│
├── EDA/
│   └── data/
│       └── Datasets bruts et nettoyés.
│          ⚠️ Les fichiers lourds (>100 MB) ne sont pas versionnés (voir .gitignore).
│
├── ingestion/
│   └── Scripts d’importation et de préparation des données :
│       - lecture des fichiers CSV
│       - nettoyage, typage, suppression des doublons
│       - sauvegarde au format exploitable
│
├── modelisation/
│   └── Notebooks et scripts pour la modélisation :
│       - régression, classification, ou clustering
│       - sélection et évaluation des modèles
│       - visualisation des performances
│
├── stockage/
│   └── Scripts ou requêtes liés au stockage :
│       - export vers bases SQL ou fichiers parquet
│       - automatisation des flux de données
│
└── .gitignore
└── Liste des fichiers et dossiers ignorés (datasets volumineux, outputs temporaires, notebooks checkpoints, etc.)
