"""Fonctions utilitaires pour l'ingestion des données d'accidents."""

import io

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# URL utilisée par défaut pour télécharger le jeu de données des accidents
url_csv = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/accidents-corporels-de-la-circulation-millesime/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"


def download_data(url):
    """Télécharge le CSV des accidents depuis l'URL fournie et le charge dans un DataFrame.

    Parameters
    ----------
    url : str
        URL du fichier CSV à récupérer.

    Returns
    -------
    pandas.DataFrame | None
        DataFrame contenant les données si le téléchargement est un succès, sinon ``None``.
    """
    print("Lancement du téléchargement avec l'URL corrigée. Veuillez patienter...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, sep=';')
        print("Téléchargement et chargement dans le DataFrame terminés avec succès !")
        print("-" * 60)

        nombre_lignes = df.shape[0]
        nombre_colonnes = df.shape[1]

        print(f"Le DataFrame final contient {nombre_lignes:,} lignes et {nombre_colonnes} colonnes.")
        print("-" * 60)
        return df
    except requests.exceptions.RequestException as e:
        print(f"Une erreur de connexion est survenue : {e}")
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")


def insert_data_to_db(df, table_name, engine):
    """Insère toutes les lignes d'un DataFrame dans une table PostgreSQL.

    Parameters
    ----------
    df : pandas.DataFrame
        Données structurées à insérer.
    table_name : str
        Nom de la table cible dans la base de données.
    engine : sqlalchemy.engine.Engine
        Moteur SQLAlchemy connecté à la base PostgreSQL.
    """
    with engine.begin() as conn:
        records = df.to_dict(orient="records")
        cols = ",".join(df.columns)
        placeholders = ",".join([f":{c}" for c in df.columns])
        query = text(f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})")
        conn.execute(query, records)


def connect_to_db(user, password, host, port, db_name):
    """Crée un moteur SQLAlchemy pour se connecter à une base PostgreSQL.

    Parameters
    ----------
    user : str
        Nom d'utilisateur PostgreSQL.
    password : str
        Mot de passe associé.
    host : str
        Hôte ou adresse IP du serveur PostgreSQL.
    port : str | int
        Port d'écoute PostgreSQL.
    db_name : str
        Nom de la base de données.

    Returns
    -------
    sqlalchemy.engine.Engine | None
        Moteur SQLAlchemy si la connexion est un succès, sinon ``None``.
    """
    DATABASE_URL = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
    try:
        engine = create_engine(DATABASE_URL)
        print("Connexion à la base de données PostgreSQL réussie !")
        return engine
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        return None


def fetch_data_from_db(table_name, engine):
    """Récupère toutes les lignes d'une table PostgreSQL dans un DataFrame.

    Parameters
    ----------
    table_name : str
        Nom de la table cible dans la base de données.
    engine : sqlalchemy.engine.Engine
        Moteur SQLAlchemy connecté à la base PostgreSQL.

    Returns
    -------
    pandas.DataFrame | None
        DataFrame contenant les données si la récupération est un succès, sinon ``None``.
    """
    try:
        with engine.connect() as conn:
            query = text(f"SELECT * FROM {table_name}")
            df = pd.read_sql_query(query, conn)
            print(f"Données récupérées avec succès depuis la table '{table_name}'.")
            return df
    except Exception as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return None