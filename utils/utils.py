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


from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import sys
sys.path.append("..")
  # charge le .env à la racine du projet

def connect_to_db():
    """Établit une connexion à une base de données PostgreSQL à partir du fichier .env.

    Cette fonction charge les variables d'environnement définies dans un fichier `.env`
    (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME), construit l'URL de connexion,
    puis crée un moteur SQLAlchemy permettant d’interagir avec la base PostgreSQL.

    Returns:
        sqlalchemy.engine.Engine | None: 
            Moteur SQLAlchemy si la connexion est établie avec succès, 
            sinon None en cas d’erreur (un message d’erreur est affiché).

    Example:
        >>> engine = connect_to_db()
        ✅ Connexion à la base PostgreSQL réussie !
        >>> engine.execute("SELECT 1")
        <sqlalchemy.engine.cursor.CursorResult object ...>
    """
    load_dotenv()
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    try:
        engine = create_engine(DATABASE_URL)
        print("✅ Connexion à la base PostgreSQL réussie !")
        return engine
    except Exception as e:
        print(f"❌ Erreur de connexion : {e}")
        return None


def fetch_data_from_db(table_name, engine, schema=None):
    """Récupère une table PostgreSQL dans un DataFrame Pandas.

    Cette fonction construit automatiquement le nom pleinement qualifié de la
    table (schema.table) si `schema` est fourni, cite les identifiants pour
    éviter les problèmes de casse/espaces, exécute un `SELECT *` et renvoie le
    résultat sous forme de DataFrame.

    Args:
        table_name (str): Nom de la table à lire (ex. "accidents_raw").
        engine (sqlalchemy.engine.Engine): Moteur SQLAlchemy connecté à la base.
        schema (str | None): Schéma de la table (ex. "bronze"). Si None, le
            `search_path` courant est utilisé.

    Returns:
        pandas.DataFrame | None: Le contenu de la table si la requête réussit,
        sinon None (un message d'erreur est affiché).

    Example:
        >>> engine = connect_to_db()
        >>> df = fetch_data_from_db("accidents_raw", engine, schema="bronze")
        >>> df.shape
        (475911, 69)
    """
    try:
        full = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        q = text(f"SELECT * FROM {full}")
        with engine.connect() as conn:
            df = pd.read_sql_query(q, conn)
        print(f"OK: {full}")
        return df
    except Exception as e:
        print(f"Erreur SELECT: {e}")
        return None

# =====================================================================
#   Dictionnaire de MAPPING FINAL et COMPLET (Bronze -> Silver) - v3
#   Objectif : Mapper les 69 colonnes de la source sans perte.
# =====================================================================
mapping_colonnes = {
    # --- Caractéristiques de l'accident (12) ---
    "Identifiant de l'accident": "num_acc",
    "Date et heure": "datetime",
    "Année": "an",
    "Mois": "mois",
    "Jour": "jour",
    "Heure minute": "hrmn",
    "Lumière": "lum",
    "Localisation": "agg",
    "Intersection": "int",
    "Conditions atmosphériques": "atm",
    "Collision": "col",
    "Situation": "situ",

    # --- Lieu (30) ---
    "Département": "dep",
    "Code commune": "com",
    "Code Insee": "insee",
    "Adresse": "adr",
    "Latitude": "lat",
    "Longitude": "long",
    "Code Postal": "code_postal",
    "Numéro": "num",
    "Coordonnées": "coordonnees",
    "PR": "pr",
    "Surface": "surf",
    "V1": "v1",
    "Circulation": "circ",
    "Voie réservée": "vosp",
    "Env1": "env1",
    "Voie": "voie",
    "Largeur de la chaussée": "larrout",
    "V2": "v2",
    "Largeur terre plein central": "lartpc",
    "Nombre de voies": "nbv",
    "Catégorie route": "catr",
    "PR1": "pr1",
    "Plan": "plan",
    "Profil": "prof",
    "Infrastructure": "infra",
    "Gps": "gps",
    "date": "date", 
    "year_georef": "year_georef",
    "Commune": "nom_com", 
    "Nom Officiel Commune": "com_name",

    # --- Usager (11) ---
    "Année de naissance": "an_nais",
    "Sexe": "sexe",
    "Action piéton": "actp",
    "Gravité": "grav",
    "Existence équipement de sécurité": "secu",
    "Utilisation équipement de sécurité": "secu_utl",
    "Localisation du piéton": "locp",
    "Place": "place",
    "Catégorie d'usager": "catu",
    "Piéton seul ou non": "etatp",
    "Motif trajet": "trajet",

    # --- Véhicule (8) ---
    "Identifiant véhicule": "num_veh",
    "Point de choc": "choc",
    "Manœuvre": "manv",
    "Sens": "senc",
    "Obstacle mobile heurté": "obsm",
    "Obstacle fixe heurté": "obs",
    "Catégorie véhicule": "catv",
    "Nombre d'occupants": "occutc",

    # --- Noms et Codes Officiels (8) ---
    "Code Officiel Département": "dep_code", 
    "Nom Officiel Département": "dep_name",
    "Code Officiel EPCI": "epci_code",       
    "Nom Officiel EPCI": "epci_name",
    "Code Officiel Région": "reg_code",       
    "Nom Officiel Région": "reg_name",
    "Nom Officiel Commune / Arrondissement Municipal": "com_arm_name",
    "Code Officiel Commune": "code_com"         
}

