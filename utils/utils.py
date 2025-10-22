import pandas as pd
import requests
import io

url_csv = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/accidents-corporels-de-la-circulation-millesime/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
def download_data(url):
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

len(mapping_colonnes)