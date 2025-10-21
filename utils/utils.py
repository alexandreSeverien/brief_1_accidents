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


