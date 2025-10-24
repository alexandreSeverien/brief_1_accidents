import pandas as pd
from utils.utils import fetch_data_from_db, connect_to_db, mapping_colonnes

engine = connect_to_db()

def transform_circonstances (df):
    df_colonnes = fetch_data_from_db(table_name='circonstances_accident', engine=engine, schema='silver')
    colonnes_circonstances = df_colonnes.columns.to_list()
    colonnes_circonstances_no_id = [c for c in colonnes_circonstances if c != "id_circonstance"]

    bronze_cols_in_order = [
        k for v in colonnes_circonstances_no_id
        for k, val in mapping_colonnes.items() if val == v
    ]
    df_circonstances = df[bronze_cols_in_order].copy()
    df_circonstances.columns = colonnes_circonstances_no_id
    return df_circonstances


def creer_df_vehicule(df_source: pd.DataFrame) -> pd.DataFrame:
    """
    Crée le DataFrame des véhicules à partir du DataFrame source (df_silver).
    Nettoie les colonnes, crée une clé primaire 'id_veh', et dédoublonne.
    """
    print("=====================================================")
    print("        Fonction : Création de df_vehicule           ")
    print("=====================================================")
    
    # --- ÉTAPE 1 : Nettoyage Profond ---
    colonnes_a_nettoyer = ['num_veh', 'place', 'choc', 'manv', 'catv', 'secu']
    print("\nNettoyage profond des colonnes...")
    for colonne in colonnes_a_nettoyer:
        df_source[colonne] = df_source[colonne].astype(str).str.split(',').str[0].str.strip()
    print("-> Colonnes nettoyées.")

    # --- ÉTAPE 2 : Sélection et Création du DataFrame ---
    col_vehicule = ['num_acc', 'num_veh', 'place', 'choc', 'manv', 'catv', 'secu']
    df_vehicule = df_source[col_vehicule].copy()
    
    # --- ÉTAPE 3 : Application des Contraintes d'Unicité ---
    df_vehicule.dropna(subset=['num_acc', 'num_veh'], inplace=True)
    #df_vehicule.drop_duplicates(subset=['id_veh'], inplace=True)
    
    # --- ÉTAPE 4 : Finalisation du DataFrame ---
    colonnes_finales = ['num_acc', 'num_veh', 'place', 'choc', 'manv', 'catv', 'secu']
    df_vehicule = df_vehicule[colonnes_finales]
    
    print(f"\n✅ DataFrame 'df_vehicule' créé avec succès ({len(df_vehicule):,} lignes).")
    return df_vehicule


def creer_df_commune(df_source: pd.DataFrame) -> pd.DataFrame:
    """
    Crée le DataFrame des communes à partir du DataFrame source (df_silver).
    Sélectionne les colonnes pertinentes et dédoublonne par code commune.
    """
    print("\n\n=====================================================")
    print("         Fonction : Création de df_commune           ")
    print("=====================================================")
    
    # --- ÉTAPE 1 : Sélection des Colonnes ---

    col_commune = [
        'com', 'com_name', 'com_arm_name', 'epci_code','nom_com', 'adr',
        'lat', 'long', 'code_postal', 'coordonnees', 'code_com', 'num'
    ]

    df_commune = df_source[col_commune].copy()

    # --- ÉTAPE 2 : Dédoublonnage et Nettoyage ---
    # On dédoublonne sur la colonne 'com' qui semble être la clé la plus fiable ici
    df_commune.dropna(subset=['com'], inplace=True)
    df_commune.drop_duplicates(subset=['com'], inplace=True)
    
    # --- ÉTAPE 3 : Renommage ---
    df_commune.rename(columns={'code_com': 'com_code'}, inplace=True)

    print(f"\n✅ DataFrame 'df_commune' créé avec succès ({len(df_commune):,} lignes).")
    return df_commune


def transformation_accident(df):
    df = df.rename(columns=mapping_colonnes)


    # --- Colonnes à conserver selon la table silver.region ---
    colonnes_accident = ["num_acc", "com", "adr","datetime","year_georef"]

    # --- Vérifier que les colonnes existent et les filtrer ---
    colonnes_disponibles = [col for col in colonnes_accident if col in df.columns]

    if not colonnes_disponibles:
        raise ValueError('⚠️ Aucune des colonnes ["num_acc", "com", "adr","datetime","year_georef"] n\'existe dans accidents_raw.')


    df_accident = df[colonnes_disponibles]
    
    # Supprimer les doublons sur epci_code (car c’est une clé unique)
    df_accident = df_accident.drop_duplicates(subset="num_acc", keep="first")
    #supprimer les lignes vide
    df_accident = df_accident.dropna(how="all")

    df_accident = df_accident.sort_values(by="num_acc", ascending=True)

    df_accident = df_accident.dropna(subset=["num_acc"])
    # Remplacer les valeurs manquantes 
    df_accident["adr"] = df_accident["adr"].fillna("Inconnu")
    df_accident["datetime"] = df_accident["datetime"].fillna("Inconnu")
    df_accident["year_georef"] = df_accident["year_georef"].fillna("Inconnu")
    df_accident["com"] = df_accident["com"].fillna("Inconnu")

    df_accident = df_accident.loc[:, ~df_accident.columns.duplicated()]
    return df_accident




def transform_region(df):
    # --- Colonnes à conserver selon la table silver.region ---
    colonnes_region = ["reg_code", "reg_name", "gps"]

    # --- Vérifier que les colonnes existent et les filtrer ---
    colonnes_disponibles = [col for col in colonnes_region if col in df.columns]

    if not colonnes_disponibles:
        raise ValueError("⚠️ Aucune des colonnes ['reg_code', 'reg_name', 'gps'] n'existe dans accidents_raw.")

    df_region = df[colonnes_disponibles]

    print("✅ Données filtrées pour la table 'silver.region' :")


    # Supprimer les doublons sur reg_code (car c’est une clé unique)
    df_region = df_region.drop_duplicates(subset="reg_code", keep="first")
    #supprimer les lignes vide
    df_region = df_region.dropna(how="all")

    df_region = df_region.sort_values(by="reg_code", ascending=True)
    print(df_region.head(10))

    print("🔍 Valeurs manquantes avant nettoyage :")
    print(df_region.isna().sum())
    # Supprimer les lignes sans reg_code ou reg_name (indispensables)
    df_region = df_region.dropna(subset=["reg_code", "reg_name"])
    # Remplacer les valeurs manquantes de gps / reg_name par 'Inconnu' (ou None selon ton choix)
    df_region["reg_name"] = df_region["reg_name"].fillna("Inconnu")
    df_region["gps"] = df_region["gps"].fillna("Inconnu")

    print("✅ Aperçu des données nettoyées :")
    print(df_region.head(10))
    print("🔍 Nombre de valeurs nulles par colonne :")
    print(df_region.isna().sum())
    
    return df_region


def transformation_epci(df):
    # --- Colonnes à conserver selon la table silver.region ---
    colonnes_epci = ["epci_code", "epci_name", "dep_code"]

    # --- Vérifier que les colonnes existent et les filtrer ---
    colonnes_disponibles = [col for col in colonnes_epci if col in df.columns]

    if not colonnes_disponibles:
        raise ValueError("⚠️ Aucune des colonnes ['epci_code', 'epci_name', 'dep_code'] n'existe dans accidents_raw.")

    df_epci = df[colonnes_disponibles]

    print("✅ Données filtrées pour la table 'silver.epci' :")


    # Supprimer les doublons sur epci_code (car c’est une clé unique)
    df_epci = df_epci.drop_duplicates(subset="epci_code", keep="first")
    #supprimer les lignes vide
    df_epci = df_epci.dropna(how="all")

    df_epci = df_epci.sort_values(by="epci_code", ascending=True)
    print(df_epci.head(10))

    print("🔍 Valeurs manquantes avant nettoyage :")
    print(df_epci.isna().sum())
    # Supprimer les lignes sans reg_code ou reg_name (indispensables)
    df_epci = df_epci.dropna(subset=["epci_code", "epci_name"])
    # Remplacer les valeurs manquantes de gps / reg_name par 'Inconnu' (ou None selon ton choix)
    df_epci["epci_name"] = df_epci["epci_name"].fillna("Inconnu")
    df_epci["dep_code"] = df_epci["dep_code"].fillna("Inconnu")

    print("✅ Aperçu des données nettoyées :")
    print(df_epci.head(10))
    print("🔍 Nombre de valeurs nulles par colonne :")
    print(df_epci.isna().sum())
    
    return df_epci




def transformation_personne(df):
    colonnes_personnes = [
    'num_acc', 'num_veh', 'an_nais', 'sexe', 'actp', 'grav',
    'locp', 'catu', 'etatp', 'occutc'
    ]
    df_personnes = df[colonnes_personnes].copy()
    df_personnes = df_personnes.drop_duplicates(subset=['num_acc', 'num_veh', 'sexe'])

    # Supprimer lignes avec clés manquantes
    df_personnes = df_personnes.dropna(subset=['num_acc', 'num_veh', 'sexe'])

    df_personnes['an_nais'] = df_personnes['an_nais'].fillna('inconnu')  # évite des suppressions massives de données tout en indiquant clairement où les données sont manquantes.
    df_personnes['locp'] = df_personnes['locp'].fillna('inconnu')
    df_personnes['etatp'] = df_personnes['etatp'].fillna('inconnu')
    df_personnes['occutc'] = df_personnes['occutc'].fillna('inconnu')
    df_personnes['actp'] = df_personnes['actp'].fillna('inconnu')
    return df_personnes


def transformation_departement(df):
    colonnes_departement = [
        'dep_code', 'dep_name', 'reg_code', 'insee', 'dep'
    ]
    df_departement = df[colonnes_departement].copy()
    df_departement = df_departement.drop_duplicates()
    return df_departement