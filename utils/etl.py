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
    Crée le DataFrame 'vehicule' à partir d'un DataFrame source de la couche bronze
    (colonnes FR) ou déjà renommé (silver). Renomme via `mapping_colonnes`, nettoie,
    déduplique sur (num_acc, num_veh) et renvoie les colonnes prêtes pour l'insert.
    """

    print("=====================================================")
    print("        Fonction : Création de df_vehicule           ")
    print("=====================================================")

    # 1) Renommage vers silver (silencieux si colonnes déjà bonnes)
    df_work = df_source.rename(columns=mapping_colonnes)

    # Colonnes attendues côté silver
    required = ['num_acc', 'num_veh', 'place', 'choc', 'manv', 'catv', 'secu']
    missing = [c for c in required if c not in df_work.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes après renommage: {missing}")

    # 2) Nettoyage profond (garde la 1re valeur si 'a,b,c', trim, remplace 'nan'/'')
    print("\nNettoyage profond des colonnes...")
    for col in ['num_veh', 'place', 'choc', 'manv', 'catv', 'secu']:
        df_work[col] = (
            df_work[col]
            .astype(str)
            .str.split(',').str[0]
            .str.strip()
            .replace({'nan': None, '': None})
        )
    print("-> Colonnes nettoyées.")

    # 3) Sélection + contraintes d’unicité
    cols = ['num_acc', 'num_veh', 'place', 'choc', 'manv', 'catv', 'secu']
    df_vehicule = (
        df_work[cols]
        .dropna(subset=['num_acc', 'num_veh'])
        .drop_duplicates(subset=['num_acc', 'num_veh'])
    )

    print(f"\n✅ DataFrame 'df_vehicule' créé avec succès ({len(df_vehicule):,} lignes).")
    return df_vehicule


def creer_df_commune(df_source: pd.DataFrame) -> pd.DataFrame:
    """
    Crée le DataFrame des communes à partir du DataFrame source.
    Sélectionne les colonnes pertinentes et dédoublonne par code commune.
    """
    print("\n\n=====================================================")
    print("         Fonction : Création de df_commune           ")
    print("=====================================================")

    # --- Renommage sécurisé ---
    df_source = df_source.rename(columns=mapping_colonnes)

    # --- Colonnes attendues ---
    col_commune = [
        'com', 'com_name', 'com_arm_name', 'epci_code', 'nom_com',
        'adr', 'lat', 'long', 'code_postal', 'coordonnees', 'code_com', 'num'
    ]
    missing = [c for c in col_commune if c not in df_source.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes après renommage: {missing}")

    # --- Sélection ---
    df_commune = df_source[col_commune].copy()

    # --- Nettoyage simple ---
    df_commune['code_com'] = df_commune['code_com'].astype(str).str.strip()
    df_commune['com'] = df_commune['com'].astype(str).str.strip()

    # --- Dédoublonnage ---
    df_commune.dropna(subset=['com'], inplace=True)
    df_commune.drop_duplicates(subset=['com'], inplace=True)

    # --- Renommage final ---
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
    df = df.rename(columns=mapping_colonnes)
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
    df = df.rename(columns=mapping_colonnes)
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




def transformation_personne(df_source: pd.DataFrame) -> pd.DataFrame:
    """
    Construit le DF 'personne' à partir de df_source :
    - renomme via mapping_colonnes,
    - nettoie les champs potentiellement multi-valeurs (on garde le 1er élément),
    - sélectionne les colonnes,
    - dédoublonne et gère les NA utiles.
    """
    # 1) Renommer FR -> silver
    df = df_source.rename(columns=mapping_colonnes)

    # 2) Colonnes attendues
    cols = ['num_acc', 'num_veh', 'an_nais', 'sexe', 'actp', 'grav',
            'locp', 'catu', 'etatp', 'occutc']
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes après renommage: {missing}")

    # 3) Nettoyage "profound" (on garde le 1er élément si séparé par des virgules)
    for c in cols:
        df[c] = df[c].astype(str).str.split(',').str[0].str.strip()

    # 4) Sélection
    df_personnes = df[cols].copy()

    # 5) Clés présentes + dédoublonnage
    df_personnes.dropna(subset=['num_acc', 'num_veh', 'sexe'], inplace=True)
    df_personnes.drop_duplicates(subset=['num_acc', 'num_veh', 'sexe'], inplace=True)

    # 6) Remplissages utiles
    for c in ['an_nais', 'locp', 'etatp', 'occutc', 'actp', 'grav', 'catu']:
        df_personnes[c] = df_personnes[c].replace({'nan': None}).fillna('inconnu')

    return df_personnes


def transformation_departement(df):
    df = df.rename(columns=mapping_colonnes)
    colonnes_departement = [
        'dep_code', 'dep_name', 'reg_code', 'insee', 'dep'
    ]

    df_departement = df[colonnes_departement].copy()

    # Supprimer les doublons sur dep_code (clé unique)
    df_departement = df_departement.drop_duplicates(subset=['dep_code'], keep='first')

    return df_departement