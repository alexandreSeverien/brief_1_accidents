import pandas as pd
from utils.utils import connect_to_db, mapping_colonnes

engine = connect_to_db()


def transform_circonstances(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données d'accidents en DataFrame des circonstances (table silver.circonstances_accident).

    Étapes :
    - Renomme les colonnes selon mapping_colonnes.
    - Nettoie les valeurs multi-listes (garde la 1re).
    - Tronque les chaînes selon la taille maximale SQL.
    - Supprime les doublons et lignes sans clé 'num_acc'.
    """
    df = df.rename(columns=mapping_colonnes)

    cols = [
        'num_acc','lum','agg','int','atm','col','pr','surf','v1','v2','circ','vosp',
        'env1','voie','larrout','lartpc','nbv','catr','pr1','plan','prof','infra',
        'situ','secu_utl','trajet','senc','obsm','obs'
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes pour circonstances: {missing}")

    out = df[cols].copy()

    for c in cols:
        out[c] = out[c].astype(str).str.split(',').str[0].str.strip().replace({'nan': None, '': None})

    maxlen = {
        'lum': 15, 'agg': 4, 'int': 1, 'atm': 50, 'col': 50, 'pr': 50, 'surf': 50,
        'v1': 50, 'v2': 10, 'circ': 1, 'vosp': 50, 'env1': 50, 'voie': 50,
        'larrout': 50, 'lartpc': 50, 'nbv': 10, 'catr': 50, 'pr1': 50, 'plan': 50,
        'prof': 50, 'infra': 10, 'situ': 1, 'secu_utl': 50, 'trajet': 50,
        'senc': 50, 'obsm': 50, 'obs': 50
    }
    for c, n in maxlen.items():
        out[c] = out[c].astype(object).where(out[c].isna(), out[c].astype(str).str.slice(0, n))

    out = out.dropna(subset=['num_acc']).drop_duplicates(subset=['num_acc'])
    return out


def creer_df_vehicule(df_source: pd.DataFrame) -> pd.DataFrame:
    """
    Crée le DataFrame 'vehicule' (table silver.vehicule).

    Étapes :
    - Renomme les colonnes selon mapping_colonnes.
    - Nettoie les valeurs multi-listes (garde la 1re).
    - Supprime les valeurs manquantes et doublons sur (num_acc, num_veh).
    """
    print("=====================================================")
    print("        Fonction : Création de df_vehicule           ")
    print("=====================================================")

    df_work = df_source.rename(columns=mapping_colonnes)
    required = ['num_acc', 'num_veh', 'place', 'choc', 'manv', 'catv', 'secu']
    missing = [c for c in required if c not in df_work.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes après renommage: {missing}")

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
    Crée le DataFrame 'commune' (table silver.commune).

    Étapes :
    - Renomme les colonnes selon mapping_colonnes.
    - Convertit lat/long avec point décimal.
    - Supprime les doublons et lignes sans code commune.
    """
    print("\n\n=====================================================")
    print("         Fonction : Création de df_commune           ")
    print("=====================================================")

    df = df_source.rename(columns=mapping_colonnes)
    cols = [
        'com','com_name','com_arm_name','epci_code','nom_com','adr',
        'lat','long','code_postal','coordonnees','code_com','num'
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes pour commune: {missing}")

    out = df[cols].copy()
    out['code_com'] = out['code_com'].astype(str).str.strip()
    out['com'] = out['com'].astype(str).str.strip()

    for c in ['lat', 'long']:
        out[c] = (
            out[c]
            .astype(str)
            .str.replace(',', '.', regex=False)
            .replace({'nan': None, '': None})
        )
        out[c] = pd.to_numeric(out[c], errors='coerce')

    out = out.dropna(subset=['com']).drop_duplicates(subset=['com'])
    out = out.rename(columns={'code_com': 'com_code'})
    print(f"\n✅ DataFrame 'df_commune' créé avec succès ({len(out):,} lignes).")
    return out


def transformation_accident(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données en DataFrame 'accident' (table silver.accident).

    Étapes :
    - Renomme les colonnes via mapping_colonnes.
    - Convertit datetime au format TIMESTAMP (et supprime les NaT).
    - Nettoie les colonnes textuelles et supprime les doublons sur num_acc.
    """
    df = df.rename(columns=mapping_colonnes)
    target_cols = ["num_acc", "com", "adr", "datetime", "year_georef"]
    cols_ok = [c for c in target_cols if c in df.columns]
    if set(["num_acc", "datetime"]).difference(cols_ok):
        raise ValueError("Colonnes indispensables manquantes: 'num_acc' et/ou 'datetime'.")

    df_accident = df[cols_ok].copy()
    df_accident["datetime"] = pd.to_datetime(df_accident["datetime"], errors="coerce")
    df_accident = df_accident.dropna(subset=["datetime"])
    df_accident = df_accident.dropna(subset=["num_acc"])

    if "com" in df_accident.columns:
        df_accident = df_accident.dropna(subset=["com"])
    if "adr" in df_accident.columns:
        df_accident["adr"] = df_accident["adr"].fillna("Inconnu")
    if "year_georef" in df_accident.columns:
        df_accident["year_georef"] = (
            df_accident["year_georef"].astype(str).replace({"nan": None}).fillna("Inconnu")
        )

    df_accident = df_accident.drop_duplicates(subset=["num_acc"], keep="first")
    df_accident = df_accident.loc[:, ~df_accident.columns.duplicated()]
    return df_accident


def transform_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données en DataFrame 'region' (table silver.region).

    Étapes :
    - Renomme les colonnes selon mapping_colonnes.
    - Supprime doublons, lignes vides et remplit valeurs manquantes.
    """
    df = df.rename(columns=mapping_colonnes)
    colonnes_region = ["reg_code", "reg_name", "gps"]
    colonnes_disponibles = [col for col in colonnes_region if col in df.columns]

    if not colonnes_disponibles:
        raise ValueError("⚠️ Colonnes ['reg_code', 'reg_name', 'gps'] non trouvées.")

    df_region = df[colonnes_disponibles]
    df_region = df_region.drop_duplicates(subset="reg_code", keep="first").dropna(how="all")
    df_region = df_region.sort_values(by="reg_code", ascending=True)
    df_region = df_region.dropna(subset=["reg_code", "reg_name"])
    df_region["reg_name"] = df_region["reg_name"].fillna("Inconnu")
    df_region["gps"] = df_region["gps"].fillna("Inconnu")
    return df_region


def transformation_epci(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données en DataFrame 'epci' (table silver.epci).

    Étapes :
    - Renomme les colonnes via mapping_colonnes.
    - Supprime doublons et lignes vides.
    - Remplit epci_name et dep_code si manquants.
    """
    df = df.rename(columns=mapping_colonnes)
    colonnes_epci = ["epci_code", "epci_name", "dep_code"]
    colonnes_disponibles = [col for col in colonnes_epci if col in df.columns]

    if not colonnes_disponibles:
        raise ValueError("⚠️ Colonnes ['epci_code', 'epci_name', 'dep_code'] non trouvées.")

    df_epci = df[colonnes_disponibles]
    df_epci = df_epci.drop_duplicates(subset="epci_code", keep="first").dropna(how="all")
    df_epci = df_epci.dropna(subset=["epci_code", "epci_name"])
    df_epci["epci_name"] = df_epci["epci_name"].fillna("Inconnu")
    df_epci["dep_code"] = df_epci["dep_code"].fillna("Inconnu")
    return df_epci


def transformation_personne(df_source: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données en DataFrame 'personne' (table silver.personnes).

    Étapes :
    - Renomme les colonnes via mapping_colonnes.
    - Nettoie les multi-valeurs (garde la 1re).
    - Supprime les doublons et lignes sans clé (num_acc, num_veh, sexe).
    - Nettoie les valeurs littérales inutiles ('None', '-1').
    """
    df = df_source.rename(columns=mapping_colonnes)
    cols = ['num_acc','num_veh','an_nais','sexe','actp','grav','locp','catu','etatp','occutc']
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes pour personnes: {missing}")

    for c in cols:
        df[c] = df[c].astype(str).str.split(',').str[0].str.strip()

    out = df[cols].copy()
    out = out.replace({'nan': None, '': None})
    out = out.dropna(subset=['num_acc', 'num_veh', 'sexe']).drop_duplicates(subset=['num_acc','num_veh','sexe'])

    for c in ['an_nais','locp','etatp','occutc','actp','grav','catu']:
        out[c] = out[c].replace({'None': None, '-1': None})
    return out


def transformation_departement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données en DataFrame 'departement' (table silver.departement).

    Étapes :
    - Renomme les colonnes via mapping_colonnes.
    - Sélectionne les colonnes utiles et supprime les doublons sur dep_code.
    """
    df = df.rename(columns=mapping_colonnes)
    colonnes_departement = ['dep_code', 'dep_name', 'reg_code', 'insee', 'dep']
    df_departement = df[colonnes_departement].copy()
    df_departement = df_departement.drop_duplicates(subset=['dep_code'], keep='first')
    return df_departement