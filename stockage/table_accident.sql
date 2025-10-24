
DROP TABLE IF EXISTS silver.personne CASCADE;
DROP TABLE IF EXISTS silver.vehicule CASCADE;
DROP TABLE IF EXISTS silver.circonstances_accident CASCADE;
DROP TABLE IF EXISTS silver.accident CASCADE;
DROP TABLE IF EXISTS silver.commune CASCADE;
DROP TABLE IF EXISTS silver.epci CASCADE;
DROP TABLE IF EXISTS silver.departement CASCADE;
DROP TABLE IF EXISTS silver.region CASCADE;


CREATE TABLE IF NOT EXISTS silver.region (
   reg_code VARCHAR(3) PRIMARY KEY,
   reg_name VARCHAR(100) NOT NULL,
   gps VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS silver.departement (
    dep_code VARCHAR(3) PRIMARY KEY,
    dep_name VARCHAR(100) NOT NULL,
    reg_code VARCHAR(3) NOT NULL,
    insee VARCHAR(10),
    dep VARCHAR(10),
    CONSTRAINT fk_reg_code FOREIGN KEY (reg_code) REFERENCES silver.region(reg_code)
    ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS silver.epci (
   epci_code VARCHAR(10) PRIMARY KEY,
   epci_name VARCHAR(150) NOT NULL,
   dep_code VARCHAR(3) NOT NULL,
   CONSTRAINT fk_epci_departement
       FOREIGN KEY (dep_code)
       REFERENCES departement(dep_code)
       ON UPDATE CASCADE
       ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS silver.commune (
   com VARCHAR(10) PRIMARY KEY,
   com_name VARCHAR(150),
   com_arm_name VARCHAR(150),
   epci_code VARCHAR(10),
   nom_com VARCHAR(150),
   adr VARCHAR(500),
   lat FLOAT,
   long FLOAT,
   code_postal VARCHAR(10),
   coordonnees VARCHAR(100),
   com_code VARCHAR(20),
   num VARCHAR(20),
   CONSTRAINT fk_commune_epci
       FOREIGN KEY (epci_code)
       REFERENCES epci(epci_code)
       ON UPDATE CASCADE,
       ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS silver.accident (
   num_acc VARCHAR(20) PRIMARY KEY,
   num_veh VARCHAR(20),
   com VARCHAR(10) NOT NULL,
   adr VARCHAR(500),
   datetime TIMESTAMP NOT NULL,
   year_georef VARCHAR(5),
   CONSTRAINT fk_accident_commune
       FOREIGN KEY (com)
       REFERENCES commune(com)
       ON UPDATE CASCADE
       ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS circonstances_accident (
   id_circonstance INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
   num_acc VARCHAR(20) UNIQUE,
   lum VARCHAR(50),          -- ex: "Nuit sans éclairage public"
   agg VARCHAR(30),          -- ex: "En agglomération"
   int VARCHAR(10),          -- certains codes textuels possibles
   atm VARCHAR(50),          -- "Temps éblouissant", "Pluie légère"
   col VARCHAR(80),          -- "Deux véhicules – par le coté"
   pr VARCHAR(50),
   surf VARCHAR(50),
   v1 VARCHAR(50),
   v2 VARCHAR(50),
   circ VARCHAR(30),         -- "Bidirectionnelle", "A chaussées séparées"
   vosp VARCHAR(50),
   env1 VARCHAR(50),
   voie VARCHAR(50),
   larrout VARCHAR(50),
   lartpc VARCHAR(50),
   nbv VARCHAR(10),
   catr VARCHAR(50),         -- "Voie Communale", "Route Départementale"
   pr1 VARCHAR(50),
   plan VARCHAR(50),         -- "Partie rectiligne", "En courbe à gauche"
   prof VARCHAR(30),         -- "Plat", "Pente", "Sommet de côte"
   infra VARCHAR(80),        -- "Bretelle d’échangeur ou de raccordement"
   situ VARCHAR(30),         -- "Sur chaussée", "Sur trottoir"
   secu_utl VARCHAR(255),    -- "Oui,Non déterminable,Oui"
   trajet VARCHAR(255),      -- "Promenade – loisirs,Autre"
   senc VARCHAR(255),        -- "PK ou PR ou numéro d’adresse postale ..."
   obsm VARCHAR(255),        -- "Véhicule,Véhicule" etc.
   obs VARCHAR(255),         -- "Support de signalisation verticale ..."
   CONSTRAINT fk_circonstances_accident_accident
       FOREIGN KEY (num_acc)
       REFERENCES accident(num_acc)
       ON UPDATE CASCADE
       ON DELETE RESTRICT

CREATE TABLE IF NOT EXISTS silver.vehicule (
   num_acc VARCHAR(20) NOT NULL,
   num_veh VARCHAR(15) NOT NULL,
   place VARCHAR(15),
   choc VARCHAR(255),  --<-- Corrigé
   manv VARCHAR(255),  --<-- Corrigé
   catv VARCHAR(255),  --<-- Corrigé
   secu VARCHAR(255),  --<-- Corrigé
   CONSTRAINT pk_vehicule PRIMARY KEY (num_acc, num_veh),
   CONSTRAINT fk_vehicule_accident
       FOREIGN KEY (num_acc)
       REFERENCES accident(num_acc)
       ON UPDATE CASCADE
       ON DELETE RESTRICT


CREATE TABLE IF NOT EXISTS silver.personnes (
    num_acc VARCHAR(50),
    num_veh VARCHAR(50),
    an_nais INT,
    sexe VARCHAR(20),
    actp VARCHAR(100),
    locp VARCHAR(50),
    occutc VARCHAR(100),
    catu VARCHAR(50),
    etatp VARCHAR(50),
    grav VARCHAR(100),
    PRIMARY KEY (num_acc, num_veh),
    CONSTRAINT fk_personne_vehicule
       FOREIGN KEY (num_acc, num_veh)
       REFERENCES vehicule(num_acc, num_veh)
       ON UPDATE CASCADE
       ON DELETE RESTRICT
);

