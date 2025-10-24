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
    dep VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS silver.epci (
   epci_code VARCHAR(10) PRIMARY KEY,
   epci_name VARCHAR(150) NOT NULL,
   dep_code VARCHAR(3) NOT NULL
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
   num VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS silver.accident (
   num_acc VARCHAR(20) PRIMARY KEY,
   num_veh VARCHAR(20),
   com VARCHAR(10) NOT NULL,
   adr VARCHAR(500),
   datetime TIMESTAMP NOT NULL,
   year_georef VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS silver.circonstances_accident (
   num_acc VARCHAR(20) PRIMARY KEY,
   lum VARCHAR(15),
   agg VARCHAR(4),
   int VARCHAR(1),
   atm VARCHAR(50),
   col VARCHAR(50),
   pr VARCHAR(50),
   surf VARCHAR(50),
   v1 VARCHAR(50),
   v2 VARCHAR(10),
   circ VARCHAR(1),
   vosp VARCHAR(50),
   env1 VARCHAR(50),
   voie VARCHAR(50),
   larrout VARCHAR(50),
   lartpc VARCHAR(50),
   nbv VARCHAR(10),
   catr VARCHAR(50),
   pr1 VARCHAR(50),
   plan VARCHAR(50),
   prof VARCHAR(50),
   infra VARCHAR(10),
   situ VARCHAR(1),
   secu_utl VARCHAR(50),
   trajet VARCHAR(50),
   senc VARCHAR(50),
   obsm VARCHAR(50),
   obs VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS silver.vehicule (
   num_acc VARCHAR(20) NOT NULL,
   num_veh VARCHAR(15) NOT NULL,
   place VARCHAR(15),
   choc VARCHAR(255),
   manv VARCHAR(255),
   catv VARCHAR(255),
   secu VARCHAR(255),
   CONSTRAINT pk_vehicule PRIMARY KEY (num_acc, num_veh)
);

CREATE TABLE IF NOT EXISTS silver.personnes (
    num_acc VARCHAR(50),
    num_veh VARCHAR(50),
    an_nais VARCHAR(50),
    sexe VARCHAR(20),
    actp VARCHAR(100),
    locp VARCHAR(50),
    occutc VARCHAR(100),
    catu VARCHAR(50),
    etatp VARCHAR(50),
    grav VARCHAR(100),
    PRIMARY KEY (num_acc, num_veh, sexe)
);