-- 1️ Nombre total d’accidents par année
WITH accidents_par_annee AS (
    SELECT EXTRACT(YEAR FROM datetime) AS annee, COUNT(*) AS nb_accidents
    FROM silver.accident
    GROUP BY annee
)
SELECT *
FROM accidents_par_annee
ORDER BY annee;


-- 2️ Les communes les plus accidentogènes
WITH accidents_communes AS (
    SELECT c.com_name, COUNT(a.num_acc) AS nb_accidents
    FROM silver.accident a
    JOIN silver.commune c ON a.com = c.com
    GROUP BY c.com_name
)
SELECT *
FROM accidents_communes
ORDER BY nb_accidents DESC
LIMIT 10;


-- 3️ Gravité moyenne des accidents par type de route
WITH accidents_personnes AS (
    SELECT c.catr,
           COUNT(p.num_acc) AS nb_personnes,
           AVG(CASE
                   WHEN p.grav = 'Indemne' THEN 0
                   WHEN p.grav = 'Blessé léger' THEN 1
                   WHEN p.grav = 'Blessé hospitalisé' THEN 2
                   WHEN p.grav = 'Tué' THEN 3
               END) AS grav_moyenne
    FROM silver.personnes p
    JOIN silver.vehicule v ON p.num_acc = v.num_acc AND p.num_veh = v.num_veh
    JOIN silver.circonstances_accident c ON v.num_acc = c.num_acc
    GROUP BY c.catr
)
SELECT *
FROM accidents_personnes
ORDER BY grav_moyenne DESC;


-- 5️ Accidents selon les conditions de luminosité
SELECT lum, COUNT(*) AS nb_accidents
FROM silver.circonstances_accident
GROUP BY lum
ORDER BY nb_accidents DESC;


-- 6️ Accidents selon le type de route
SELECT catr, COUNT(*) AS nb_accidents
FROM silver.circonstances_accident
GROUP BY catr
ORDER BY nb_accidents DESC;


-- 7️ Répartition par sexe et gravité
SELECT sexe, grav, COUNT(*) AS nb_personnes
FROM silver.personnes
GROUP BY sexe, grav
ORDER BY nb_personnes DESC;


-- 8️ Tranche d’âge la plus touchée
SELECT 
    CASE 
        WHEN an_nais >= 2005 THEN '0-18'
        WHEN an_nais >= 1990 THEN '19-34'
        WHEN an_nais >= 1970 THEN '35-54'
        ELSE '55+' 
    END AS tranche_age,
    COUNT(*) AS nb_personnes
FROM silver.personnes
GROUP BY tranche_age
ORDER BY nb_personnes DESC;


-- 9️ Types de véhicules les plus impliqués
SELECT catv, COUNT(*) AS nb_vehicules
FROM silver.vehicule
GROUP BY catv
ORDER BY nb_vehicules DESC;



-- 11 Carte de chaleur par département
SELECT d.dep_name, COUNT(a.num_acc) AS nb_accidents
FROM silver.accident a
JOIN silver.commune c ON a.com = c.com
JOIN silver.epci e ON c.epci_code = e.epci_code
JOIN silver.departement d ON e.dep_code = d.dep_code
GROUP BY d.dep_name
ORDER BY nb_accidents DESC;


-- 12 Accidents la nuit vs jour par département
SELECT d.dep_name,
       SUM(CASE WHEN ca.lum LIKE 'Nuit%' THEN 1 ELSE 0 END) AS nb_nuit,
       SUM(CASE WHEN ca.lum NOT LIKE 'Nuit%' THEN 1 ELSE 0 END) AS nb_jour
FROM silver.accident a
JOIN silver.commune c ON a.com = c.com
JOIN silver.epci e ON c.epci_code = e.epci_code
JOIN silver.departement d ON e.dep_code = d.dep_code
JOIN silver.circonstances_accident ca ON a.num_acc = ca.num_acc
GROUP BY d.dep_name
ORDER BY nb_nuit DESC;