-- The following file is a Postgres SQL file.

-- Table creation
CREATE TABLE gender (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE insurers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE districts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE entries (
    id SERIAL PRIMARY KEY,
    age INTEGER NOT NULL,
    program INTEGER NOT NULL,
    insurer_id INTEGER NOT NULL REFERENCES insurers(id),
    district_id INTEGER NOT NULL REFERENCES districts(id),
    gender_id INTEGER NOT NULL REFERENCES gender(id),
    creation_date DATE NOT NULL
);

CREATE TABLE population (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    age INTEGER NOT NULL,
    population INTEGER NOT NULL,
    district_id INTEGER NOT NULL REFERENCES districts(id)
);

-- Basic enum insertions
-- Genders
INSERT INTO gender (id, name) VALUES (0, 'Male');
INSERT INTO gender (id, name) VALUES (1, 'Female');
INSERT INTO gender (id, name) VALUES (2, 'Non Binary');
INSERT INTO gender (id, name) VALUES (3, 'Other');
INSERT INTO gender (id, name) VALUES (4, 'Unknown');

-- Insurers
INSERT INTO insurers (id, name) VALUES (0, 'Unkown');
INSERT INTO insurers (id, name) VALUES(1, 'None');
INSERT INTO insurers (id, name) VALUES(2, 'Other');
INSERT INTO insurers (id, name) VALUES(3, 'Capital Salud');
INSERT INTO insurers (id, name) VALUES(4, 'Nueva EPS');
INSERT INTO insurers (id, name) VALUES(5, 'Salud Total');
INSERT INTO insurers (id, name) VALUES(6, 'Prepagada Suramericana');
INSERT INTO insurers (id, name) VALUES(7, 'Ferrocarriles');
INSERT INTO insurers (id, name) VALUES(8, 'Salud Bolivar');
INSERT INTO insurers (id, name) VALUES(9, 'Compensar');
INSERT INTO insurers (id, name) VALUES(10, 'Sanitas');
INSERT INTO insurers (id, name) VALUES(11, 'Famisanar');
INSERT INTO insurers (id, name) VALUES(12, 'AlianSalud');
INSERT INTO insurers (id, name) VALUES(13, 'Coosalud');
INSERT INTO insurers (id, name) VALUES(14, 'SOS');
INSERT INTO insurers (id, name) VALUES(15, 'Mallamas');

-- Districs
INSERT INTO districts (id, name) VALUES (1, 'Usaquen');
INSERT INTO districts (id, name) VALUES (2, 'Chapinero');
INSERT INTO districts (id, name) VALUES (3, 'Santa Fe');
INSERT INTO districts (id, name) VALUES (4, 'San Cristobal');
INSERT INTO districts (id, name) VALUES (5, 'Usme');
INSERT INTO districts (id, name) VALUES (6, 'Tunjuelito');
INSERT INTO districts (id, name) VALUES (7, 'Bosa');
INSERT INTO districts (id, name) VALUES (8, 'Kennedy');
INSERT INTO districts (id, name) VALUES (9, 'Fontibon');
INSERT INTO districts (id, name) VALUES (10, 'Engativa');
INSERT INTO districts (id, name) VALUES (11, 'Suba');
INSERT INTO districts (id, name) VALUES (12, 'Barrios Unidos');
INSERT INTO districts (id, name) VALUES (13, 'Teusaquillo');
INSERT INTO districts (id, name) VALUES (14, 'Los Martires');
INSERT INTO districts (id, name) VALUES (15, 'Antonio Narino');
INSERT INTO districts (id, name) VALUES (16, 'Puente Aranda');
INSERT INTO districts (id, name) VALUES (17, 'La Candelaria');
INSERT INTO districts (id, name) VALUES (18, 'Rafael Uribe');
INSERT INTO districts (id, name) VALUES (19, 'Ciudad Bolivar');
INSERT INTO districts (id, name) VALUES (20, 'Sumapaz');
INSERT INTO districts (id, name) VALUES (99, 'Unknown');

-- 1.1 Vista Consolidado
CREATE VIEW VISTA_CONSOLIDADO AS
SELECT 
    e.id id,
    e.age age,
    g.name gender,
    CONCAT('PROGRAM ', e.program) program,
    CONCAT(d.id, '- ', d.name) district,
    i.name insurer,
    e.creation_date date
FROM entries e
INNER JOIN gender g ON e.gender_id = g.id
INNER JOIN districts d ON e.district_id = d.id
INNER JOIN insurers i ON e.insurer_id = i.id
ORDER BY e.id;

-- 1.2 Vista Indicadores

CREATE VIEW VISTA_INDICADORES AS

-- Entries per Age-Zone
WITH epa AS (
    SELECT 
        EXTRACT(YEAR FROM creation_date) AS year,
        age,
        district_id,
        COUNT(*) count
    FROM entries
    GROUP BY year, age, district_id
),

-- Population per Age-Zone
ppa AS (
    SELECT 
        year, 
        age, 
        district_id, 
        SUM(population) count
    FROM population
    GROUP BY year, age, district_id
    ORDER BY year, age, district_id
),

-- Joined data
records AS (
    SELECT
        epa.year AS year,
        epa.age AS age,
        epa.district_id AS district_id,
        epa.count AS entries,
        ppa.count AS population
    FROM epa INNER JOIN ppa 
        ON epa.age = ppa.age 
        AND epa.district_id = ppa.district_id
        AND epa.year = ppa.year
    ORDER BY district_id, age, year
)

SELECT
    year,
    (SELECT CONCAT(id, '- ', name) FROM districts WHERE id = district_id) district,
    CASE
        WHEN age BETWEEN 0 AND 4 THEN '0-4'
        WHEN age BETWEEN 5 AND 9 THEN '5-9'
        WHEN age BETWEEN 10 AND 14 THEN '10-14'
        WHEN age BETWEEN 15 AND 19 THEN '15-19'
        WHEN age BETWEEN 20 AND 24 THEN '20-24'
        WHEN age BETWEEN 25 AND 29 THEN '25-29'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
        WHEN age BETWEEN 60 AND 64 THEN '60-59'
        WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age BETWEEN 70 AND 74 THEN '70-59'
        WHEN age BETWEEN 75 AND 79 THEN '75-79'
        WHEN age BETWEEN 80 AND 84 THEN '80-59'
        WHEN age BETWEEN 85 AND 89 THEN '85-89'
        WHEN age BETWEEN 90 AND 94 THEN '90-59'
        WHEN age BETWEEN 95 AND 99 THEN '95-99'
    END AS age_range,
    SUM(entries) entries,
    SUM(population) population,
    (SUM(entries) * 100.0 / SUM(population)) percentage
FROM records
GROUP BY year, age_range, district_id
ORDER BY year, age_range, district_id;