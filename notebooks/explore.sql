-- =============================================================================
-- SirenWatch: exploring the data lake with DuckDB (httpfs extension)
--
-- Run:  make explore
--   (= uv run --env-file .env python notebooks/run_sql.py notebooks/explore.sql)
-- or, with the DuckDB CLI and the AWS_* variables exported:
--   duckdb -c ".read notebooks/explore.sql"
--
-- No credentials here: the `credential_chain` secret ('env' chain) reads
-- AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the environment, like boto3.
-- =============================================================================

INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;

-- MinIO seen from this machine: published port localhost:9000, "path" addressing, no TLS.
-- For real S3: remove ENDPOINT / URL_STYLE / USE_SSL and set the right REGION.
CREATE OR REPLACE SECRET sirenwatch_s3 (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- A view over all the daily partitions. hive_partitioning exposes the
-- `date=YYYY-MM-DD` folder as a `date` column: a filter on `date` only reads the
-- files it needs (partition pruning).
CREATE OR REPLACE VIEW annonces AS
SELECT *
FROM read_parquet(
    's3://sirenwatch/clean/bodacc/date=*/annonces.parquet',
    hive_partitioning = true,
    hive_types = {'date': DATE}
);


-- -----------------------------------------------------------------------------
-- 1. Data lake checks
-- -----------------------------------------------------------------------------

-- 1a. Coverage: partitions loaded (empty days included), days with notices, period.
--     A day without publication has a partition but no row: files are counted.
SELECT
    (SELECT count(*) FROM glob('s3://sirenwatch/clean/bodacc/date=*/annonces.parquet'))
                          AS partitions,
    count(DISTINCT date)  AS jours_avec_annonces,
    count(*)              AS annonces,
    min(date)             AS premiere_parution,
    max(date)             AS derniere_parution
FROM annonces;

-- 1b. Duplicate id_annonce across all partitions (expected: 0 rows).
--     This is the check behind "a re-run backfill creates no duplicates".
SELECT id_annonce, count(*) AS occurrences
FROM annonces
GROUP BY id_annonce
HAVING count(*) > 1;

-- 1c. Quality by month: missing SIREN rate, unclassified labels.
SELECT
    strftime(date_parution, '%Y-%m')                                   AS mois,
    count(*)                                                           AS annonces,
    round(100 * avg(CASE WHEN siren IS NULL THEN 1 ELSE 0 END), 1)     AS pct_siren_manquant,
    round(100 * avg(CASE WHEN evenement = 'autre' THEN 1 ELSE 0 END), 1) AS pct_non_classe
FROM annonces
GROUP BY ALL
ORDER BY mois;


-- -----------------------------------------------------------------------------
-- 2. Procedures by type, department and month (the main query)
--
-- A "procedure" = a notice of an opening judgment (evenement = 'ouverture').
-- The other notices (claims, closing…) are stages of an already counted procedure.
-- Corrections are excluded (they republish an existing notice).
-- Conversions (receivership -> liquidation) are not counted as new procedures;
-- see query 3 to track them separately.
-- -----------------------------------------------------------------------------

-- 2a. Long format: one row per (month, department, type).
SELECT
    strftime(date_parution, '%Y-%m') AS mois,
    departement,
    type_procedure,
    count(*)                         AS procedures_ouvertes
FROM annonces
WHERE evenement = 'ouverture'
  AND type_avis = 'annonce'
GROUP BY ALL
ORDER BY mois, departement, type_procedure;

-- 2b. Same result as a crosstab: one column per type, easier to read.
PIVOT (
    SELECT strftime(date_parution, '%Y-%m') AS mois, departement, type_procedure
    FROM annonces
    WHERE evenement = 'ouverture' AND type_avis = 'annonce'
)
ON type_procedure IN ('sauvegarde', 'redressement', 'liquidation')
USING count(*)
GROUP BY mois, departement
ORDER BY mois, departement;


-- -----------------------------------------------------------------------------
-- 3. National view: procedure stages by month and type
-- -----------------------------------------------------------------------------

SELECT
    strftime(date_parution, '%Y-%m')                     AS mois,
    type_procedure,
    count(*) FILTER (WHERE evenement = 'ouverture')      AS ouvertures,
    count(*) FILTER (WHERE evenement = 'conversion')     AS conversions,
    count(*) FILTER (WHERE evenement = 'plan')           AS plans,
    count(*) FILTER (WHERE evenement = 'cloture')        AS clotures
FROM annonces
WHERE type_avis = 'annonce'
  AND type_procedure <> 'indetermine'
GROUP BY ALL
ORDER BY mois, type_procedure;


-- -----------------------------------------------------------------------------
-- 4. Most affected departments over the period (openings, all types)
-- -----------------------------------------------------------------------------

SELECT
    departement,
    count(*)                                                    AS procedures_ouvertes,
    count(*) FILTER (WHERE type_procedure = 'liquidation')      AS dont_liquidations,
    round(100 * count(*) FILTER (WHERE type_procedure = 'liquidation') / count(*), 1)
                                                                AS pct_liquidation
FROM annonces
WHERE evenement = 'ouverture' AND type_avis = 'annonce'
GROUP BY departement
ORDER BY procedures_ouvertes DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 5. Company level: SIREN histories over the period
--
-- `sirens` holds every SIREN of a notice (possibly several debtors):
-- unnest() gives one row per (notice, company), without loss.
-- -----------------------------------------------------------------------------

WITH evenements AS (
    SELECT unnest(sirens) AS siren, date_parution, type_procedure, evenement
    FROM annonces
    WHERE type_avis = 'annonce'
)
SELECT
    siren,
    count(*) AS annonces,
    string_agg(strftime(date_parution, '%d/%m') || ' ' || evenement || ' ' || type_procedure,
               ' → ' ORDER BY date_parution)  AS parcours
FROM evenements
GROUP BY siren
HAVING count(DISTINCT evenement) >= 3
ORDER BY annonces DESC, siren
LIMIT 10;
