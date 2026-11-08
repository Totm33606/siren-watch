-- =============================================================================
-- SirenWatch : exploration du data lake avec DuckDB (extension httpfs)
--
-- Lancer :  make explore
--   (= uv run --env-file .env python notebooks/run_sql.py notebooks/explore.sql)
-- ou, avec la CLI DuckDB et les variables AWS_* exportées :
--   duckdb -c ".read notebooks/explore.sql"
--
-- Aucun identifiant ici : le secret `credential_chain` (chaîne 'env') lit
-- AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY dans l'environnement, comme boto3.
-- =============================================================================

INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;

-- MinIO vu depuis le poste : port publié localhost:9000, adressage "path", sans TLS.
-- Pour S3 réel : retirer ENDPOINT / URL_STYLE / USE_SSL et mettre la bonne REGION.
CREATE OR REPLACE SECRET sirenwatch_s3 (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- Une vue sur toutes les partitions journalières. hive_partitioning expose le
-- dossier `date=YYYY-MM-DD` comme une colonne `date` : un filtre sur `date` ne lit
-- que les fichiers utiles (partition pruning).
CREATE OR REPLACE VIEW annonces AS
SELECT *
FROM read_parquet(
    's3://sirenwatch/clean/bodacc/date=*/annonces.parquet',
    hive_partitioning = true,
    hive_types = {'date': DATE}
);


-- -----------------------------------------------------------------------------
-- 1. Contrôles du data lake
-- -----------------------------------------------------------------------------

-- 1a. Couverture : partitions chargées (jours vides compris), jours avec annonces, période.
--     Un jour sans parution a une partition, mais sans ligne : on compte les fichiers.
SELECT
    (SELECT count(*) FROM glob('s3://sirenwatch/clean/bodacc/date=*/annonces.parquet'))
                          AS partitions,
    count(DISTINCT date)  AS jours_avec_annonces,
    count(*)              AS annonces,
    min(date)             AS premiere_parution,
    max(date)             AS derniere_parution
FROM annonces;

-- 1b. Doublons sur id_annonce, toutes partitions confondues (attendu : 0 ligne).
--     C'est le contrôle du critère de fin de V1 « backfill relancé sans doublon ».
SELECT id_annonce, count(*) AS occurrences
FROM annonces
GROUP BY id_annonce
HAVING count(*) > 1;

-- 1c. Qualité par mois : taux de SIREN manquants, natures non classées.
SELECT
    strftime(date_parution, '%Y-%m')                                   AS mois,
    count(*)                                                           AS annonces,
    round(100 * avg(CASE WHEN siren IS NULL THEN 1 ELSE 0 END), 1)     AS pct_siren_manquant,
    round(100 * avg(CASE WHEN evenement = 'autre' THEN 1 ELSE 0 END), 1) AS pct_non_classe
FROM annonces
GROUP BY ALL
ORDER BY mois;


-- -----------------------------------------------------------------------------
-- 2. Procédures par type, par département et par mois (requête demandée)
--
-- Une « procédure » = une annonce de jugement d'ouverture (evenement = 'ouverture').
-- Les autres annonces (créances, clôture…) sont des étapes d'une procédure déjà
-- comptée. Les rectificatifs sont exclus (ils republient une annonce existante).
-- Les conversions (redressement -> liquidation) ne sont pas comptées comme de
-- nouvelles procédures ; voir la requête 3 pour les suivre à part.
-- -----------------------------------------------------------------------------

-- 2a. Forme longue : une ligne par (mois, département, type).
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

-- 2b. Même résultat en tableau croisé : un type par colonne, plus lisible.
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
-- 3. Vue nationale : étapes des procédures par mois et par type
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
-- 4. Départements les plus touchés sur la période (ouvertures, tous types)
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
-- 5. Niveau entreprise : parcours des SIREN sur la période
--
-- `sirens` contient tous les SIREN d'une annonce (plusieurs débiteurs possibles) :
-- unnest() donne une ligne par (annonce, entreprise), sans perte.
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
