# BODACC source: decisions

Exploration done on 2026-11-07 with real API calls, over the period 2026-10-24 to
2026-11-06 (8,277 insolvency notices). This document records what `extract` and
`transform` do, and why.

## Platform and dataset

| Item | Value |
|---|---|
| Platform | DILA's Opendatasoft portal, **Explore v2.1** API |
| Base URL | `https://bodacc-datadila.opendatasoft.com/api/explore/v2.1` |
| Dataset | `annonces-commerciales` (about 51.3 million notices, all kinds) |
| Authentication | none |
| Observed quota | `X-RateLimit-Limit: 5000000` calls per day, reset at 00:00 UTC |
| Documentation | API: <https://help.opendatasoft.com/apis/ods-explore-v2/> · dataset: <https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/> |
| License | [Licence Ouverte / Open Licence 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) |

The other datasets of the portal (`ac_mapping_famille_avis`, `ac_mapping_type_avis`,
`liste-departements`…) are label tables. They are not used in V1.

## Filter

The filter is applied **server-side**, with ODSQL syntax in the `where` parameter:

```
familleavis = "collective" AND dateparution = date'YYYY-MM-DD'
```

- `familleavis = "collective"` matches the label "Procédures collectives"
  (insolvency proceedings), according to the `ac_mapping_famille_avis` table.
- Conciliation (`conciliation`) and professional recovery
  (`retablissement_professionnel`) are **separate families**. They are excluded from V1,
  whose scope is insolvency proceedings only.
- `dateparution` has type `date`; it is filtered with the `date'…'` literal.
- Insolvency proceedings are all published in **BODACC A** (`publicationavis = "A"`).
  The filter does not need to say so.

## Endpoint: pagination, with fallback to the export

Two endpoints return the records, **in the same format** (checked: the objects in
`results` are identical, key by key, to those of the export):

| Endpoint | Limits (checked through the `InvalidRESTParameterError` error) |
|---|---|
| `GET /catalog/datasets/annonces-commerciales/records` | `limit` ≤ 100 and `offset + limit` ≤ 10,000; returns `{"total_count", "results"}` |
| `GET /catalog/datasets/annonces-commerciales/exports/json` | no pagination and no cap: one JSON array in a single response |

**Decision: the paginated `records` endpoint with pages of 100, falling back to
`exports/json` if `total_count` exceeds 10,000.**

- Daily volume stays below the cap: the all-time maximum is 3,007 notices
  (2008-06-17), the recent maximum 2,661 (2025-12-28). A busy day is about 20 to 30
  pages, and the fallback is only a safety net.
- `order_by=id` makes the order stable across pages, and the `raw/` file
  deterministic.
- **Consistency check:** the number of notices retrieved must equal the `total_count`
  of the first page, with no duplicate `id`. Otherwise (dataset modified during
  pagination), the extraction fails, and an Airflow retry of the task redoes it from
  scratch.
- A day without notices returns `total_count = 0` and `results = []` with HTTP 200:
  `[]` is written.

Typical request (page 2):

```
GET {base}/catalog/datasets/annonces-commerciales/records
    ?where=familleavis="collective" AND dateparution=date'2026-11-05'
    &order_by=id&limit=100&offset=100
```

**Retries:** up to 5 attempts with exponential backoff (2, 4, 8 then 16 s, capped at
60 s) on network errors, timeouts, 429 and 5xx. For a 429, the delay in the
`Retry-After` header is honoured when present. No retry on other 4xx: a malformed
request will not succeed on the second try.

**Content of `raw/`:** the concatenation of the `results` of all pages, as a UTF-8
JSON array, in API order. This is exactly the format of the export and of the fixture.
The `total_count` envelope is not kept. The records themselves are never modified.

## Availability and schedule

- The metadata shows `data_processed = 2026-11-06T06:17Z`: the publication of day **D
  is loaded the same morning** (around 06:00–07:00 UTC). At exploration time
  (Saturday 11/07, during the day), the latest available date was 2026-11-06.
- **Publication days are irregular.** Some weekdays have no publication (Tuesday
  2026-11-03, Monday 2026-10-26), and some notices are published on Saturday or Sunday
  (2026-10-31, 2026-10-25, 2025-12-28). The "zero rows on a weekday" check in
  `validate` will therefore warn on genuine days without publication, which is why it
  must not fail the run.
- `parution` (e.g. `20260213`) is the **BODACC issue number** (year + sequence number),
  **not a date**. The date is `dateparution`.
- **DAG schedule:** every day, weekends included, at **10:00 UTC**, with
  `CronTriggerTimetable`. Checked in Airflow 3.4: `create_cron_data_intervals = False`,
  so a run's logical date is its trigger time. The run of day D at 10:00 therefore
  processes the publication of day D, loaded around 06:00–07:00 UTC, which leaves about
  3 hours of margin. If the DILA ever loaded later than 10:00, the run would find 0
  notices (a `validate` warning, not a failure). The date can then be replayed with
  `make backfill START=D END=D REPROCESS=completed`.

## Structure of a notice (useful fields)

The `listepersonnes` and `jugement` fields are **strings containing JSON**. They must
be decoded with `json.loads`.

| API field | Type | Example | Note |
|---|---|---|---|
| `id` | text | `A202602133625` | unique in the dataset (checked: 8,277 distinct ids for 8,277 notices) |
| `dateparution` | date | `2026-11-05` | |
| `typeavis` | text | `annonce` / `rectificatif` | 13 corrections out of 510 on 2026-11-05 (2.5%), with `parutionavisprecedent` filled |
| `commercant` | text | `AT CONSEIL` or `NOM, Prénom` | company name, or name of a natural person |
| `registre` | list of text | `["814811493", "814 811 493"]` | each SIREN appears **twice** (with and without spaces); 4 elements for 2 persons; `null` without a SIREN |
| `listepersonnes` | JSON (text) | `{"personne": {...}}` | `personne` is an object, or a **list** when there are several persons (34 cases out of 8,277) |
| `jugement` | JSON (text) | `{"famille": …, "nature": …, "date": …, "complementJugement": …}` | always present; `date` missing in 2 cases |
| `tribunal` | text | `Greffe du Tribunal de Commerce de Sedan` | 225 distinct values; never empty |
| `numerodepartement` | text | `08`, `2A`, `974` | **text**, not integer (Corsica, overseas territories) |
| `departement_nom_officiel` | text | `Ardennes` | |
| `ville`, `cp` | text | `Sedan`, `08200` | empty in 7 cases out of 8,277 |
| `url_complete` | text | link to bodacc.fr | |

### Where to find the SIREN

The SIREN is the 9-digit identifier of a French company, with a Luhn check digit.

1. **`registre`**: present in 93.4% of notices. Values with 9 digits and no spaces are
   kept. Over two weeks, `registre` always matches exactly the
   `numeroImmatriculation.numeroIdentification` values of `listepersonnes`, and **all
   7,616 distinct SIRENs pass the Luhn check**.
2. **`listepersonnes[].inscriptionRM.numeroIdentificationRM`**: craftspeople registered
   in the trades register (*répertoire des métiers*) but not in the trade and companies
   register (RCS). `registre` is then empty, but this 9-digit number is indeed a SIREN
   (the 130 found all pass the Luhn check). It recovers **about 24% of the missing
   SIRENs**.
3. Without either (`nonInscrit = "N"`, 416 cases out of 8,277, i.e. 5%), the SIREN is
   genuinely absent from the source.

## Mapping to the `clean/` schema

**Grain: one row per notice** (decision of 2026-11-07), which guarantees that
`id_annonce` is unique. A notice can concern several debtors (0.4% of cases, for
example a sole trader and their property company). No SIREN is lost: the `sirens`
column holds them all. In V2, a "one row per company" view will be obtained with
`unnest(sirens)`.

| `clean/` column | Source | Rule |
|---|---|---|
| `id_annonce` | `id` | as is |
| `date_parution` | `dateparution` | `date` |
| `siren_brut` | first value of `registre`, otherwise `numeroIdentificationRM` | text as published; `null` if absent |
| `siren` | `siren_brut` without spaces | filled only if it has 9 digits **and** passes the Luhn check; otherwise `null` |
| `sirens` | `registre`, then `numeroIdentificationRM` of each person | list of valid SIRENs, deduplicated, in source order; empty list if none. Both sources are combined, rather than using the trades register only as a fallback, so as not to lose the craftsperson of a notice that also mentions an RCS-registered company |
| `nb_personnes` | `listepersonnes.personne` | number of persons concerned |
| `raison_sociale` | `commercant` | as is |
| `type_procedure` | derived from `jugement.nature` | `sauvegarde`, `redressement`, `liquidation` or `indetermine` |
| `evenement` | derived from `jugement.nature` | `ouverture`, `conversion`, `plan`, `cloture`, `creances`, `sanction` or `autre` |
| `nature_jugement` | `jugement.nature` | source label as is |
| `date_jugement` | `jugement.date` | `date` |
| `type_avis` | `typeavis` | to filter out corrections |
| `tribunal` | `tribunal` | as is |
| `departement` | `numerodepartement` | text |
| `ville` | `ville` | may be `null` |
| `url` | `url_complete` | as is |
| `date_extraction` | provided by the pipeline | timestamp of the run, passed as a parameter, never the system clock |

### Grouping court-decision labels

The two derived columns are computed in `transform` with keyword rules on
`jugement.nature`, and tested against the fixtures. An unknown label falls into
`indetermine` / `autre`, and `validate` logs the list of unclassified labels: a new
label published by the DILA shows up instead of being silently misfiled.

| `jugement.nature` (examples) | `type_procedure` | `evenement` |
|---|---|---|
| Jugement d'ouverture de liquidation judiciaire | liquidation | ouverture |
| Jugement de clôture pour insuffisance d'actif | liquidation | cloture |
| Liste des créances nées après le jugement d'ouverture d'une procédure de liquidation judiciaire | liquidation | creances |
| Jugement de conversion en liquidation judiciaire | liquidation | conversion |
| Jugement d'ouverture d'une procédure de redressement judiciaire | redressement | ouverture |
| Jugement de plan de redressement | redressement | plan |
| Jugement d'ouverture d'une procédure de sauvegarde | sauvegarde | ouverture |
| Dépôt de l'état des créances | indetermine | creances |
| Jugement arrêtant un plan de cession | indetermine | plan |
| Jugement d'interdiction de gérer | indetermine | sanction |
| Autre jugement et ordonnance (about 10%) | indetermine | autre |

- The rules were checked against the 45 labels observed over two weeks (8,277
  notices). Special cases:
  - "Rétractation de jugement d'ouverture…" cancels an opening: classified `autre`.
  - "Jugement d'extension de liquidation judiciaire / de redressement judiciaire"
    (proceedings extended to another debtor): classified `autre` for now. It could be
    counted as an `ouverture` for the debtor concerned.
  - Closings "pour insuffisance d'actif" and "pour extinction du passif" are
    liquidation closings: `liquidation`, even though the word does not appear.
- To count *opened* procedures by type, filter on `evenement = 'ouverture'`. Whether to
  include `conversion` is still open: does a receivership converted into a liquidation
  count as a new liquidation?
- "Autre jugement et ordonnance" notices could often be classified from the free text
  `complementJugement`. Free-text analysis being fragile, it is out of V1's scope.

Distribution of labels on one day (510 notices on 2026-11-05): opening of judicial
liquidation 149, closing for insufficient assets 100, filing of the statement of claims
58, other judgments and orders 53, opening of receivership 42, list of claims arising
after the opening of a liquidation 42, conversion into liquidation 20, others fewer
than 10 each.

## Fixtures (`tests/fixtures/`)

| File | Content |
|---|---|
| `bodacc_collective_sample.json` | 12 real notices, in the exact format of the `exports/json` response (JSON array), chosen to cover the cases listed below |
| `bodacc_collective_empty.json` | `[]`, the response for a day without notices |

Cases covered: company in liquidation, in receivership, being closed, with a safeguard
plan, with a filing of the statement of claims and with a sale plan; natural person
with a SIREN; craftsperson (SIREN only in `inscriptionRM`); company without a SIREN;
notice with several persons; correction; missing city.

**Anonymisation.** Companies are left as they are: this is public legal data. For
**natural persons**:
- surname, first name, usual name and personal address are replaced **everywhere in
  the notice**, including in the judgment text, which often repeats the name;
- the SIREN is replaced with a fake `999…` SIREN that passes the Luhn check;
- `id`, `numeroannonce` and `url_complete` are changed (`…900xx`), because they would
  lead back to the real notice.

An automated check confirmed that no original value remains.
