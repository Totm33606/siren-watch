"""
### bodacc_daily

Collecte quotidienne des annonces BODACC de **procédures collectives**.

`extract` → `transform` → `validate`, pour la date de parution **D** = date logique du run :

- `extract` : API BODACC → `raw/bodacc/date=D/annonces.json`
- `transform` : `raw/` → `clean/bodacc/date=D/annonces.parquet`
- `validate` : contrôles qualité ; fait échouer le run si KO

Toute la logique est dans le paquet `sirenwatch` ; ce fichier ne fait que relier les tâches.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, task

# Pas d'import de `sirenwatch` ici, au niveau du module : le dag-processor exécute ce
# fichier toutes les ~30 s pour lire la structure du DAG. Les imports du code métier
# sont faits dans les tâches, donc seulement à l'exécution.


@dag(
    dag_id="bodacc_daily",
    # Tous les jours à 10:00 UTC, week-end compris (le BODACC paraît parfois le samedi
    # ou le dimanche). La parution du jour D est chargée vers 06:00-07:00 UTC : 10:00
    # laisse de la marge.
    # CronTriggerTimetable (comportement par défaut d'un cron en Airflow 3) : la date
    # logique d'un run EST son heure de déclenchement. Le run du 2026-11-06 10:00 UTC
    # traite donc la parution du 2026-11-06, pas celle de la veille.
    schedule=CronTriggerTimetable("0 10 * * *", timezone="UTC"),
    # catchup : à l'activation, Airflow crée les runs manquants depuis start_date
    # (ici une semaine). L'historique plus ancien se charge avec `make backfill`.
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,
    # Au plus 3 dates en parallèle : ~20 requêtes par date, l'API n'est pas saturée.
    max_active_runs=3,
    default_args={
        # extract réessaie déjà chaque requête HTTP ; ces retries Airflow couvrent
        # les pannes plus longues (API ou MinIO indisponibles plusieurs minutes).
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["bodacc", "v1"],
    doc_md=__doc__,
)
def bodacc_daily():
    # `ds` (date logique, "YYYY-MM-DD") et `ti` (instance de tâche) sont injectés
    # par Airflow parce que les paramètres portent ces noms de variables de contexte.

    @task
    def extract(ds: str | None = None, ti=None) -> dict[str, str]:
        from sirenwatch.bodacc.extract import extract as extract_day

        raw_key = extract_day(datetime.date.fromisoformat(ds))
        # Horodatage de l'extraction = début de cette tâche, fourni par Airflow (le
        # code métier ne lit jamais l'horloge). Transmis à transform par XCom : un
        # petit dictionnaire, pas les données, qui transitent par MinIO.
        return {"raw_key": raw_key, "extracted_at": ti.start_date.isoformat()}

    @task
    def transform(extraction: dict[str, str], ds: str | None = None) -> str:
        from sirenwatch.bodacc.transform import transform as transform_day

        return transform_day(
            datetime.date.fromisoformat(ds),
            datetime.datetime.fromisoformat(extraction["extracted_at"]),
        )

    @task
    def validate(clean_key: str, ds: str | None = None) -> dict[str, object]:
        from sirenwatch.bodacc.validate import validate as validate_day

        report = validate_day(datetime.date.fromisoformat(ds))
        # Résumé renvoyé en XCom : visible dans l'interface, onglet XCom de la tâche.
        return {
            "clean_key": clean_key,
            "row_count": report.row_count,
            "missing_siren_rate": round(report.missing_siren_rate, 4),
            "warnings": report.warnings,
        }

    validate(transform(extract()))


bodacc_daily()
