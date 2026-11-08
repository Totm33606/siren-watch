"""
### bodacc_daily

Daily collection of BODACC **insolvency notices** (procédures collectives).

`extract` → `transform` → `validate`, for publication date **D** = the run's logical date:

- `extract`: BODACC API → `raw/bodacc/date=D/annonces.json`
- `transform`: `raw/` → `clean/bodacc/date=D/annonces.parquet`
- `validate`: quality checks; fails the run if a check fails

All the logic lives in the `sirenwatch` package; this file only wires the tasks together.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, task

# No `sirenwatch` import here, at module level: the DAG processor executes this file
# every ~30 s to read the DAG structure. Business code is imported inside the tasks,
# so only when they run.


@dag(
    dag_id="bodacc_daily",
    # Every day at 10:00 UTC, weekends included (the BODACC sometimes publishes on
    # Saturday or Sunday). Day D's publication is loaded around 06:00-07:00 UTC: 10:00
    # leaves a margin.
    # CronTriggerTimetable (the default for a cron in Airflow 3): a run's logical date
    # IS its trigger time. The 2026-11-06 10:00 UTC run therefore processes the
    # 2026-11-06 publication, not the previous day's.
    schedule=CronTriggerTimetable("0 10 * * *", timezone="UTC"),
    # catchup: when the DAG is enabled, Airflow creates the missing runs since the
    # latest run, or since start_date (here one week). Older history is loaded with
    # `make backfill`.
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,
    # At most 3 dates in parallel: ~20 requests per date, the API is not overloaded.
    max_active_runs=3,
    default_args={
        # extract already retries each HTTP request; these Airflow retries cover longer
        # outages (API or MinIO unavailable for several minutes).
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["bodacc", "v1"],
    doc_md=__doc__,
)
def bodacc_daily():
    # `ds` (logical date, "YYYY-MM-DD") and `ti` (task instance) are injected by Airflow
    # because the parameters are named after these context variables.

    @task
    def extract(ds: str | None = None, ti=None) -> dict[str, str]:
        from sirenwatch.bodacc.extract import extract as extract_day

        raw_key = extract_day(datetime.date.fromisoformat(ds))
        # Extraction timestamp = start of this task, provided by Airflow (the business
        # code never reads the clock). Passed to transform via XCom: a small dict, not
        # the data, which goes through MinIO.
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
        # Summary returned as XCom: visible in the UI, in the task's XCom tab.
        return {
            "clean_key": clean_key,
            "row_count": report.row_count,
            "missing_siren_rate": round(report.missing_siren_rate, 4),
            "warnings": report.warnings,
        }

    validate(transform(extract()))


bodacc_daily()
