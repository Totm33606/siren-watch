"""Intégrité du DAG bodacc_daily.

Les tests qui chargent le DAG ont besoin d'Airflow : ils tournent dans l'image via
`make test-dags`, et sont ignorés (skip) par `make test` sur le poste, où Airflow
n'est pas installé. Le test « pas de code métier » ne lit que le fichier source
et tourne partout.
"""

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_FILE = DAGS_DIR / "bodacc_daily.py"
DAG_ID = "bodacc_daily"


@pytest.fixture(scope="module")
def dag():
    pytest.importorskip("airflow", reason="Airflow absent : lancer `make test-dags`")
    from airflow.dag_processing.dagbag import DagBag

    # Airflow 3.4 : DagBag ne charge que dag_folder (plus d'option include_examples).
    dagbag = DagBag(dag_folder=str(DAGS_DIR))
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert DAG_ID in dagbag.dags
    return dagbag.dags[DAG_ID]


def test_un_seul_dag(dag):
    assert dag.dag_id == DAG_ID


def test_trois_taches_en_chaine(dag):
    assert set(dag.task_ids) == {"extract", "transform", "validate"}
    assert dag.get_task("extract").downstream_task_ids == {"transform"}
    assert dag.get_task("transform").downstream_task_ids == {"validate"}
    assert dag.get_task("validate").downstream_task_ids == set()


def test_planning_quotidien_en_utc(dag):
    from airflow.sdk import CronTriggerTimetable

    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert dag.timetable.expression == "0 10 * * *"
    assert str(dag.timetable.timezone) == "UTC"


def test_catchup_et_concurrence(dag):
    assert dag.catchup is True  # requis pour le rattrapage et le backfill
    assert 1 <= dag.max_active_runs <= 5  # ne pas saturer l'API


def test_retries(dag):
    for task in dag.tasks:
        assert task.retries >= 1, task.task_id


def test_start_date_fixe_en_utc(dag):
    assert dag.start_date is not None
    assert dag.start_date.utcoffset().total_seconds() == 0


def test_pas_de_code_metier_au_niveau_du_module():
    """Le DAG n'importe sirenwatch que dans les tâches, pour un parsing rapide."""
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    top_level_modules = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_level_modules += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            top_level_modules.append(node.module or "")
    assert not [m for m in top_level_modules if m.startswith("sirenwatch")]
