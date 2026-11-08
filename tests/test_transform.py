"""Tests for transform.py from the fixture (no network calls, no real storage)."""

import datetime
import json
from pathlib import Path

import pyarrow as pa
import pytest

from sirenwatch.bodacc import transform as tr

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = json.loads((FIXTURES / "bodacc_collective_sample.json").read_text(encoding="utf-8"))
EXTRACTED_AT = datetime.datetime(2026, 11, 7, 8, 0, tzinfo=datetime.UTC)
D = datetime.date(2026, 10, 25)


@pytest.fixture(scope="module")
def rows():
    table = tr.transform_records(SAMPLE, EXTRACTED_AT)
    return {r["id_annonce"]: r for r in table.to_pylist()}


class FakeStorage:
    bucket = "test"

    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def read_bytes(self, key):
        if key not in self.objects:
            raise FileNotFoundError(key)
        return self.objects[key]

    def write_bytes(self, key, data, content_type="application/octet-stream"):
        self.objects[key] = data


# --- SIREN -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        ("934616558", True),
        ("732829320", True),
        ("934616559", False),  # wrong Luhn check digit
        ("93461655", False),  # 8 digits
        ("934 616 558", False),  # not normalised
        ("ABCDEFGHI", False),
        ("", False),
        (None, False),
    ],
)
def test_is_valid_siren(value, ok):
    assert tr.is_valid_siren(value) is ok


def test_normalize_siren():
    assert tr.normalize_siren("934 616 558") == "934616558"
    # Non-breaking spaces (U+00A0), written as escapes so that they stay visible.
    assert tr.normalize_siren("934\u00a0616\u00a0558") == "934616558"
    assert tr.normalize_siren(None) is None


# --- classification ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("nature", "attendu"),
    [
        ("Jugement d'ouverture de liquidation judiciaire", ("liquidation", "ouverture")),
        ("Jugement de clôture pour insuffisance d'actif", ("liquidation", "cloture")),
        ("Jugement de clôture pour extinction du passif", ("liquidation", "cloture")),
        (
            "Liste des créances nées après le jugement d'ouverture d'une procédure de "
            "liquidation judiciaire",
            ("liquidation", "creances"),
        ),
        ("Jugement de conversion en liquidation judiciaire", ("liquidation", "conversion")),
        (
            "Jugement prononçant la résolution du plan de sauvegarde et la liquidation judiciaire",
            ("liquidation", "conversion"),
        ),
        (
            "Jugement d'ouverture d'une procédure de redressement judiciaire",
            ("redressement", "ouverture"),
        ),
        (
            "Jugement de conversion en redressement judiciaire de la procédure de sauvegarde",
            ("redressement", "conversion"),
        ),
        ("Jugement de plan de redressement", ("redressement", "plan")),
        (
            "Jugement mettant fin à la procédure de redressement judiciaire",
            ("redressement", "cloture"),
        ),
        ("Jugement d'ouverture d'une procédure de sauvegarde", ("sauvegarde", "ouverture")),
        ("Jugement arrêtant le plan de sauvegarde", ("sauvegarde", "plan")),
        ("Dépôt de l'état des créances", ("indetermine", "creances")),
        ("Dépôt du projet de répartition", ("indetermine", "creances")),
        ("Jugement arrêtant un plan de cession", ("indetermine", "plan")),
        ("Jugement d'interdiction de gérer", ("indetermine", "sanction")),
        ("Jugement de faillite personnelle", ("indetermine", "sanction")),
        (
            "Rétractation de jugement d'ouverture sur tierce opposition",
            ("indetermine", "autre"),
        ),
        ("Autre jugement et ordonnance", ("indetermine", "autre")),
        ("Libellé jamais vu", ("indetermine", "autre")),
        (None, ("indetermine", "autre")),
    ],
)
def test_classify_nature(nature, attendu):
    assert tr.classify_nature(nature) == attendu


def test_classify_insensible_aux_accents_et_a_la_casse():
    assert tr.classify_nature("JUGEMENT DE CLOTURE POUR INSUFFISANCE D'ACTIF") == (
        "liquidation",
        "cloture",
    )


# --- fixture notices ----------------------------------------------------------------------


def test_une_ligne_par_annonce(rows):
    assert len(rows) == len(SAMPLE)


def test_personne_morale_cas_nominal(rows):
    r = rows["A202602052542"]
    assert r["date_parution"] == datetime.date(2026, 10, 25)
    assert r["siren"] == "934616558"
    assert r["siren_brut"] == "934616558"
    assert r["sirens"] == ["934616558"]  # registre lists each SIREN twice
    assert r["nb_personnes"] == 1
    assert r["raison_sociale"] == "AT CONSEIL"
    assert (r["type_procedure"], r["evenement"]) == ("liquidation", "ouverture")
    assert r["nature_jugement"] == "Jugement d'ouverture de liquidation judiciaire"
    assert r["date_jugement"] == datetime.date(2026, 10, 8)
    assert r["type_avis"] == "annonce"
    assert r["tribunal"] == "Greffe du Tribunal de Commerce de Sedan"
    assert r["departement"] == "08"
    assert r["ville"] == "Sedan"
    assert r["url"].endswith("A202602052542")
    assert r["date_extraction"] == EXTRACTED_AT


def test_artisan_siren_depuis_le_repertoire_des_metiers(rows):
    r = rows["A2026020590007"]  # empty registre, SIREN in inscriptionRM
    assert r["siren_brut"] == "999 007 008"  # as published
    assert r["siren"] == "999007008"
    assert r["sirens"] == ["999007008"]


def test_sans_siren(rows):
    r = rows["A202602052539"]
    assert r["siren"] is None
    assert r["siren_brut"] is None
    assert r["sirens"] == []


def test_plusieurs_personnes(rows):
    r = rows["A202602052793"]
    assert r["nb_personnes"] == 2
    assert r["siren"] == "345239875"  # the first one
    assert r["sirens"] == ["345239875", "521018788"]  # no SIREN lost


def test_rectificatif(rows):
    assert rows["A202602065501"]["type_avis"] == "rectificatif"


def test_ville_absente(rows):
    r = rows["A202602053071"]
    assert r["ville"] is None
    assert (r["type_procedure"], r["evenement"]) == ("sauvegarde", "plan")


def test_siren_brut_invalide_conserve_mais_siren_vide():
    record = {**SAMPLE[0], "registre": ["934616559", "934 616 559"]}  # wrong Luhn check digit
    row = tr.record_to_row(record, EXTRACTED_AT)
    assert row["siren_brut"] == "934616559"
    assert row["siren"] is None
    assert row["sirens"] == []


def test_date_de_jugement_illisible_ignoree():
    jugement = json.loads(SAMPLE[0]["jugement"]) | {"date": "pas une date"}
    row = tr.record_to_row({**SAMPLE[0], "jugement": json.dumps(jugement)}, EXTRACTED_AT)
    assert row["date_jugement"] is None


# --- table and Parquet ----------------------------------------------------------------


def test_schema_explicite():
    table = tr.transform_records(SAMPLE, EXTRACTED_AT)
    assert table.schema.equals(tr.CLEAN_SCHEMA)


def test_jour_vide_donne_une_table_vide_au_bon_schema():
    table = tr.transform_records([], EXTRACTED_AT)
    assert table.num_rows == 0
    assert table.schema.equals(tr.CLEAN_SCHEMA)


def test_date_extraction_sans_fuseau_refusee():
    with pytest.raises(ValueError, match="fuseau"):
        tr.transform_records(SAMPLE, datetime.datetime(2026, 11, 7, 8, 0))


def test_aller_retour_parquet():
    table = tr.transform_records(SAMPLE, EXTRACTED_AT)
    back = tr.parquet_bytes_to_table(tr.table_to_parquet_bytes(table))
    assert back.equals(table)
    assert back.schema.field("sirens").type == pa.list_(pa.string())


# --- transform(): raw/ -> clean/ -----------------------------------------------------------


def test_transform_lit_raw_et_ecrit_clean():
    raw_key = "raw/bodacc/date=2026-10-25/annonces.json"
    storage = FakeStorage({raw_key: json.dumps(SAMPLE, ensure_ascii=False).encode()})
    key = tr.transform(D, EXTRACTED_AT, storage=storage)
    assert key == "clean/bodacc/date=2026-10-25/annonces.parquet"
    table = tr.parquet_bytes_to_table(storage.objects[key])
    assert table.num_rows == len(SAMPLE)


def test_transform_jour_vide():
    storage = FakeStorage({"raw/bodacc/date=2026-10-25/annonces.json": b"[]"})
    key = tr.transform(D, EXTRACTED_AT, storage=storage)
    assert tr.parquet_bytes_to_table(storage.objects[key]).num_rows == 0


def test_transform_sans_raw_echoue():
    with pytest.raises(FileNotFoundError):
        tr.transform(D, EXTRACTED_AT, storage=FakeStorage())


def test_transform_refuse_un_datetime_comme_date():
    with pytest.raises(TypeError):
        tr.transform(EXTRACTED_AT, EXTRACTED_AT, storage=FakeStorage())
