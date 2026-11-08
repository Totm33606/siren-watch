"""Tests for validate.py from the fixture (no network calls, no real storage)."""

import datetime
import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from sirenwatch.bodacc import transform as tr
from sirenwatch.bodacc import validate as va

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = json.loads((FIXTURES / "bodacc_collective_sample.json").read_text(encoding="utf-8"))
EXTRACTED_AT = datetime.datetime(2026, 11, 7, 8, 0, tzinfo=datetime.UTC)
D = datetime.date(2026, 10, 25)  # a Sunday
MONDAY = datetime.date(2026, 10, 26)


def table_for(date):
    """Clean table: the fixture's notices, all dated `date`."""
    records = [{**r, "dateparution": date.isoformat()} for r in SAMPLE]
    return tr.transform_records(records, EXTRACTED_AT)


def replace_column(table, name, values):
    index = table.schema.get_field_index(name)
    field = table.schema.field(name)
    return table.set_column(index, field, pa.array(values, type=field.type))


class FakeStorage:
    bucket = "test"

    def __init__(self, objects):
        self.objects = objects

    def read_bytes(self, key):
        return self.objects[key]


# --- valid cases -------------------------------------------------------------------


def test_table_propre_valide():
    report = va.check_table(table_for(D), D)
    assert report.ok, report.errors
    assert report.row_count == 12
    assert report.warnings == []


def test_taux_de_siren_manquants():
    # 2 notices out of 12 have no SIREN in the fixture.
    report = va.check_table(table_for(D), D)
    assert report.missing_siren_rate == pytest.approx(2 / 12)


def test_natures_non_classees_listees():
    report = va.check_table(table_for(D), D)
    assert report.unclassified_natures == {"Autre jugement et ordonnance": 1}


def test_zero_ligne_un_dimanche_sans_avertissement():
    report = va.check_table(tr.transform_records([], EXTRACTED_AT), D)
    assert report.ok
    assert report.warnings == []
    assert report.missing_siren_rate == 0.0


def test_zero_ligne_un_jour_ouvre_avertit_sans_echouer():
    report = va.check_table(tr.transform_records([], EXTRACTED_AT), MONDAY)
    assert report.ok
    assert len(report.warnings) == 1
    assert "jour ouvré" in report.warnings[0]


# --- failing cases -----------------------------------------------------------------


def test_doublon_id_annonce():
    table = table_for(D)
    table = pa.concat_tables([table, table.slice(0, 1)])
    report = va.check_table(table, D)
    assert not report.ok
    assert any("id_annonce en double" in e for e in report.errors)


def test_siren_invalide():
    table = table_for(D)
    sirens = table.column("siren").to_pylist()
    sirens[0] = "934616559"  # wrong Luhn check digit
    report = va.check_table(replace_column(table, "siren", sirens), D)
    assert any("siren invalide" in e for e in report.errors)


def test_siren_absent_de_sirens():
    table = table_for(D)
    lists = table.column("sirens").to_pylist()
    lists[0] = []
    report = va.check_table(replace_column(table, "sirens", lists), D)
    assert any("absent de sirens" in e for e in report.errors)


def test_colonne_manquante():
    table = table_for(D).drop_columns(["tribunal"])
    report = va.check_table(table, D)
    assert report.errors == ["Colonnes manquantes : ['tribunal']"]


def test_mauvais_type():
    table = table_for(D)
    index = table.schema.get_field_index("nb_personnes")
    table = table.set_column(index, "nb_personnes", pc.cast(table.column(index), pa.string()))
    report = va.check_table(table, D)
    assert any("Types incorrects" in e and "nb_personnes" in e for e in report.errors)


def test_valeur_obligatoire_nulle():
    table = table_for(D)
    values = table.column("type_avis").to_pylist()
    values[0] = None
    field = pa.field("type_avis", pa.string())  # nullable, so that a None can be injected
    index = table.schema.get_field_index("type_avis")
    table = table.set_column(index, field, pa.array(values))
    report = va.check_table(table, D)
    assert any("type_avis (obligatoire)" in e for e in report.errors)


def test_ligne_d_une_autre_date():
    report = va.check_table(table_for(D), MONDAY)
    assert any("autre date" in e for e in report.errors)


def test_valeur_hors_liste():
    table = table_for(D)
    values = table.column("evenement").to_pylist()
    values[0] = "ouvertur"
    report = va.check_table(replace_column(table, "evenement", values), D)
    assert any("Valeurs inconnues dans evenement" in e for e in report.errors)


# --- validate(): reading clean/ and failing the run ------------------------------------


def test_validate_ok_journalise_les_indicateurs(caplog):
    key = "clean/bodacc/date=2026-10-25/annonces.parquet"
    storage = FakeStorage({key: tr.table_to_parquet_bytes(table_for(D))})
    with caplog.at_level(logging.INFO, logger="sirenwatch.bodacc.validate"):
        report = va.validate(D, storage=storage)
    assert report.ok
    assert "12 ligne(s)" in caplog.text
    assert "Taux de SIREN manquants : 16.7 %" in caplog.text


def test_validate_echec_leve_une_erreur():
    table = table_for(D)
    table = pa.concat_tables([table, table.slice(0, 1)])
    key = "clean/bodacc/date=2026-10-25/annonces.parquet"
    storage = FakeStorage({key: tr.table_to_parquet_bytes(table)})
    with pytest.raises(va.ValidationError, match="contrôle"):
        va.validate(D, storage=storage)
