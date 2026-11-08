"""Contrôles de qualité de `clean/` pour une date. Fait échouer le run si KO.

Contrôles bloquants (erreurs) :
- schéma et types conformes à CLEAN_SCHEMA, colonnes obligatoires sans valeur nulle ;
- `siren` valide (9 chiffres + clé de Luhn) quand il est présent, idem pour `sirens` ;
- pas de doublon sur `id_annonce` ;
- toutes les lignes ont `date_parution` = la date contrôlée ;
- `type_procedure` et `evenement` dans leurs listes de valeurs.

Journalisé sans échec :
- nombre de lignes, taux de SIREN manquants, natures de jugement non classées ;
- zéro ligne un jour ouvré : avertissement (le BODACC a de vrais jours sans parution).

Contrôles en Python simple : un framework comme Great Expectations serait
disproportionné pour une seule table.
"""

from __future__ import annotations

import datetime
import logging
from collections import Counter
from dataclasses import dataclass, field

import pyarrow as pa

from sirenwatch.bodacc.transform import (
    CLEAN_SCHEMA,
    EVENEMENTS,
    TYPES_PROCEDURE,
    is_valid_siren,
    parquet_bytes_to_table,
)
from sirenwatch.config import load_settings
from sirenwatch.storage import Storage, partition_key

log = logging.getLogger(__name__)

MAX_EXAMPLES = 5  # valeurs fautives citées dans un message d'erreur
_CHECKED_COLUMNS = (
    "id_annonce",
    "date_parution",
    "siren",
    "sirens",
    "type_procedure",
    "evenement",
    "nature_jugement",
)


class ValidationError(RuntimeError):
    """Au moins un contrôle bloquant a échoué."""


@dataclass
class ValidationReport:
    date: datetime.date
    row_count: int = 0
    missing_siren_rate: float = 0.0
    unclassified_natures: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def check_table(table: pa.Table, date: datetime.date) -> ValidationReport:
    """Applique tous les contrôles à `table` et renvoie un rapport (ne lève pas)."""
    report = ValidationReport(date=date, row_count=table.num_rows)

    _check_schema(table, report)
    if report.errors:
        return report  # colonnes manquantes ou mal typées : la suite n'aurait pas de sens

    # Seules les colonnes contrôlées sont converties en listes Python.
    cols = {name: table.column(name).to_pylist() for name in _CHECKED_COLUMNS}
    _check_not_null(table, report)
    _check_sirens(cols, report)
    _check_unique_ids(cols["id_annonce"], report)
    _check_date(cols["date_parution"], date, report)
    _check_allowed(cols, "type_procedure", TYPES_PROCEDURE, report)
    _check_allowed(cols, "evenement", EVENEMENTS, report)

    n = table.num_rows
    report.missing_siren_rate = sum(s is None for s in cols["siren"]) / n if n else 0.0
    report.unclassified_natures = dict(
        Counter(
            nat
            for nat, ev in zip(cols["nature_jugement"], cols["evenement"], strict=True)
            if ev == "autre"
        )
    )
    if n == 0 and date.weekday() < 5:
        report.warnings.append(
            f"Aucune annonce le {date} (jour ouvré) : jour sans parution ou extraction trop tôt ?"
        )
    return report


def validate(date: datetime.date, storage: Storage | None = None) -> ValidationReport:
    """Contrôle `clean/` pour `date`. Lève ValidationError si un contrôle bloquant échoue."""
    storage = storage or Storage.from_settings(load_settings())
    key = partition_key("clean", "bodacc", date, "annonces.parquet")
    report = check_table(parquet_bytes_to_table(storage.read_bytes(key)), date)

    log.info("Validation %s : %d ligne(s)", date, report.row_count)
    log.info("Taux de SIREN manquants : %.1f %%", report.missing_siren_rate * 100)
    if report.unclassified_natures:
        log.info("Natures non classées (evenement = autre) : %s", report.unclassified_natures)
    for warning in report.warnings:
        log.warning(warning)
    for error in report.errors:
        log.error(error)
    if not report.ok:
        raise ValidationError(f"{len(report.errors)} contrôle(s) en échec pour {date}")
    return report


# --- contrôles -------------------------------------------------------------------


def _check_schema(table: pa.Table, report: ValidationReport) -> None:
    expected = {f.name: f.type for f in CLEAN_SCHEMA}
    actual = {f.name: f.type for f in table.schema}
    missing = [name for name in expected if name not in actual]
    extra = [name for name in actual if name not in expected]
    wrong = [
        f"{name} ({actual[name]} au lieu de {typ})"
        for name, typ in expected.items()
        if name in actual and not actual[name].equals(typ)
    ]
    if missing:
        report.errors.append(f"Colonnes manquantes : {missing}")
    if extra:
        report.errors.append(f"Colonnes inattendues : {extra}")
    if wrong:
        report.errors.append(f"Types incorrects : {wrong}")


def _check_not_null(table: pa.Table, report: ValidationReport) -> None:
    for f in CLEAN_SCHEMA:
        if not f.nullable:
            nulls = table.column(f.name).null_count
            if nulls:
                report.errors.append(f"{nulls} valeur(s) nulle(s) dans {f.name} (obligatoire)")


def _check_sirens(cols: dict[str, list], report: ValidationReport) -> None:
    bad = [s for s in cols["siren"] if s is not None and not is_valid_siren(s)]
    if bad:
        report.errors.append(f"{len(bad)} siren invalide(s), ex. {bad[:MAX_EXAMPLES]}")

    bad_list = [s for lst in cols["sirens"] if lst for s in lst if not is_valid_siren(s)]
    if bad_list:
        report.errors.append(
            f"{len(bad_list)} valeur(s) invalide(s) dans sirens, ex. {bad_list[:MAX_EXAMPLES]}"
        )

    # Cohérence : le siren principal fait partie de la liste complète.
    incoherent = [
        i
        for i, s, lst in zip(cols["id_annonce"], cols["siren"], cols["sirens"], strict=True)
        if s is not None and s not in (lst or [])
    ]
    if incoherent:
        report.errors.append(
            f"{len(incoherent)} annonce(s) dont siren est absent de sirens, "
            f"ex. {incoherent[:MAX_EXAMPLES]}"
        )


def _check_unique_ids(ids: list, report: ValidationReport) -> None:
    duplicates = [i for i, n in Counter(ids).items() if n > 1]
    if duplicates:
        report.errors.append(
            f"{len(duplicates)} id_annonce en double, ex. {duplicates[:MAX_EXAMPLES]}"
        )


def _check_date(dates: list, date: datetime.date, report: ValidationReport) -> None:
    other = Counter(d for d in dates if d != date)
    if other:
        report.errors.append(f"Lignes d'une autre date que {date} : {dict(other)}")


def _check_allowed(cols: dict[str, list], name: str, allowed, report: ValidationReport) -> None:
    unknown = Counter(v for v in cols[name] if v is not None and v not in allowed)
    if unknown:
        report.errors.append(f"Valeurs inconnues dans {name} : {dict(unknown)}")
