"""Transformation `raw/` (raw API JSON) -> `clean/` (typed Parquet).

One row per notice. The mapping rules are documented in README.md (section "Mapping to
the `clean/` schema").

The core is `transform_records()`, a pure function (list of notices -> Arrow table)
tested without storage or network. `transform()` only reads `raw/`, calls that function
and writes `clean/`.
"""

from __future__ import annotations

import datetime
import io
import json
import logging
import re
import unicodedata
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from sirenwatch.config import load_settings
from sirenwatch.storage import Storage, partition_key

log = logging.getLogger(__name__)

TYPES_PROCEDURE = ("sauvegarde", "redressement", "liquidation", "indetermine")
EVENEMENTS = ("ouverture", "conversion", "plan", "cloture", "creances", "sanction", "autre")

# Explicit schema: pyarrow is not left to infer the types (a column that is entirely
# null on some day would be typed `null`, making partitions incompatible with each
# other). `nullable=False` documents what validate checks.
CLEAN_SCHEMA = pa.schema(
    [
        pa.field("id_annonce", pa.string(), nullable=False),
        pa.field("date_parution", pa.date32(), nullable=False),
        pa.field("siren", pa.string()),
        pa.field("siren_brut", pa.string()),
        pa.field("sirens", pa.list_(pa.string()), nullable=False),
        pa.field("nb_personnes", pa.int32(), nullable=False),
        pa.field("raison_sociale", pa.string()),
        pa.field("type_procedure", pa.string(), nullable=False),
        pa.field("evenement", pa.string(), nullable=False),
        pa.field("nature_jugement", pa.string()),
        pa.field("date_jugement", pa.date32()),
        pa.field("type_avis", pa.string(), nullable=False),
        pa.field("tribunal", pa.string()),
        pa.field("departement", pa.string()),
        pa.field("ville", pa.string()),
        pa.field("url", pa.string()),
        pa.field("date_extraction", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


# --- SIREN ---------------------------------------------------------------------

_SIREN_RE = re.compile(r"\d{9}")


def normalize_siren(value: str | None) -> str | None:
    """Remove spaces and non-breaking spaces: '814 811 493' -> '814811493'."""
    if value is None:
        return None
    return re.sub(r"\s", "", value)


def is_valid_siren(value: str | None) -> bool:
    """9 digits and a correct Luhn check digit (every observed value passes it)."""
    if value is None or not _SIREN_RE.fullmatch(value):
        return False
    total = 0
    for i, ch in enumerate(reversed(value)):
        d = int(ch)
        if i % 2:  # every second digit from the right is doubled
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return total % 10 == 0


# --- classification of court-decision labels -------------------------------------


def _fold(text: str) -> str:
    """Lowercase without accents: 'Clôture' -> 'cloture'. Makes the rules robust."""
    decomposed = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def classify_nature(nature: str | None) -> tuple[str, str]:
    """(type_procedure, evenement) from the `jugement.nature` label.

    Keyword rules, from the most specific to the most general. An unknown label gives
    ('indetermine', 'autre'): validate lists these labels.
    """
    n = _fold(nature or "")

    # Procedure: the most advanced one mentioned wins ("résolution du plan de sauvegarde
    # et la liquidation judiciaire" -> liquidation). Closings for insufficient assets or
    # extinguished liabilities are ways of closing a liquidation.
    if "liquidation" in n or "insuffisance d'actif" in n or "extinction du passif" in n:
        procedure = "liquidation"
    elif "redressement" in n:
        procedure = "redressement"
    elif "sauvegarde" in n:
        procedure = "sauvegarde"
    else:
        procedure = "indetermine"

    # Stage: "créances" is checked before "ouverture" ("Liste des créances nées après le
    # jugement d'ouverture…" is a filing of claims, not an opening).
    if "retractation" in n:
        # "Rétractation de jugement d'ouverture…" cancels an opening: not an opening.
        evenement = "autre"
    elif "creance" in n or "repartition" in n:
        evenement = "creances"
    elif "conversion" in n or "resolution du plan" in n:
        evenement = "conversion"
    elif "ouverture" in n:
        evenement = "ouverture"
    elif "cloture" in n or "mettant fin" in n:
        evenement = "cloture"
    elif "plan" in n:
        evenement = "plan"
    elif "interdiction" in n or "faillite personnelle" in n:
        evenement = "sanction"
    else:
        evenement = "autre"
    return procedure, evenement


# --- notice -> row ------------------------------------------------------------------


def _personnes(record: dict[str, Any]) -> list[dict[str, Any]]:
    """`listepersonnes` is a JSON string; `personne` is an object OR a list."""
    if not record.get("listepersonnes"):
        return []
    personne = json.loads(record["listepersonnes"]).get("personne") or []
    return personne if isinstance(personne, list) else [personne]


def _rm_numbers(personnes: list[dict[str, Any]]) -> list[str]:
    """Trades register numbers (craftspeople): these are SIRENs."""
    return [
        p["inscriptionRM"]["numeroIdentificationRM"]
        for p in personnes
        if (p.get("inscriptionRM") or {}).get("numeroIdentificationRM")
    ]


def _parse_date(value: str | None) -> datetime.date | None:
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value[:10])
    except ValueError:
        log.warning("Date de jugement illisible ignorée : %r", value)
        return None


def record_to_row(record: dict[str, Any], date_extraction: datetime.datetime) -> dict[str, Any]:
    personnes = _personnes(record)
    registre = record.get("registre") or []
    rm = _rm_numbers(personnes)

    # siren_brut: the source value as is, never corrected.
    siren_brut = registre[0] if registre else (rm[0] if rm else None)
    siren_norm = normalize_siren(siren_brut)
    siren = siren_norm if is_valid_siren(siren_norm) else None

    # sirens: all the valid SIRENs of the notice (RCS, then RM), deduplicated, in source
    # order. `registre` lists each SIREN twice.
    sirens: list[str] = []
    for value in [*registre, *rm]:
        s = normalize_siren(value)
        if is_valid_siren(s) and s not in sirens:
            sirens.append(s)

    jugement = json.loads(record["jugement"]) if record.get("jugement") else {}
    nature = jugement.get("nature")
    type_procedure, evenement = classify_nature(nature)

    return {
        "id_annonce": record["id"],
        "date_parution": datetime.date.fromisoformat(record["dateparution"]),
        "siren": siren,
        "siren_brut": siren_brut,
        "sirens": sirens,
        "nb_personnes": len(personnes),
        "raison_sociale": record.get("commercant"),
        "type_procedure": type_procedure,
        "evenement": evenement,
        "nature_jugement": nature,
        "date_jugement": _parse_date(jugement.get("date")),
        "type_avis": record["typeavis"],
        "tribunal": record.get("tribunal"),
        "departement": record.get("numerodepartement"),
        "ville": record.get("ville"),
        "url": record.get("url_complete"),
        "date_extraction": date_extraction,
    }


def transform_records(
    records: list[dict[str, Any]], date_extraction: datetime.datetime
) -> pa.Table:
    """Raw notices -> Arrow table matching CLEAN_SCHEMA (one row per notice)."""
    if date_extraction.tzinfo is None:
        raise ValueError("date_extraction doit porter un fuseau horaire (UTC conseillé)")
    rows = [record_to_row(r, date_extraction) for r in records]
    return pa.Table.from_pylist(rows, schema=CLEAN_SCHEMA)


# --- I/O ---------------------------------------------------------------------------


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    # zstd: much more compact than snappy, for a negligible CPU cost here; read by DuckDB.
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def parquet_bytes_to_table(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data))


def transform(
    date: datetime.date,
    date_extraction: datetime.datetime,
    storage: Storage | None = None,
) -> str:
    """Read `raw/` for `date`, write `clean/`. Return the key written.

    `date_extraction` is provided by the caller (the DAG), never read from the clock.
    Idempotent: re-running rewrites the same object. `[]` gives a valid empty Parquet.
    """
    if isinstance(date, datetime.datetime) or not isinstance(date, datetime.date):
        raise TypeError(f"date doit être un datetime.date, reçu {type(date).__name__}")
    storage = storage or Storage.from_settings(load_settings())

    raw_key = partition_key("raw", "bodacc", date, "annonces.json")
    records = json.loads(storage.read_bytes(raw_key).decode("utf-8"))
    table = transform_records(records, date_extraction)

    clean_key = partition_key("clean", "bodacc", date, "annonces.parquet")
    storage.write_bytes(clean_key, table_to_parquet_bytes(table), "application/vnd.apache.parquet")
    log.info("Écrit s3://%s/%s (%d lignes)", storage.bucket, clean_key, table.num_rows)
    return clean_key
