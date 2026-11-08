"""Transformation `raw/` (JSON brut de l'API) -> `clean/` (Parquet typé).

Une ligne par annonce. Les règles de correspondance sont documentées dans README.md
(section « Correspondance retenue avec le schéma clean/ »).

Le cœur est `transform_records()`, une fonction pure (liste d'annonces -> table Arrow)
testée sans stockage ni réseau. `transform()` ne fait que lire `raw/`, appeler cette
fonction et écrire `clean/`.
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

# Schéma explicite : on ne laisse pas pyarrow deviner les types (une colonne
# entièrement nulle un jour serait typée `null`, et les partitions deviendraient
# incompatibles entre elles). `nullable=False` documente ce que validate vérifie.
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
    """Retire espaces et espaces insécables : '814 811 493' -> '814811493'."""
    if value is None:
        return None
    return re.sub(r"\s", "", value)


def is_valid_siren(value: str | None) -> bool:
    """9 chiffres et clé de Luhn correcte (toutes les valeurs observées la respectent)."""
    if value is None or not _SIREN_RE.fullmatch(value):
        return False
    total = 0
    for i, ch in enumerate(reversed(value)):
        d = int(ch)
        if i % 2:  # un chiffre sur deux en partant de la droite est doublé
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return total % 10 == 0


# --- classification des natures de jugement -------------------------------------


def _fold(text: str) -> str:
    """Minuscules sans accents : 'Clôture' -> 'cloture'. Rend les règles robustes."""
    decomposed = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def classify_nature(nature: str | None) -> tuple[str, str]:
    """(type_procedure, evenement) à partir du libellé `jugement.nature`.

    Règles par mots-clés, de la plus spécifique à la plus générale. Un libellé
    inconnu donne ('indetermine', 'autre') : validate liste ces libellés.
    """
    n = _fold(nature or "")

    # Procédure : la plus avancée citée l'emporte ("résolution du plan de sauvegarde
    # et la liquidation judiciaire" -> liquidation). Les clôtures pour insuffisance
    # d'actif ou extinction du passif sont des modes de clôture de la liquidation.
    if "liquidation" in n or "insuffisance d'actif" in n or "extinction du passif" in n:
        procedure = "liquidation"
    elif "redressement" in n:
        procedure = "redressement"
    elif "sauvegarde" in n:
        procedure = "sauvegarde"
    else:
        procedure = "indetermine"

    # Événement : "créances" avant "ouverture" ("Liste des créances nées après le
    # jugement d'ouverture…" est un dépôt de créances, pas une ouverture).
    if "retractation" in n:
        # "Rétractation de jugement d'ouverture…" annule une ouverture : pas une ouverture.
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


# --- annonce -> ligne -------------------------------------------------------------


def _personnes(record: dict[str, Any]) -> list[dict[str, Any]]:
    """`listepersonnes` est une chaîne JSON ; `personne` est un objet OU une liste."""
    if not record.get("listepersonnes"):
        return []
    personne = json.loads(record["listepersonnes"]).get("personne") or []
    return personne if isinstance(personne, list) else [personne]


def _rm_numbers(personnes: list[dict[str, Any]]) -> list[str]:
    """Numéros du répertoire des métiers (artisans) : ce sont des SIREN."""
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

    # siren_brut : la valeur source telle quelle, jamais corrigée.
    siren_brut = registre[0] if registre else (rm[0] if rm else None)
    siren_norm = normalize_siren(siren_brut)
    siren = siren_norm if is_valid_siren(siren_norm) else None

    # sirens : tous les SIREN valides de l'annonce (RCS puis RM), sans doublon,
    # dans l'ordre de la source. `registre` liste chaque SIREN deux fois.
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
    """Annonces brutes -> table Arrow conforme à CLEAN_SCHEMA (une ligne par annonce)."""
    if date_extraction.tzinfo is None:
        raise ValueError("date_extraction doit porter un fuseau horaire (UTC conseillé)")
    rows = [record_to_row(r, date_extraction) for r in records]
    return pa.Table.from_pylist(rows, schema=CLEAN_SCHEMA)


# --- E/S -------------------------------------------------------------------------


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    # zstd : bien plus compact que snappy pour un coût CPU négligeable ici ; lu par DuckDB.
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def parquet_bytes_to_table(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data))


def transform(
    date: datetime.date,
    date_extraction: datetime.datetime,
    storage: Storage | None = None,
) -> str:
    """Lit `raw/` pour `date`, écrit `clean/`. Renvoie la clé écrite.

    `date_extraction` est fourni par l'appelant (le DAG), jamais lu sur l'horloge.
    Idempotent : relancer réécrit le même objet. `[]` produit un Parquet vide valide.
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
