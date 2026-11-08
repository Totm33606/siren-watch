"""Extraction des annonces BODACC de procédures collectives pour une date de parution.

Écrit la réponse brute dans `raw/bodacc/date=YYYY-MM-DD/annonces.json`, sous la forme
d'un tableau JSON des enregistrements renvoyés par l'API, dans l'ordre de l'API
(tri par `id`). C'est exactement le format de l'endpoint d'export, et celui de
`tests/fixtures/bodacc_collective_sample.json`. Voir README.md pour les choix.

Lancement manuel :  uv run --env-file .env python -m sirenwatch.bodacc.extract 2026-11-06
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import time
from collections.abc import Callable
from typing import Any

import httpx

from sirenwatch.config import load_settings
from sirenwatch.storage import Storage, partition_key

log = logging.getLogger(__name__)

BASE_URL = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1"
DATASET = "annonces-commerciales"
RECORDS_PATH = f"/catalog/datasets/{DATASET}/records"
EXPORT_PATH = f"/catalog/datasets/{DATASET}/exports/json"

PAGE_SIZE = 100  # maximum autorisé par l'endpoint `records`
MAX_WINDOW = 10_000  # offset + limit ne peut pas dépasser cette valeur

MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 2.0
BACKOFF_MAX_SECONDS = 60.0
TIMEOUT = httpx.Timeout(30.0, connect=10.0)
USER_AGENT = "sirenwatch/0.1 (collecte BODACC procedures collectives)"

Record = dict[str, Any]
Sleep = Callable[[float], None]


class BodaccAPIError(RuntimeError):
    """L'API n'a pas répondu correctement, même après les retries."""


class IncoherentExtractionError(RuntimeError):
    """Le nombre d'annonces récupérées ne correspond pas au total annoncé par l'API."""


def build_where(date: datetime.date) -> str:
    """Filtre ODSQL : procédures collectives parues à `date`."""
    _check_date(date)
    return f"familleavis = \"collective\" AND dateparution = date'{date.isoformat()}'"


def fetch_annonces(
    date: datetime.date,
    client: httpx.Client,
    *,
    page_size: int = PAGE_SIZE,
    max_window: int = MAX_WINDOW,
    sleep: Sleep = time.sleep,
) -> list[Record]:
    """Récupère toutes les annonces de procédures collectives parues à `date`.

    Endpoint paginé `records` par pages de `page_size`. Si le total dépasse la
    fenêtre maximale de pagination (`offset + limit <= 10 000`), bascule sur
    l'endpoint d'export, qui renvoie tout en une réponse.
    """
    base_params = {"where": build_where(date), "order_by": "id"}

    first = _get_json(client, RECORDS_PATH, {**base_params, "limit": page_size}, sleep=sleep)
    total = first["total_count"]
    log.info("BODACC %s : %d annonce(s) annoncée(s) par l'API", date, total)

    if total > max_window:
        log.info("Total > %d : bascule sur l'endpoint d'export", max_window)
        records = _get_json(client, EXPORT_PATH, base_params, sleep=sleep)
    else:
        records = list(first["results"])
        for offset in range(page_size, total, page_size):
            page = _get_json(
                client,
                RECORDS_PATH,
                {**base_params, "limit": page_size, "offset": offset},
                sleep=sleep,
            )
            records.extend(page["results"])

    _check_coherence(records, total)
    return records


def extract(
    date: datetime.date,
    storage: Storage | None = None,
    client: httpx.Client | None = None,
    *,
    sleep: Sleep = time.sleep,
) -> str:
    """Extrait les annonces parues à `date` et les écrit dans `raw/`. Renvoie la clé écrite.

    Idempotent : relancer la même date réécrit le même objet. Une date sans annonce
    produit un tableau vide `[]`, qui est une partition valide.
    """
    _check_date(date)
    storage = storage or Storage.from_settings(load_settings())
    owns_client = client is None
    client = client or make_http_client()
    try:
        records = fetch_annonces(date, client, sleep=sleep)
    finally:
        if owns_client:
            client.close()

    key = partition_key("raw", "bodacc", date, "annonces.json")
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")
    storage.write_bytes(key, body, "application/json")
    log.info(
        "Écrit s3://%s/%s (%d annonces, %d octets)", storage.bucket, key, len(records), len(body)
    )
    return key


def make_http_client() -> httpx.Client:
    return httpx.Client(base_url=BASE_URL, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})


# --- HTTP avec retries ----------------------------------------------------------


def _get_json(
    client: httpx.Client,
    path: str,
    params: dict[str, Any],
    *,
    sleep: Sleep,
    max_attempts: int = MAX_ATTEMPTS,
) -> Any:
    """GET avec retries et backoff exponentiel.

    On réessaie sur ce qui peut être transitoire : erreurs réseau et timeouts,
    429 (trop de requêtes) et 5xx. Une autre erreur 4xx signale une requête
    invalide : réessayer ne changerait rien, on échoue tout de suite.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(path, params=params)
        except httpx.TransportError as exc:
            reason = f"{type(exc).__name__}: {exc}"
            delay = _backoff(attempt)
        else:
            if response.status_code == 429 or response.status_code >= 500:
                reason = f"HTTP {response.status_code}"
                delay = _retry_after(response) or _backoff(attempt)
            elif response.is_error:
                raise BodaccAPIError(
                    f"HTTP {response.status_code} sur {path} : {response.text[:300]}"
                )
            else:
                return response.json()

        if attempt == max_attempts:
            raise BodaccAPIError(f"Échec après {max_attempts} tentatives sur {path} ({reason})")
        log.warning(
            "Tentative %d/%d échouée (%s), nouvel essai dans %.1f s",
            attempt,
            max_attempts,
            reason,
            delay,
        )
        sleep(delay)
    raise AssertionError("inatteignable")  # pragma: no cover


def _backoff(attempt: int) -> float:
    """2 s, 4 s, 8 s, 16 s… plafonné à 60 s."""
    return min(BACKOFF_BASE_SECONDS * 2 ** (attempt - 1), BACKOFF_MAX_SECONDS)


def _retry_after(response: httpx.Response) -> float | None:
    """Délai demandé par le serveur (en-tête Retry-After en secondes), s'il est fourni."""
    value = response.headers.get("Retry-After", "")
    try:
        return min(max(float(value), 0.0), BACKOFF_MAX_SECONDS)
    except ValueError:
        return None


# --- contrôles ----------------------------------------------------------------


def _check_coherence(records: list[Record], total: int) -> None:
    """Détecte une pagination incohérente (jeu de données modifié entre deux pages)."""
    ids = [r.get("id") for r in records]
    if len(records) != total or len(set(ids)) != len(ids):
        raise IncoherentExtractionError(
            f"{len(records)} annonces récupérées ({len(set(ids))} ids distincts) "
            f"pour un total annoncé de {total}"
        )


def _check_date(date: datetime.date) -> None:
    if isinstance(date, datetime.datetime) or not isinstance(date, datetime.date):
        raise TypeError(f"date doit être un datetime.date, reçu {type(date).__name__}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "date", type=datetime.date.fromisoformat, help="date de parution YYYY-MM-DD"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    extract(args.date)


if __name__ == "__main__":
    main()
