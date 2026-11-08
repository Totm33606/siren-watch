"""Extract the BODACC insolvency notices published on a given date.

Writes the raw response to `raw/bodacc/date=YYYY-MM-DD/annonces.json`, as a JSON array
of the records returned by the API, in API order (sorted by `id`). This is exactly the
format of the export endpoint, and of `tests/fixtures/bodacc_collective_sample.json`.
See README.md for the design decisions.

Manual run:  uv run --env-file .env sirenwatch-extract 2026-11-06
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

PAGE_SIZE = 100  # maximum allowed by the `records` endpoint
MAX_WINDOW = 10_000  # offset + limit cannot exceed this value

MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 2.0
BACKOFF_MAX_SECONDS = 60.0
TIMEOUT = httpx.Timeout(30.0, connect=10.0)
USER_AGENT = "sirenwatch/0.1 (collecte BODACC procedures collectives)"

Record = dict[str, Any]
Sleep = Callable[[float], None]


class BodaccAPIError(RuntimeError):
    """The API did not answer correctly, even after the retries."""


class IncoherentExtractionError(RuntimeError):
    """The number of notices retrieved does not match the total announced by the API."""


def build_where(date: datetime.date) -> str:
    """ODSQL filter: insolvency notices published on `date`."""
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
    """Fetch all the insolvency notices published on `date`.

    Paginated `records` endpoint, `page_size` records per page. If the total exceeds the
    pagination window (`offset + limit <= 10,000`), fall back to the export endpoint,
    which returns everything in a single response.
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
    """Extract the notices published on `date` and write them to `raw/`. Return the key.

    Idempotent: re-running the same date rewrites the same object. A date without
    notices produces an empty array `[]`, which is a valid partition.
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


# --- HTTP with retries ------------------------------------------------------------


def _get_json(
    client: httpx.Client,
    path: str,
    params: dict[str, Any],
    *,
    sleep: Sleep,
    max_attempts: int = MAX_ATTEMPTS,
) -> Any:
    """GET with retries and exponential backoff.

    Retries what may be transient: network errors and timeouts, 429 (too many requests)
    and 5xx. Any other 4xx means an invalid request: retrying would not help, so it
    fails immediately.
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
    """2 s, 4 s, 8 s, 16 s… capped at 60 s."""
    return min(BACKOFF_BASE_SECONDS * 2 ** (attempt - 1), BACKOFF_MAX_SECONDS)


def _retry_after(response: httpx.Response) -> float | None:
    """Delay requested by the server (Retry-After header, in seconds), if any."""
    value = response.headers.get("Retry-After", "")
    try:
        return min(max(float(value), 0.0), BACKOFF_MAX_SECONDS)
    except ValueError:
        return None


# --- checks ---------------------------------------------------------------------


def _check_coherence(records: list[Record], total: int) -> None:
    """Detect inconsistent pagination (dataset modified between two pages)."""
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
