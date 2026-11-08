"""Tests for extract.py against a fake API (httpx.MockTransport): no network calls."""

import datetime
import json
from pathlib import Path

import httpx
import pytest

from sirenwatch.bodacc import extract as ex

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = json.loads((FIXTURES / "bodacc_collective_sample.json").read_text(encoding="utf-8"))
D = datetime.date(2026, 11, 5)


class FakeStorage:
    bucket = "test"

    def __init__(self):
        self.objects: dict[str, tuple[bytes, str]] = {}

    def write_bytes(self, key, data, content_type="application/octet-stream"):
        self.objects[key] = (data, content_type)


class FakeAPI:
    """Mimics the Opendatasoft API: paginated `records` (limit/offset) and `exports/json`."""

    def __init__(self, records, failures=None):
        self.records = records
        self.failures = list(failures or [])  # responses served before the real ones
        self.calls: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.failures:
            failure = self.failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure
        if request.url.path.endswith("/exports/json"):
            return httpx.Response(200, json=self.records)
        limit = int(request.url.params["limit"])
        offset = int(request.url.params.get("offset", 0))
        page = self.records[offset : offset + limit]
        return httpx.Response(200, json={"total_count": len(self.records), "results": page})

    def client(self):
        return httpx.Client(base_url=ex.BASE_URL, transport=httpx.MockTransport(self))


class Sleeps(list):
    def __call__(self, seconds):
        self.append(seconds)


# --- filter --------------------------------------------------------------------


def test_filtre_famille_et_date():
    assert ex.build_where(D) == ("familleavis = \"collective\" AND dateparution = date'2026-11-05'")


def test_refuse_un_datetime():
    with pytest.raises(TypeError):
        ex.build_where(datetime.datetime(2026, 11, 5, 8, 0))


def test_requete_envoyee():
    api = FakeAPI(SAMPLE)
    ex.fetch_annonces(D, api.client(), sleep=Sleeps())
    params = api.calls[0].url.params
    assert api.calls[0].url.path.endswith("/annonces-commerciales/records")
    assert params["where"] == ex.build_where(D)
    assert params["order_by"] == "id"  # stable order across pages
    assert params["limit"] == "100"


# --- pagination ----------------------------------------------------------------


def test_pagination_recupere_toutes_les_pages_dans_l_ordre():
    api = FakeAPI(SAMPLE)  # 12 notices
    records = ex.fetch_annonces(D, api.client(), page_size=5, sleep=Sleeps())
    assert records == SAMPLE
    offsets = [c.url.params.get("offset", "0") for c in api.calls]
    assert offsets == ["0", "5", "10"]


def test_une_seule_page_si_tout_tient():
    api = FakeAPI(SAMPLE)
    ex.fetch_annonces(D, api.client(), page_size=100, sleep=Sleeps())
    assert len(api.calls) == 1


def test_bascule_sur_export_au_dela_de_la_fenetre():
    api = FakeAPI(SAMPLE)
    records = ex.fetch_annonces(D, api.client(), page_size=5, max_window=10, sleep=Sleeps())
    assert records == SAMPLE
    assert api.calls[-1].url.path.endswith("/exports/json")
    assert len(api.calls) == 2  # first page (for total_count) + export


def test_pagination_incoherente_detectee():
    class ShrinkingAPI(FakeAPI):
        # The dataset "loses" a notice between two pages.
        def __call__(self, request):
            response = super().__call__(request)
            if request.url.params.get("offset") == "10":
                return httpx.Response(200, json={"total_count": 12, "results": SAMPLE[10:11]})
            return response

    with pytest.raises(ex.IncoherentExtractionError, match="11 annonces"):
        ex.fetch_annonces(D, ShrinkingAPI(SAMPLE).client(), page_size=5, sleep=Sleeps())


# --- retries -------------------------------------------------------------------


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_retry_sur_429_et_5xx(status):
    sleeps = Sleeps()
    api = FakeAPI(SAMPLE, failures=[httpx.Response(status), httpx.Response(status)])
    assert ex.fetch_annonces(D, api.client(), sleep=sleeps) == SAMPLE
    assert sleeps == [2.0, 4.0]  # exponential backoff


def test_retry_sur_erreur_reseau():
    sleeps = Sleeps()
    api = FakeAPI(SAMPLE, failures=[httpx.ConnectError("refusé"), httpx.ReadTimeout("lent")])
    assert ex.fetch_annonces(D, api.client(), sleep=sleeps) == SAMPLE
    assert len(sleeps) == 2


def test_retry_after_respecte():
    sleeps = Sleeps()
    api = FakeAPI(SAMPLE, failures=[httpx.Response(429, headers={"Retry-After": "7"})])
    ex.fetch_annonces(D, api.client(), sleep=sleeps)
    assert sleeps == [7.0]


def test_pas_de_retry_sur_400():
    sleeps = Sleeps()
    api = FakeAPI(SAMPLE, failures=[httpx.Response(400, json={"error_code": "ODSQLError"})])
    with pytest.raises(ex.BodaccAPIError, match="HTTP 400"):
        ex.fetch_annonces(D, api.client(), sleep=sleeps)
    assert sleeps == []
    assert len(api.calls) == 1


def test_abandon_apres_le_nombre_max_de_tentatives():
    sleeps = Sleeps()
    api = FakeAPI(SAMPLE, failures=[httpx.Response(503)] * ex.MAX_ATTEMPTS)
    with pytest.raises(ex.BodaccAPIError, match="5 tentatives"):
        ex.fetch_annonces(D, api.client(), sleep=sleeps)
    assert sleeps == [2.0, 4.0, 8.0, 16.0]


def test_backoff_plafonne():
    assert ex._backoff(10) == ex.BACKOFF_MAX_SECONDS


# --- writing to raw/ -------------------------------------------------------------


def test_extract_ecrit_la_partition_raw():
    storage = FakeStorage()
    key = ex.extract(D, storage=storage, client=FakeAPI(SAMPLE).client(), sleep=Sleeps())
    assert key == "raw/bodacc/date=2026-11-05/annonces.json"
    data, content_type = storage.objects[key]
    assert content_type == "application/json"
    assert json.loads(data.decode("utf-8")) == SAMPLE  # raw content, unchanged
    assert "Procédures collectives".encode() in data  # readable UTF-8, no \u escapes


def test_extract_jour_sans_annonce_ecrit_un_tableau_vide():
    storage = FakeStorage()
    key = ex.extract(D, storage=storage, client=FakeAPI([]).client(), sleep=Sleeps())
    assert storage.objects[key][0] == b"[]"


def test_extract_idempotent():
    storage = FakeStorage()
    for _ in range(2):
        ex.extract(D, storage=storage, client=FakeAPI(SAMPLE).client(), sleep=Sleeps())
    assert list(storage.objects) == ["raw/bodacc/date=2026-11-05/annonces.json"]
