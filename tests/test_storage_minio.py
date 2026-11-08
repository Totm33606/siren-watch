"""Integration test against the real MinIO started by `make up`.

Excluded from `make test` (`integration` marker). Run it with `make test-integration`,
which loads `.env` for the endpoint and the credentials.
"""

import os
import uuid

import pytest

from sirenwatch.config import load_settings
from sirenwatch.storage import Storage

pytestmark = pytest.mark.integration


@pytest.fixture
def storage():
    if not os.environ.get("SIRENWATCH_S3_ENDPOINT_URL"):
        pytest.skip("SIRENWATCH_S3_ENDPOINT_URL absent : lancer via `make test-integration`")
    return Storage.from_settings(load_settings())


def test_aller_retour_minio(storage):
    # Dedicated prefix and random name: never touches the raw/ and clean/ zones.
    key = f"_tests/roundtrip-{uuid.uuid4().hex}.json"
    payload = '{"message": "procédure collective"}'.encode()
    try:
        storage.write_bytes(key, payload, "application/json")
        assert storage.exists(key)
        assert storage.read_bytes(key) == payload
        assert key in storage.list_keys("_tests/")
    finally:
        storage.delete(key)
    assert not storage.exists(key)
