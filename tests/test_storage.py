"""Tests de storage.py sur un S3 simulé en mémoire (moto) : aucun appel réseau."""

import datetime

import boto3
import pytest
from moto import mock_aws

from sirenwatch.config import Settings
from sirenwatch.storage import Storage, make_client, partition_key

BUCKET = "sirenwatch-test"
D = datetime.date(2026, 11, 5)


@pytest.fixture
def storage(monkeypatch):
    # Faux identifiants : moto intercepte les appels, mais boto3 exige des clés.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield Storage(client, BUCKET)


# --- partition_key -----------------------------------------------------------


def test_partition_key_format_hive():
    assert partition_key("raw", "bodacc", D, "annonces.json") == (
        "raw/bodacc/date=2026-11-05/annonces.json"
    )
    assert partition_key("clean", "bodacc", D, "annonces.parquet") == (
        "clean/bodacc/date=2026-11-05/annonces.parquet"
    )


def test_partition_key_zone_inconnue():
    with pytest.raises(ValueError, match="Zone inconnue"):
        partition_key("staging", "bodacc", D, "annonces.json")


@pytest.mark.parametrize("filename", ["", "a/b.json"])
def test_partition_key_nom_de_fichier_invalide(filename):
    with pytest.raises(ValueError):
        partition_key("raw", "bodacc", D, filename)


def test_partition_key_refuse_un_datetime():
    # Un datetime porte une heure : la partition doit dépendre de la date seule.
    with pytest.raises(TypeError):
        partition_key("raw", "bodacc", datetime.datetime(2026, 11, 5, 8, 0), "annonces.json")


# --- lecture / écriture ------------------------------------------------------


def test_ecrire_puis_relire(storage):
    key = partition_key("raw", "bodacc", D, "annonces.json")
    payload = '[{"id": "A1", "familleavis_lib": "Procédures collectives"}]'.encode()
    storage.write_bytes(key, payload, "application/json")
    assert storage.read_bytes(key) == payload


def test_reecrire_remplace_sans_doublon(storage):
    # Idempotence : relancer une date réécrit la même clé.
    key = partition_key("raw", "bodacc", D, "annonces.json")
    storage.write_bytes(key, b"v1")
    storage.write_bytes(key, b"v2")
    assert storage.read_bytes(key) == b"v2"
    assert storage.list_keys("raw/") == [key]


def test_objet_vide_autorise(storage):
    # Une date sans annonce doit pouvoir produire une partition valide.
    key = partition_key("raw", "bodacc", D, "annonces.json")
    storage.write_bytes(key, b"")
    assert storage.read_bytes(key) == b""


def test_content_type_conserve(storage):
    storage.write_bytes("raw/x.json", b"[]", "application/json")
    head = storage._client.head_object(Bucket=BUCKET, Key="raw/x.json")
    assert head["ContentType"] == "application/json"


def test_lire_cle_absente(storage):
    with pytest.raises(FileNotFoundError, match=r"raw/absent\.json"):
        storage.read_bytes("raw/absent.json")


def test_exists(storage):
    storage.write_bytes("raw/present.json", b"[]")
    assert storage.exists("raw/present.json")
    assert not storage.exists("raw/absent.json")


def test_list_keys_filtre_par_prefixe_et_trie(storage):
    for day in (6, 4, 5):
        storage.write_bytes(
            partition_key("raw", "bodacc", datetime.date(2026, 11, day), "a.json"), b"[]"
        )
    storage.write_bytes(partition_key("clean", "bodacc", D, "a.parquet"), b"")
    assert storage.list_keys("raw/bodacc/") == [
        "raw/bodacc/date=2026-11-04/a.json",
        "raw/bodacc/date=2026-11-05/a.json",
        "raw/bodacc/date=2026-11-06/a.json",
    ]
    assert storage.list_keys("rien/") == []


def test_delete(storage):
    storage.write_bytes("raw/x.json", b"[]")
    storage.delete("raw/x.json")
    assert not storage.exists("raw/x.json")
    storage.delete("raw/x.json")  # supprimer une clé absente n'est pas une erreur


# --- make_client ---------------------------------------------------------------


def test_client_minio_adressage_path():
    settings = Settings("sirenwatch", "http://minio:9000", "us-east-1")
    client = make_client(settings)
    assert client.meta.endpoint_url == "http://minio:9000"
    assert client.meta.config.s3["addressing_style"] == "path"
    assert client.meta.region_name == "us-east-1"


def test_client_s3_reel_endpoint_aws():
    settings = Settings("sirenwatch", None, "eu-west-3")
    client = make_client(settings)
    assert client.meta.endpoint_url == "https://s3.eu-west-3.amazonaws.com"
    assert "addressing_style" not in (client.meta.config.s3 or {})


def test_from_settings():
    storage = Storage.from_settings(Settings("sirenwatch", "http://minio:9000", "us-east-1"))
    assert storage.bucket == "sirenwatch"
