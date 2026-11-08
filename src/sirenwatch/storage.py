"""Accès au stockage objet (S3 / MinIO), uniquement via boto3.

C'est le seul module qui parle au stockage. Le reste du code manipule des clés
(`raw/bodacc/date=2026-11-05/annonces.json`) et des octets, sans savoir s'il y a
MinIO ou S3 derrière.
"""

from __future__ import annotations

import datetime
from typing import Literal

import boto3
from botocore.client import BaseClient as S3Client
from botocore.config import Config
from botocore.exceptions import ClientError

from sirenwatch.config import Settings

Zone = Literal["raw", "clean"]
ZONES: tuple[Zone, ...] = ("raw", "clean")


def partition_key(zone: Zone, source: str, date: datetime.date, filename: str) -> str:
    """Clé d'un fichier dans une partition journalière.

    Format "Hive" `cle=valeur` : DuckDB (`hive_partitioning`), Spark ou Athena
    reconnaissent `date=…` comme une colonne virtuelle et peuvent ne lire que les
    partitions utiles.

    >>> partition_key("raw", "bodacc", datetime.date(2026, 11, 5), "annonces.json")
    'raw/bodacc/date=2026-11-05/annonces.json'
    """
    if zone not in ZONES:
        raise ValueError(f"Zone inconnue : {zone!r} (attendu : {', '.join(ZONES)})")
    for name, value in (("source", source), ("filename", filename)):
        if not value or "/" in value:
            raise ValueError(f"{name} invalide : {value!r}")
    # isoformat() et non str() : refuse explicitement un datetime (qui a une heure).
    if isinstance(date, datetime.datetime) or not isinstance(date, datetime.date):
        raise TypeError(f"date doit être un datetime.date, reçu {type(date).__name__}")
    return f"{zone}/{source}/date={date.isoformat()}/{filename}"


def make_client(settings: Settings) -> S3Client:
    """Client S3 configuré pour MinIO ou S3 réel selon `settings.s3_endpoint_url`."""
    s3_options = {}
    if settings.s3_endpoint_url:
        # MinIO : adressage "path" (http://minio:9000/sirenwatch/cle). En adressage
        # "virtual host" par défaut, boto3 viserait http://sirenwatch.minio:9000,
        # un nom DNS qui n'existe pas dans le réseau Docker.
        s3_options["addressing_style"] = "path"
    config = Config(
        region_name=settings.aws_region,
        # Retries de boto3 sur les erreurs réseau, 5xx et throttling (SlowDown).
        retries={"max_attempts": 5, "mode": "standard"},
        s3=s3_options,
    )
    return boto3.client("s3", endpoint_url=settings.s3_endpoint_url, config=config)


class Storage:
    """Lecture et écriture d'objets dans un bucket unique."""

    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client = client
        self.bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> Storage:
        return cls(make_client(settings), settings.s3_bucket)

    def write_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Écrit (ou remplace) l'objet `key`.

        Un PUT S3 est atomique : un lecteur voit l'ancienne ou la nouvelle version,
        jamais un fichier à moitié écrit. Réécrire la même clé est donc le moyen
        simple de rendre une tâche idempotente.
        """
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def read_bytes(self, key: str) -> bytes:
        """Lit l'objet `key`. Lève FileNotFoundError s'il n'existe pas."""
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in {"NoSuchKey", "404"}:
                raise FileNotFoundError(f"s3://{self.bucket}/{key}") from exc
            raise
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in {"NoSuchKey", "404"}:
                return False
            raise
        return True

    def list_keys(self, prefix: str = "") -> list[str]:
        """Clés commençant par `prefix`, triées. Gère la pagination (1000 clés par page)."""
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def delete(self, key: str) -> None:
        """Supprime `key` (sans erreur si la clé n'existe pas, comme S3)."""
        self._client.delete_object(Bucket=self.bucket, Key=key)


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))
