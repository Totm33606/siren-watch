"""Object storage access (S3 / MinIO), through boto3 only.

This is the only module that talks to the storage. The rest of the code handles keys
(`raw/bodacc/date=2026-11-05/annonces.json`) and bytes, without knowing whether MinIO or
S3 is behind them.
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
    """Key of a file in a daily partition.

    Hive-style `key=value` format: DuckDB (`hive_partitioning`), Spark or Athena
    recognise `date=…` as a virtual column and can read only the partitions they need.

    >>> partition_key("raw", "bodacc", datetime.date(2026, 11, 5), "annonces.json")
    'raw/bodacc/date=2026-11-05/annonces.json'
    """
    if zone not in ZONES:
        raise ValueError(f"Zone inconnue : {zone!r} (attendu : {', '.join(ZONES)})")
    for name, value in (("source", source), ("filename", filename)):
        if not value or "/" in value:
            raise ValueError(f"{name} invalide : {value!r}")
    # A datetime is rejected explicitly: it carries a time of day, and the partition
    # must depend on the date only.
    if isinstance(date, datetime.datetime) or not isinstance(date, datetime.date):
        raise TypeError(f"date doit être un datetime.date, reçu {type(date).__name__}")
    return f"{zone}/{source}/date={date.isoformat()}/{filename}"


def make_client(settings: Settings) -> S3Client:
    """S3 client configured for MinIO or real S3, depending on `settings.s3_endpoint_url`."""
    s3_options = {}
    if settings.s3_endpoint_url:
        # MinIO: "path" addressing (http://minio:9000/sirenwatch/key). With the default
        # "virtual host" addressing, boto3 would target http://sirenwatch.minio:9000, a
        # host name that does not exist on the Docker network.
        s3_options["addressing_style"] = "path"
    config = Config(
        region_name=settings.aws_region,
        # boto3 retries on network errors, 5xx and throttling (SlowDown).
        retries={"max_attempts": 5, "mode": "standard"},
        s3=s3_options,
    )
    return boto3.client("s3", endpoint_url=settings.s3_endpoint_url, config=config)


class Storage:
    """Reads and writes objects in a single bucket."""

    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client = client
        self.bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> Storage:
        return cls(make_client(settings), settings.s3_bucket)

    def write_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Write (or replace) object `key`.

        An S3 PUT is atomic: a reader sees the old or the new version, never a
        half-written file. Rewriting the same key is therefore the simple way to make a
        task idempotent.
        """
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def read_bytes(self, key: str) -> bytes:
        """Read object `key`. Raise FileNotFoundError if it does not exist."""
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
        """Keys starting with `prefix`, sorted. Handles pagination (1,000 keys per page)."""
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def delete(self, key: str) -> None:
        """Delete `key` (no error if the key does not exist, as in S3)."""
        self._client.delete_object(Bucket=self.bucket, Key=key)


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))
