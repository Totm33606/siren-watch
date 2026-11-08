"""Configuration read from environment variables.

Everything comes from the environment: `.env` locally (loaded by Docker Compose in the
containers, or by `uv run --env-file .env` on the host), runtime variables in
production. No default value holds a secret.

S3 credentials are deliberately not read here: boto3 finds them through its standard
chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, profile, IAM role). Moving from MinIO
to real S3 therefore only means changing variables.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

ENV_S3_BUCKET = "SIRENWATCH_S3_BUCKET"
ENV_S3_ENDPOINT_URL = "SIRENWATCH_S3_ENDPOINT_URL"
ENV_AWS_REGION = "AWS_DEFAULT_REGION"

DEFAULT_AWS_REGION = "us-east-1"


class ConfigError(RuntimeError):
    """Missing or invalid configuration."""


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    # None = default AWS endpoint; a URL = S3-compatible storage (MinIO).
    s3_endpoint_url: str | None
    aws_region: str


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build the settings from `env` (default: `os.environ`).

    Passing an explicit mapping makes the function testable without touching the
    process environment.
    """
    env = os.environ if env is None else env

    bucket = _clean(env.get(ENV_S3_BUCKET))
    if bucket is None:
        raise ConfigError(f"Variable d'environnement manquante : {ENV_S3_BUCKET}")

    endpoint_url = _clean(env.get(ENV_S3_ENDPOINT_URL))
    if endpoint_url is not None and not endpoint_url.startswith(("http://", "https://")):
        raise ConfigError(
            f"{ENV_S3_ENDPOINT_URL} doit commencer par http:// ou https:// "
            f"(reçu : {endpoint_url!r})"
        )

    region = _clean(env.get(ENV_AWS_REGION)) or DEFAULT_AWS_REGION

    return Settings(s3_bucket=bucket, s3_endpoint_url=endpoint_url, aws_region=region)


def _clean(value: str | None) -> str | None:
    """Treat an empty variable (`VAR=` in .env) as missing."""
    if value is None:
        return None
    value = value.strip()
    return value or None
