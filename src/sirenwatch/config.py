"""Lecture de la configuration depuis les variables d'environnement.

Tout passe par l'environnement : `.env` en local (chargé par Docker Compose dans les
conteneurs, ou par `uv run --env-file .env` sur le poste), variables du runtime en
production. Aucune valeur par défaut ne contient de secret.

Les identifiants S3 ne sont volontairement pas lus ici : boto3 les trouve tout seul
via sa chaîne standard (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, profil, rôle IAM).
Passer de MinIO à S3 réel ne demande donc que de changer des variables.
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
    """Configuration absente ou invalide."""


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    # None = endpoint AWS par défaut ; une URL = stockage compatible S3 (MinIO).
    s3_endpoint_url: str | None
    aws_region: str


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Construit les réglages à partir de `env` (par défaut : `os.environ`).

    Passer un dictionnaire explicite rend la fonction testable sans toucher à
    l'environnement du processus.
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
    """Traite une variable vide (`VAR=` dans .env) comme absente."""
    if value is None:
        return None
    value = value.strip()
    return value or None
