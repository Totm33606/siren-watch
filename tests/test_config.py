import pytest

from sirenwatch.config import DEFAULT_AWS_REGION, ConfigError, Settings, load_settings

MINIO_ENV = {
    "SIRENWATCH_S3_BUCKET": "sirenwatch",
    "SIRENWATCH_S3_ENDPOINT_URL": "http://localhost:9000",
    "AWS_DEFAULT_REGION": "eu-west-3",
}


def test_lit_toutes_les_variables():
    assert load_settings(MINIO_ENV) == Settings(
        s3_bucket="sirenwatch",
        s3_endpoint_url="http://localhost:9000",
        aws_region="eu-west-3",
    )


def test_bucket_obligatoire():
    env = {k: v for k, v in MINIO_ENV.items() if k != "SIRENWATCH_S3_BUCKET"}
    with pytest.raises(ConfigError, match="SIRENWATCH_S3_BUCKET"):
        load_settings(env)


def test_variable_vide_traitee_comme_absente():
    with pytest.raises(ConfigError, match="SIRENWATCH_S3_BUCKET"):
        load_settings({**MINIO_ENV, "SIRENWATCH_S3_BUCKET": "  "})


def test_sans_endpoint_on_vise_aws():
    # S3 réel : pas d'endpoint, boto3 utilise l'endpoint AWS de la région.
    env = {"SIRENWATCH_S3_BUCKET": "sirenwatch", "SIRENWATCH_S3_ENDPOINT_URL": ""}
    settings = load_settings(env)
    assert settings.s3_endpoint_url is None


def test_endpoint_sans_schema_refuse():
    with pytest.raises(ConfigError, match="http://"):
        load_settings({**MINIO_ENV, "SIRENWATCH_S3_ENDPOINT_URL": "minio:9000"})


def test_region_par_defaut():
    settings = load_settings({"SIRENWATCH_S3_BUCKET": "sirenwatch"})
    assert settings.aws_region == DEFAULT_AWS_REGION


def test_lit_os_environ_par_defaut(monkeypatch):
    for key, value in MINIO_ENV.items():
        monkeypatch.setenv(key, value)
    assert load_settings().s3_bucket == "sirenwatch"


def test_settings_immuables():
    settings = load_settings(MINIO_ENV)
    with pytest.raises(AttributeError):
        settings.s3_bucket = "autre"
