# SirenWatch Airflow image: the official image + the `sirenwatch` package.
#
# The DAG (dags/, mounted as a volume) only imports `sirenwatch`: all the logic lives
# in the package, installed here. After changing src/, rebuild the image: `make up`
# does it (docker compose up --build).

ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.13
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Official constraints file: the exact versions tested with this Airflow release. A
# sirenwatch dependency that conflicts with Airflow fails the build here, not at
# runtime. boto3, httpx and pyarrow are already in the image: pip sees that they
# satisfy pyproject.toml and does not upgrade them.
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Minimal copy (see .dockerignore): this layer is only rebuilt when the package code or
# metadata change.
COPY --chown=airflow:0 pyproject.toml README.md LICENSE /opt/sirenwatch/
COPY --chown=airflow:0 src /opt/sirenwatch/src

# Stay the image's `airflow` user (not root), as the documentation recommends.
# "apache-airflow==" is repeated so that pip cannot change the Airflow version.
RUN pip install --no-cache-dir \
        --constraint "${CONSTRAINTS_URL}" \
        "apache-airflow==${AIRFLOW_VERSION}" \
        /opt/sirenwatch
