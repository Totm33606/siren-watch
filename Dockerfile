# Image Airflow de SirenWatch : l'image officielle + le paquet `sirenwatch`.
#
# Le DAG (dags/, monté en volume) n'importe que `sirenwatch` ; toute la logique
# vit dans le paquet, installé ici. Après une modification de src/, il faut
# reconstruire l'image : `make up` le fait (docker compose up --build).

ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.13
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Fichier de contraintes officiel : les versions exactes testées avec cette
# version d'Airflow. Si une dépendance de sirenwatch entrait en conflit avec
# Airflow, le build échouerait ici, et non au runtime. boto3, httpx et pyarrow
# sont déjà dans l'image : pip constate qu'ils satisfont pyproject.toml et ne
# les met pas à jour.
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Copie minimale (voir .dockerignore) : la couche n'est reconstruite que si le
# code ou les métadonnées du paquet changent.
COPY --chown=airflow:0 pyproject.toml README.md LICENSE /opt/sirenwatch/
COPY --chown=airflow:0 src /opt/sirenwatch/src

# On reste l'utilisateur `airflow` de l'image (pas root), comme le recommande la doc.
# "apache-airflow==" est répété pour que pip ne puisse pas changer la version d'Airflow.
RUN pip install --no-cache-dir \
        --constraint "${CONSTRAINTS_URL}" \
        "apache-airflow==${AIRFLOW_VERSION}" \
        /opt/sirenwatch
