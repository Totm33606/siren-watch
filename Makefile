.PHONY: up down ps logs test test-integration test-dags explore lint format backfill

COMPOSE := docker compose
DAG_ID := bodacc_daily
# Rejouer les runs déjà réussis lors d'un backfill : none | failed | completed.
REPROCESS ?= none

# Sur une machine vierge, .env n'existe pas : on le crée depuis le modèle, en générant
# des secrets propres à ce poste (le modèle versionné ne contient que des valeurs fictives).
# Make ne refait pas cette copie si .env existe déjà (cible fichier).
#  - JWT : 32 octets aléatoires en hexadécimal.
#  - Fernet : 32 octets en base64 « URL-safe » (+/ remplacés par -_), le format attendu
#    par Airflow. openssl est présent sous Linux, macOS et Git Bash.
# On écrit via sed > fichier plutôt que `sed -i`, dont la syntaxe diffère entre GNU et macOS.
ENV_FILE := .env
$(ENV_FILE):
	@jwt=$$(openssl rand -hex 32); \
	fernet=$$(openssl rand -base64 32 | tr '+/' '-_'); \
	sed -e "s|^AIRFLOW__API_AUTH__JWT_SECRET=.*|AIRFLOW__API_AUTH__JWT_SECRET=$$jwt|" \
	    -e "s|^FERNET_KEY=.*|FERNET_KEY=$$fernet|" $(ENV_FILE).example > $@
	@echo "$@ créé depuis $@.example, avec un secret JWT et une clé Fernet générés."

# --build : reconstruit l'image Airflow si le Dockerfile ou src/ ont changé (sinon,
# le cache Docker rend l'étape quasi instantanée).
# --wait : rend la main quand les services sont "healthy" (et les one-shot terminés),
# et échoue sinon. Pratique pour enchaîner des commandes après `make up`.
up: .env
	$(COMPOSE) up -d --build --wait
	@echo "Airflow : http://localhost:8080  |  Console MinIO : http://localhost:9001"

# Sans -v : la base Airflow et les données MinIO (volumes nommés) sont conservées.
down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs -f --tail=100

# Tests unitaires, sans réseau (les tests marqués `integration` sont exclus).
test:
	uv run pytest

# Tests contre le MinIO local (nécessite `make up`). --env-file charge .env sur le poste,
# où l'endpoint est http://localhost:9000.
test-integration: .env
	uv run --env-file .env pytest -m integration -v

# Test d'intégrité du DAG, exécuté DANS l'image Airflow (Airflow ne s'installe pas
# nativement sous Windows). tests/ est monté en lecture seule par le service airflow-cli
# (docker-compose.yml) ; pytest est installé à la volée dans ce conteneur jetable (--rm),
# sans alourdir l'image de production.
test-dags:
	$(COMPOSE) run --rm airflow-cli \
		bash -c "pip install --quiet --disable-pip-version-check pytest && python -m pytest -p no:cacheprovider -v /opt/airflow/tests/test_dag_integrity.py"

# Backfill : Airflow 3 crée un run par date, exécuté par le scheduler. Un backfill a sa
# propre limite de concurrence (--max-active-runs), distincte de celle du DAG : on la
# fixe à 3 pour ne pas saturer l'API. REPROCESS=completed rejoue aussi les dates réussies.
# END est inclusif : --to-date YYYY-MM-DD seul vaudrait minuit et exclurait le run de
# END (déclenché à 10:00 UTC), d'où le T23:59:59.
# `airflow backfill create` ne fait qu'enregistrer des runs en base : c'est le scheduler
# qui les exécute, et seulement si le DAG est actif. Sinon les runs resteraient en
# attente sans aucune erreur. On vérifie donc ces deux points AVANT de créer les runs.
backfill:
	@test -n "$(START)" && test -n "$(END)" || \
		(echo "Usage : make backfill START=YYYY-MM-DD END=YYYY-MM-DD [REPROCESS=none|failed|completed]" && exit 1)
	@$(COMPOSE) ps --status running --services 2>/dev/null | grep -qx airflow-scheduler || \
		(echo "Le scheduler ne tourne pas : les runs resteraient en attente. Lancez d'abord : make up" && exit 1)
	@paused=$$($(COMPOSE) exec -T airflow-scheduler airflow dags details $(DAG_ID) -o plain 2>/dev/null \
		| awk '$$1 == "is_paused" {print $$2}'); \
	if [ "$$paused" = "True" ]; then \
		echo "Le DAG $(DAG_ID) est en pause : le scheduler n'exécuterait pas les runs."; \
		echo "Activez-le d'abord : docker compose exec airflow-scheduler airflow dags unpause $(DAG_ID)"; \
		exit 1; \
	elif [ "$$paused" != "False" ]; then \
		echo "DAG $(DAG_ID) introuvable : vérifiez qu'il apparaît dans l'interface (erreur d'import ?)."; \
		exit 1; \
	fi
	$(COMPOSE) run --rm airflow-cli airflow backfill create --dag-id $(DAG_ID) \
		--from-date $(START) --to-date $(END)T23:59:59 --reprocess-behavior $(REPROCESS) \
		--max-active-runs 3

# Requêtes DuckDB sur clean/ depuis le poste (nécessite make up). .env fournit les
# identifiants S3, lus par le secret credential_chain du fichier SQL.
explore: .env
	uv run --env-file .env python notebooks/run_sql.py notebooks/explore.sql

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .
