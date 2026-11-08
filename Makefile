.PHONY: up down ps logs test test-integration test-dags explore lint format backfill clean

# On Windows, GNU make runs recipes with cmd.exe when sh.exe is not on the PATH (e.g. in
# PowerShell), which breaks the POSIX recipes below. Use Git for Windows' sh instead,
# located from `git --exec-path` (…/Git/mingw64/libexec/git-core → …/Git). Git\bin\sh.exe,
# not Git\usr\bin\sh.exe, puts /usr/bin and /mingw64/bin (sed, awk, grep, openssl) on its
# PATH. The `bash` on the Windows PATH is WSL's, another Linux machine: do not use it.
# make also runs lines without shell metacharacters (no |, $, >…) directly, without sh,
# looking the program up on the Windows PATH: Git's tools are added to it (";"-separated).
ifeq ($(OS),Windows_NT)
GIT_ROOT := $(subst /mingw64/libexec/git-core,,$(shell git --exec-path))
ifeq ($(GIT_ROOT),)
$(error Git pour Windows est introuvable : il fournit le shell utilisé par ce Makefile)
endif
SHELL := $(GIT_ROOT)/bin/sh.exe
export PATH := $(GIT_ROOT)/usr/bin;$(GIT_ROOT)/mingw64/bin;$(PATH)
endif

COMPOSE := docker compose
DAG_ID := bodacc_daily
# Backfill policy for dates that already have a run: none | failed | completed.
REPROCESS ?= none

# On a fresh machine .env does not exist: create it from the template, with secrets
# generated for this machine (the committed template only holds placeholders).
# File target: make never overwrites an existing .env.
#  - JWT secret: 32 random bytes, hex-encoded.
#  - Fernet key: 32 random bytes in URL-safe base64 (+/ become -_), as Airflow expects.
# `sed … > file` rather than `sed -i`, whose syntax differs between GNU and macOS.
ENV_FILE := .env
$(ENV_FILE):
	@jwt=$$(openssl rand -hex 32); \
	fernet=$$(openssl rand -base64 32 | tr '+/' '-_'); \
	sed -e "s|^AIRFLOW__API_AUTH__JWT_SECRET=.*|AIRFLOW__API_AUTH__JWT_SECRET=$$jwt|" \
	    -e "s|^FERNET_KEY=.*|FERNET_KEY=$$fernet|" $(ENV_FILE).example > $@
	@echo "$@ créé depuis $@.example, avec un secret JWT et une clé Fernet générés."

# --build: rebuild the Airflow image if the Dockerfile or src/ changed (cached otherwise).
# --wait: return once services are healthy and one-shot services have exited; fail
# otherwise. Handy for chaining commands after `make up`.
up: .env
	$(COMPOSE) up -d --build --wait
	@echo "Airflow : http://localhost:8080  |  Console MinIO : http://localhost:9001"

# Without -v: the Airflow database and the MinIO data (named volumes) are kept.
down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs -f --tail=100

# Unit tests, no network (tests marked `integration` are excluded).
test:
	uv run pytest

# Tests against the local MinIO (requires `make up`). --env-file loads .env, where the
# endpoint seen from this machine is http://localhost:9000.
test-integration: .env
	uv run --env-file .env pytest -m integration -v

# DAG integrity tests, run INSIDE the Airflow image (Airflow does not install natively
# on Windows). tests/ is mounted read-only by the airflow-cli service; pytest is installed
# on the fly in this throwaway container (--rm), keeping it out of the production image.
test-dags:
	$(COMPOSE) run --rm airflow-cli \
		bash -c "pip install --quiet --disable-pip-version-check pytest && python -m pytest -p no:cacheprovider -v /opt/airflow/tests/test_dag_integrity.py"

# Backfill: Airflow 3 creates one run per date, executed by the scheduler.
#  - --max-active-runs: backfill-specific concurrency limit (the DAG's own limit does not
#    apply), set to 3 to spare the API. REPROCESS=completed also re-runs successful dates.
#  - END is inclusive: a bare --to-date means midnight and would exclude the run of END
#    (triggered at 10:00 UTC), hence T23:59:59.
# `airflow backfill create` only records runs in the database: the scheduler executes
# them, and only if the DAG is active. Otherwise they stay queued without any error, so
# both conditions are checked BEFORE creating the runs.
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

# DuckDB queries on clean/ from this machine (requires make up). .env provides the S3
# credentials, read by the credential_chain secret of the SQL file.
explore: .env
	uv run --env-file .env python notebooks/run_sql.py notebooks/explore.sql

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

# Remove the caches and build artefacts of the dev tools (Python, pytest, ruff, mypy,
# coverage, build): they are regenerated on demand. Leaves untouched .env, .venv (uv
# environment, slow to recreate), logs/ and config/ (used by the Airflow containers) and
# the data (Docker volumes: see `docker compose down -v`).
# find skips .venv, .git and logs/ (-prune); `-exec … +` runs rm once, at the end.
clean:
	find . \( -path ./.venv -o -path ./.git -o -path ./logs \) -prune \
		-o -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage coverage.xml htmlcov dist build
	@echo "Caches et artefacts supprimés."
