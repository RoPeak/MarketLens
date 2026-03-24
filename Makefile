.PHONY: install lint format test ingest transform dbt-run dbt-test dbt-docs dashboard flows pipeline

# --- Setup ---

install:
	pip install -e ".[dev]"

# --- Code quality ---

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

# --- Tests ---

test:
	pytest tests/ -v --tb=short -m "not network"

test-all:
	pytest tests/ -v --tb=short

# --- Pipeline ---

bootstrap:
	python scripts/bootstrap_db.py

ingest:
	python scripts/run_ingest.py

transform:
	python scripts/run_transforms.py

# --- dbt ---
# dbt.exe location varies; fall back to PATH if not found at the pip install location
DBT ?= dbt

dbt-deps:
	cd dbt && $(DBT) deps

dbt-run:
	cd dbt && $(DBT) run

dbt-test:
	cd dbt && $(DBT) test

dbt-docs:
	cd dbt && $(DBT) docs generate && $(DBT) docs serve

# --- Dashboard ---

dashboard:
	streamlit run dashboard/app.py --server.port 8501

# --- Prefect orchestration ---

flows:
	python marketlens/flows/pipeline_flow.py

# --- Full pipeline (manual one-shot) ---

pipeline: ingest transform dbt-run dbt-test
	@echo "Pipeline complete."
