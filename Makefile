.PHONY: install lint format test ingest transform dbt-run dbt-test dbt-docs dashboard pipeline

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

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

# --- Dashboard ---

dashboard:
	streamlit run dashboard/app.py --server.port 8501

# --- Full pipeline (manual one-shot) ---

pipeline: ingest transform dbt-run dbt-test
	@echo "Pipeline complete."
