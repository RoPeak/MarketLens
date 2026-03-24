# Multi-stage Dockerfile for MarketLens
#
# Stage 1 (deps): install Python dependencies into a venv
# Stage 2 (runtime): copy only the venv + source — no build tools in the final image
#
# Usage:
#   docker build -t marketlens .
#   docker compose up --build

# ---------------------------------------------------------------------------
# Stage 1 — dependency builder
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS deps

WORKDIR /build

# System build dependencies (needed for some C extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create an isolated venv so we can copy just the venv in stage 2
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install production deps only (no [dev] extras)
COPY pyproject.toml .
COPY README.md .
# Dummy package init so pip can find the package during editable install
RUN mkdir -p marketlens && touch marketlens/__init__.py
RUN pip install --upgrade pip && pip install .

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime system dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    # dbt needs git for package resolution
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy venv from builder
COPY --from=deps /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source
COPY marketlens/ ./marketlens/
COPY dashboard/ ./dashboard/
COPY dbt/ ./dbt/
COPY scripts/ ./scripts/
COPY pyproject.toml README.md ./

# Create data directory (mounted as a volume in compose)
RUN mkdir -p data

# Streamlit config: disable telemetry and browser auto-open
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV STREAMLIT_SERVER_HEADLESS=true

# Default command — override in docker-compose.yml per service
CMD ["python", "marketlens/flows/pipeline_flow.py"]
