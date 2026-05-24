FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS base

WORKDIR /app

# Copy configuration files
COPY pyproject.toml uv.lock ./

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

# ── Create venv and pre-install CPU-only PyTorch ──────────────────────────────
RUN uv venv /app/.venv

# Install torch from the CPU wheel index inside the virtualenv so
# uv sync sees it satisfied and doesn't fetch the CUDA version.
RUN --mount=type=cache,target=/root/.cache/uv \
    .venv/bin/pip install \
    torch torchvision torchaudio \
    --index-url https://download.pytorch.org/whl/cpu

# ── Sync all project dependencies + prefect group ─────────────────────────────
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --group prefect

# ── Final stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim AS final

# Install system dependencies needed for Docling (OCR and PDF processing)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    poppler-utils \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the virtual environment and source code from base
COPY --from=base /app /app
COPY flows/ /app/flows/

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

EXPOSE 4200

# Start serving the flow (registers with the Prefect server)
CMD ["python", "/app/flows/arxiv_paper_ingestion.py"]
