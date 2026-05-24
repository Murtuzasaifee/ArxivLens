FROM python:3.12-slim

# ── uv ────────────────────────────────────────────────────────────────────────
# Copy the uv binary from the official image — no pip install needed for uv itself.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# ── env ───────────────────────────────────────────────────────────────────────
ENV PYTHON_VERSION=3.12

# UV settings for Docker:
#   UV_LINK_MODE=copy   — Docker layers live on different filesystems; copy
#                          prevents "can't hardlink across filesystems" errors.
#   UV_COMPILE_BYTECODE — pre-compile .pyc files for faster container startup.
#   UV_PYTHON_DOWNLOADS — don't let uv download its own Python; we use the
#                          one from python:3.12-slim.
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# ── system deps ───────────────────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libpq-dev \
    poppler-utils \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── project requirements ──────────────────────────────────────────────────────
COPY requirements-prefect.txt /tmp/requirements-prefect.txt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
    -r /tmp/requirements-prefect.txt

# ── CPU-only PyTorch ──────────────────────────────────────────────────────────
# Install torch from the CPU wheel index BEFORE requirements so
# pip/uv's resolver sees torch already satisfied and never fetches the
# ~4 GB CUDA variant that PyPI serves on Linux by default.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
    torch torchvision torchaudio \
    --index-url https://download.pytorch.org/whl/cpu

COPY flows/ /app/flows/

# src/ is mounted as a volume at runtime (same pattern as Airflow)
ENV PYTHONPATH=/app

EXPOSE 4200

# Start serving the flow (registers with the Prefect server)
CMD ["python", "/app/flows/arxiv_paper_ingestion.py"]
