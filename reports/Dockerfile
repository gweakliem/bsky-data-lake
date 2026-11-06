# syntax=docker/dockerfile:1
# Build from the repo root, with silver/ as a named context for the view SQL:
#   docker buildx build --build-context silver=silver -t bsky-reports reports
# The main context is reports/ only, so the repo's .env can't end up in the image.

FROM node:22.21.1-bookworm-slim AS node

FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv
# Node and npm, for Observable Framework.
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && useradd --system --uid 10001 --create-home reports
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 HOME=/home/reports UV_CACHE_DIR=/tmp/uv-cache

# lake.py renders posts_current from silver's SQL with silver's views.py, at these paths.
COPY --from=silver sql /app/silver/sql
COPY --from=silver src/bsky_silver/views.py /app/silver/src/bsky_silver/views.py

WORKDIR /app/reports
COPY --chown=10001 package.json package-lock.json pyproject.toml uv.lock ./
RUN mkdir -p /app/.venv /home/reports && chown -R 10001 /app/.venv /app/reports /home/reports
USER 10001
RUN npm ci --no-audit --no-fund && uv sync --frozen --no-dev
# The environment is complete from here on; `uv run` (including Framework's loaders) uses it as is.
ENV UV_NO_SYNC=1

COPY --chown=10001 . .
# DuckDB's extensions (iceberg needs avro), so builds don't download them; then a fixture build,
# which checks the image and caches Framework's npm imports (d3, Plot) in
# src/.observablehq/cache/_npm, so in-cluster builds don't fetch them either.
RUN uv run --frozen --no-dev python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs; INSTALL avro; INSTALL iceberg')" \
 && BSKY_REPORTS_FIXTURE=1 npm run build && rm -rf dist src/.observablehq/cache/data

# Exec form, so the build is PID 1 and gets SIGTERM directly. The web pod's
# sidecar runs `python lib/sync_site.py` from this image instead.
ENTRYPOINT ["uv", "run", "--frozen", "--no-dev", "python", "lib/publish.py"]
