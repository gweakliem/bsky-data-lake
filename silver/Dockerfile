# syntax=docker/dockerfile:1
# Build from this directory: docker build -t bsky-silver .
# The context is silver/ only, so the repo's .env can't end up in the image.

FROM python:3.13-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependencies first, so source changes don't invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --system --uid 10001 --create-home silver
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER 10001
# Exec form, so the job is PID 1 and gets SIGTERM directly.
ENTRYPOINT ["bsky-silver"]
