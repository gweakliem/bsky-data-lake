# syntax=docker/dockerfile:1
# Build from this directory, with enrich/ (a path dependency) as a named context:
#   docker buildx build --build-context enrich=../enrich -t bsky-embed .
# Neither context is the repo root, so the repo's .env can't end up in the image.
# Only the E2 job's dependencies are installed: the E1 harness's torch etc. are
# in the eval group, which --no-dev leaves out.

FROM python:3.13-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# uv.lock refers to enrich as ../enrich; /app/../enrich is /enrich. Only its
# package files are copied (its eval/ data stays out).
COPY --from=enrich pyproject.toml README.md /enrich/
COPY --from=enrich src /enrich/src

# Dependencies first, so source changes don't invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --no-editable

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --system --uid 10001 --create-home embed
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER 10001
# Exec form, so the job is PID 1 and gets SIGTERM directly.
ENTRYPOINT ["bsky-embed"]
