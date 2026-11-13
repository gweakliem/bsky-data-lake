# syntax=docker/dockerfile:1
# Build from this directory, with embed/ and enrich/ (path dependencies) as named contexts:
#   docker buildx build --build-context embed=../embed --build-context enrich=../enrich -t bsky-search .
# No context is the repo root, so the repo's .env can't end up in the image.
# One image for both workloads: the API by default, `bsky-index` for the CronJob.

FROM python:3.13-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# uv.lock refers to ../embed and ../enrich, which are /embed and /enrich here.
# Only their package files are copied (eval data and tests stay out).
COPY --from=enrich pyproject.toml README.md /enrich/
COPY --from=enrich src /enrich/src
COPY --from=embed pyproject.toml /embed/
COPY --from=embed src /embed/src

# Dependencies first, so source changes don't invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --no-editable

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --system --uid 10001 --create-home search
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER 10001
EXPOSE 8000
# Exec form, so the process is PID 1 and gets SIGTERM directly. The CronJob runs `bsky-index` instead.
CMD ["bsky-search", "serve", "--host", "0.0.0.0", "--port", "8000"]
