# syntax=docker/dockerfile:1
# The Jetstream ingestor. Build from the repo root: docker build -t bsky-ingest .
# .dockerignore is an allowlist, so .env and data/ never reach the context.

FROM rust:1.98-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
    && cp target/release/bsky-ingest /bsky-ingest

# distroless/cc has glibc and CA certificates and runs as a non-root user.
FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /bsky-ingest /usr/local/bin/bsky-ingest
ENTRYPOINT ["/usr/local/bin/bsky-ingest"]
