# syntax=docker/dockerfile:1.7

FROM rust:1-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp /app/target/release/ormpindexer /tmp/ormpindexer

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system ormpindexer \
    && useradd --system --gid ormpindexer --home-dir /nonexistent --shell /usr/sbin/nologin ormpindexer

COPY --from=builder /tmp/ormpindexer /usr/local/bin/ormpindexer
COPY migrations /app/migrations

USER ormpindexer
WORKDIR /app

ENTRYPOINT ["/usr/local/bin/ormpindexer"]
