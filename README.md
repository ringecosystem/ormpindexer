# ORMP Indexer

This repository is the Rust indexer for ORMP events read from the datalens REST API.

Tron indexing was removed.

The previous TypeScript/Subsquid implementation was removed for HBX-457. Use git history if you need to inspect the old implementation.

## Build

```bash
cargo build
```

## Run

```bash
ORMPINDEXER_DATALENS_ENDPOINT=http://localhost:8080 cargo run -- run
```

The indexer sends `POST /v1/query` and `GET /v1/heads/{chain}` with `x-datalens-application` and, when `ORMPINDEXER_DATALENS_TOKEN` is set, `Authorization: Bearer`. Log queries request `block_timestamp` and `transaction_from`. A poll advances the checkpoint through `served.end` and retries the remainder on the next poll when `incomplete_reason` is set. `finalized` and `durable` use `durable_only` queries and the `finalized` head. `safe` uses `safe_to_latest` and the `safe` head. `latest` uses `latest_only` and the `latest` head.

Optional environment variables:

- `ORMPINDEXER_DATALENS_ENDPOINT`
- `ORMPINDEXER_DATALENS_APPLICATION`
- `ORMPINDEXER_DATALENS_TOKEN`
- `ORMPINDEXER_DATABASE_URL`
- `ORMPINDEXER_ENABLED_CHAINS`
- `ORMPINDEXER_FINALITY_MODE` (`finalized`, `durable`, `safe`, or `latest`)
- `ORMPINDEXER_DATALENS_HEAD_BUFFER_BLOCKS`
- `ORMPINDEXER_DATALENS_QUERY_MAX_ATTEMPTS`

## Migrate

```bash
cargo run -- migrate
```
