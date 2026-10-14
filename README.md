# ORMP Indexer

This repository is the Rust indexer for ORMP events read from the datalens REST API.

Tron indexing was removed. Existing Tron rows stay in the database. `EventSource` was never stored, and the migrations have no source column. Rows with `chain_id = 728126428` in the ORMP tables, and the `tron.events` checkpoint row, are still served through GraphQL. A deployment that keeps `728126428` in `ORMPINDEXER_ENABLED_CHAINS` fails at startup because that chain is not configured.

The previous TypeScript/Subsquid implementation was removed for HBX-457. Use git history if you need to inspect the old implementation.

## Build

```bash
cargo build
```

## Run

```bash
ORMPINDEXER_DATALENS_ENDPOINT=http://localhost:8080 cargo run -- run
```

The indexer sends `POST /v1/query` and `GET /v1/heads/{chain}` with `x-datalens-application` and, when `ORMPINDEXER_DATALENS_TOKEN` is set, `Authorization: Bearer`. Log queries request every column the indexer stores, including `block_timestamp` and `transaction_from`. A poll advances the checkpoint through `served.end` and retries the remainder on the next poll when `incomplete_reason` is set. `finalized` uses `durable_only` and the `finalized` head. `durable` uses `durable_only` and the `safe` head, because that finality serves up to safe. `safe` and `latest` use `safe_to_latest`. `safe` follows the `safe` head and `latest` follows the `latest` head. `latest_only` is hot-segment only, so it is not used for checkpoint catch-up.

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
