# ORMP Indexer

This repository is the new Rust starting point for the Datalens-backed ORMP indexer.

The previous TypeScript/Subsquid implementation was intentionally removed for HBX-457. Use git history if you need to inspect the old implementation.

## Build

```bash
cargo build
```

## Run

```bash
ORMPINDEXER_DATALENS_ENDPOINT=http://localhost:8080 cargo run -- run
```

Optional environment variables:

- `ORMPINDEXER_DATALENS_ENDPOINT`
- `ORMPINDEXER_DATALENS_APPLICATION`
- `ORMPINDEXER_DATALENS_TOKEN`
- `ORMPINDEXER_DATABASE_URL`

## Migrate

```bash
cargo run -- migrate
```

No database migrations are defined yet.
