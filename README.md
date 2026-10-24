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

## Temporary Tron recovery

`tron-recovery` is a one-shot command independent of Datalens and normal enabled
chains. It scans only ORMP MessageAccepted/MessageAssigned and Msgport MessageSent
from Tron Solidity-confirmed blocks, then exits at the confirmed head sampled on
startup. It does not submit blockchain transactions.

```bash
# Preview the supplied redemption transaction; no database connection is opened.
ORMPINDEXER_TRON_RPC_URL=https://api.trongrid.io \
  cargo run -- tron-recovery --from-block 87383828 --to-block 87383930 --dry-run

# Set ORMPINDEXER_DATABASE_URL using the existing deployment secret first.
ormpindexer tron-recovery --from-block 87383828
```

Set `ORMPINDEXER_TRON_RPC_URL` to a Tron HTTP API base or `/jsonrpc` URL;
`ORMPINDEXER_TRON_API_KEY` is optional. The provider must support both JSON-RPC
`eth_getLogs`/block/transaction methods and `/walletsolidity/getnowblock`.
`--batch-size` defaults to 500 blocks and shrinks failed ranges automatically.
`--to-block` is inclusive and must not exceed the confirmed head.

Writes use existing indexer migrations and tables. A dedicated checkpoint
`tron.ormp-recovery.v1` resumes later invocations; `--from-block` only initializes
that checkpoint. Events and progress commit in the same transaction. A PostgreSQL
session advisory lock prevents overlapping invocations. Missing source data for
the latest already-imported Tron root stops recovery for manual investigation.
Dry-run always scans the explicit range and ignores saved progress.

The temporary Kubernetes CronJob runs this command every five minutes under
`apps/ringdao/ormpindexer` in devows/infra. To retire it, suspend/remove that
CronJob, then remove `src/tron_recovery/`, its fixture directory, and the command
wiring in `src/main.rs` and `src/lib.rs`. Keep recovered business rows. No normal
Tron chain configuration or Datalens service needs to be restored.

Recovery tests use a disposable PostgreSQL database selected only through
`ORMPINDEXER_RECOVERY_TEST_DATABASE_URL`; the database test truncates its tables.
