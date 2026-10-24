use anyhow::{Context, ensure};
use sqlx::{PgPool, Postgres, pool::PoolConnection};

use crate::{
    checkpoint::CheckpointStore,
    database::{EventWriter, PostgresCheckpointStore, PostgresEventWriter},
    schema::LegacyOrmPEvent,
};

use super::rpc::CHAIN_ID;

pub(super) const DATASET: &str = "tron.ormp-recovery.v1";
const LOCK_NAMESPACE: i32 = 0x5452_4f4e;

pub(super) struct RecoveryStore {
    pool: PgPool,
    _lock: PoolConnection<Postgres>,
}

impl RecoveryStore {
    pub async fn lock(pool: PgPool) -> anyhow::Result<Option<Self>> {
        let mut connection = pool.acquire().await?;
        // Session locks must never return to the pool after failure or cancellation.
        connection.close_on_drop();
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
            .bind(LOCK_NAMESPACE)
            .bind(CHAIN_ID as i32)
            .fetch_one(&mut *connection)
            .await?;
        if !locked {
            return Ok(None);
        }
        // This task owns data backfill only. The normal indexer owns schema migrations.
        for table in [
            "ormp_message_accepted",
            "ormp_message_assigned",
            "msgport_message_sent",
            "ormp_indexer_checkpoint",
        ] {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(table)
                .fetch_one(&pool)
                .await?;
            ensure!(
                exists,
                "ORMP table {table} missing; run the owning indexer's migrations first"
            );
        }
        Ok(Some(Self {
            pool,
            _lock: connection,
        }))
    }

    pub async fn next_block(&self, start: u64) -> anyhow::Result<u64> {
        let store = PostgresCheckpointStore::new(self.pool.clone());
        Ok(store
            .read_or_create(CHAIN_ID, DATASET, start)
            .await?
            .next_block)
    }

    pub async fn check_preceding_message(&self) -> anyhow::Result<()> {
        let missing: Option<String> = sqlx::query_scalar(
            "SELECT imported.hash FROM (SELECT hash FROM ormp_hash_imported
               WHERE src_chain_id = $1::NUMERIC AND target_chain_id = 46
               ORDER BY msg_index DESC LIMIT 1) imported
             WHERE NOT EXISTS (SELECT 1 FROM ormp_message_accepted accepted
                 WHERE accepted.msg_hash = imported.hash AND accepted.from_chain_id = $1::NUMERIC)
             LIMIT 1",
        )
        .bind(CHAIN_ID.to_string())
        .fetch_optional(&self.pool)
        .await?;
        ensure!(
            missing.is_none(),
            "existing Tron imported root has no source message; recover the preceding source history before advancing: {}",
            missing.unwrap_or_default()
        );
        Ok(())
    }

    pub async fn commit(&self, events: &[LegacyOrmPEvent], next_block: u64) -> anyhow::Result<()> {
        let mut events = events.to_vec();
        for event in &mut events {
            self.check_existing(event).await?;
            if let LegacyOrmPEvent::MessageAccepted {
                metadata, msg_hash, ..
            } = event
            {
                // Retired Tron indexing could use a transaction-local event index.
                // Keep existing metadata when replaying an otherwise identical message.
                let existing: Option<(String, i32)> = sqlx::query_as(
                    "SELECT block_timestamp::TEXT, log_index FROM ormp_message_accepted WHERE id = $1",
                ).bind(msg_hash.as_str()).fetch_optional(&self.pool).await?;
                if let Some((timestamp, log_index)) = existing {
                    metadata.block_timestamp = timestamp.parse()?;
                    metadata.log_index = log_index;
                }
            }
            // Legacy Tron rows used another event-id format. Preserve that ID when
            // replaying the same assignment instead of creating a duplicate row.
            if let LegacyOrmPEvent::MessageAssigned {
                metadata,
                msg_hash,
                oracle,
                relayer,
                oracle_fee,
                relayer_fee,
                params,
            } = event
            {
                let ids: Vec<String> = sqlx::query_scalar(
                    "SELECT id FROM ormp_message_assigned WHERE chain_id = $1::NUMERIC
                     AND lower(transaction_hash) = lower($2) AND msg_hash = $3
                     AND oracle = $4 AND relayer = $5 AND oracle_fee = $6::NUMERIC
                     AND relayer_fee = $7::NUMERIC AND params = $8",
                )
                .bind(CHAIN_ID.to_string())
                .bind(&metadata.transaction_hash)
                .bind(msg_hash.as_str())
                .bind(oracle.as_str())
                .bind(relayer.as_str())
                .bind(oracle_fee.to_string())
                .bind(relayer_fee.to_string())
                .bind(params.as_str())
                .fetch_all(&self.pool)
                .await?;
                ensure!(
                    ids.len() <= 1,
                    "multiple existing Tron assignment rows; reconcile duplicates first"
                );
                if let Some(id) = ids.into_iter().next() {
                    metadata.id = id;
                }
            }
        }
        PostgresEventWriter::new(self.pool.clone())
            .write_events_and_advance_checkpoint(
                &PostgresCheckpointStore::new(self.pool.clone()),
                &events,
                &[],
                CHAIN_ID,
                DATASET,
                next_block,
            )
            .await
            .context("commit Tron events and recovery checkpoint")?;
        Ok(())
    }

    async fn check_existing(&self, event: &LegacyOrmPEvent) -> anyhow::Result<()> {
        if let LegacyOrmPEvent::MessageAccepted {
            metadata,
            msg_hash,
            channel,
            index,
            from_chain_id,
            from,
            to_chain_id,
            to,
            gas_limit,
            encoded,
        } = event
        {
            let conflicting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM ormp_message_accepted WHERE id = $1 AND
                  (chain_id <> $2::NUMERIC OR block_number <> $3::NUMERIC OR
                   lower(transaction_hash) <> lower($4) OR channel <> $5 OR \"index\" <> $6::NUMERIC OR
                   from_chain_id <> $7::NUMERIC OR \"from\" <> $8 OR to_chain_id <> $9::NUMERIC OR
                   \"to\" <> $10 OR gas_limit <> $11::NUMERIC OR encoded <> $12 OR msg_hash <> $1))",
            ).bind(msg_hash.as_str()).bind(metadata.chain_id.to_string()).bind(metadata.block_number.to_string())
                .bind(&metadata.transaction_hash).bind(channel).bind(index.to_string())
                .bind(from_chain_id.to_string()).bind(from).bind(to_chain_id.to_string())
                .bind(to).bind(gas_limit.to_string()).bind(encoded).fetch_one(&self.pool).await?;
            ensure!(
                !conflicting,
                "existing ORMP message conflicts with confirmed Tron event {msg_hash}"
            );
        }
        Ok(())
    }
}
