use anyhow::Context;
use sqlx::{PgPool, Postgres, Transaction, migrate::Migrator, postgres::PgPoolOptions};

use crate::{
    checkpoint::CheckpointStore,
    config::SecretString,
    schema::{
        AssignmentConfig, EventSource, LEGACY_B49E_ORACLE, LEGACY_B49E_ORACLE_FROM_BLOCK,
        LegacyOrmPEvent, MsgportMessageRecvRow, MsgportMessageSentRow, OrmpHashImportedRow,
        OrmpMessageAcceptedRow, OrmpMessageAssignedRow, OrmpMessageDispatchedRow,
        SignaturePubSignatureSubmittionRow,
    },
};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub async fn connect(database_url: &SecretString, max_connections: u32) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url.expose_secret())
        .await
        .context("connect to ORMP indexer Postgres")
}

pub async fn apply_migrations(pool: &PgPool) -> anyhow::Result<()> {
    MIGRATOR
        .run(pool)
        .await
        .context("apply ORMP indexer migrations")
}

#[derive(Clone)]
pub struct PostgresCheckpointStore {
    pool: PgPool,
}

impl PostgresCheckpointStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl CheckpointStore for PostgresCheckpointStore {
    async fn read_or_create(
        &self,
        chain_id: u64,
        dataset: &str,
        start_block: u64,
    ) -> anyhow::Result<crate::checkpoint::Checkpoint> {
        sqlx::query(
            "INSERT INTO ormp_indexer_checkpoint (chain_id, dataset, next_block, updated_at)
             VALUES ($1::NUMERIC, $2, $3::NUMERIC, now())
             ON CONFLICT (chain_id, dataset) DO NOTHING",
        )
        .bind(chain_id.to_string())
        .bind(dataset)
        .bind(start_block.to_string())
        .execute(&self.pool)
        .await?;

        let row = sqlx::query_as::<_, (String,)>(
            "SELECT next_block::TEXT
             FROM ormp_indexer_checkpoint
             WHERE chain_id = $1::NUMERIC AND dataset = $2",
        )
        .bind(chain_id.to_string())
        .bind(dataset)
        .fetch_one(&self.pool)
        .await?;

        Ok(crate::checkpoint::Checkpoint {
            chain_id,
            dataset: dataset.to_owned(),
            next_block: row.0.parse()?,
        })
    }

    async fn advance(&self, chain_id: u64, dataset: &str, next_block: u64) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE ormp_indexer_checkpoint
             SET next_block = $3::NUMERIC, updated_at = now()
             WHERE chain_id = $1::NUMERIC AND dataset = $2",
        )
        .bind(chain_id.to_string())
        .bind(dataset)
        .bind(next_block.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[allow(async_fn_in_trait)]
pub trait EventWriter {
    async fn write_events(&self, events: &[LegacyOrmPEvent]) -> anyhow::Result<usize>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DryRunEventWriter;

impl EventWriter for DryRunEventWriter {
    async fn write_events(&self, _events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        Ok(0)
    }
}

#[derive(Clone)]
pub struct PostgresEventWriter {
    pool: PgPool,
    assignment_config: AssignmentConfig,
}

impl PostgresEventWriter {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            assignment_config: AssignmentConfig::legacy_defaults(),
        }
    }

    pub fn with_assignment_config(pool: PgPool, assignment_config: AssignmentConfig) -> Self {
        Self {
            pool,
            assignment_config,
        }
    }
}

impl EventWriter for PostgresEventWriter {
    async fn write_events(&self, events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin ORMP legacy event write transaction")?;

        for event in events {
            write_legacy_event(&mut tx, event.clone(), &self.assignment_config).await?;
        }

        tx.commit()
            .await
            .context("commit ORMP legacy event write transaction")?;

        Ok(events.len())
    }
}

async fn write_legacy_event(
    tx: &mut Transaction<'_, Postgres>,
    event: LegacyOrmPEvent,
    assignment_config: &AssignmentConfig,
) -> anyhow::Result<()> {
    match event {
        LegacyOrmPEvent::HashImported { .. } => {
            insert_hash_imported(tx, OrmpHashImportedRow::from_event(event)).await
        }
        LegacyOrmPEvent::MessageAccepted { .. } => {
            let row = OrmpMessageAcceptedRow::from_event(event);
            insert_message_accepted(tx, row.clone()).await?;
            backfill_accepted_assignment(tx, &row, assignment_config).await
        }
        LegacyOrmPEvent::MessageAssigned { .. } => {
            let row = OrmpMessageAssignedRow::from_event(event);
            insert_message_assigned(tx, &row).await?;
            backfill_message_assignment(tx, &row, assignment_config).await
        }
        LegacyOrmPEvent::MessageDispatched { .. } => {
            insert_message_dispatched(tx, OrmpMessageDispatchedRow::from_event(event)).await
        }
        LegacyOrmPEvent::MsgportMessageRecv { .. } => {
            if event_source(&event) == Some(EventSource::Tron) {
                return Ok(());
            }
            insert_msgport_message_recv(tx, MsgportMessageRecvRow::from_event(event)).await
        }
        LegacyOrmPEvent::MsgportMessageSent { .. } => {
            if event_source(&event) == Some(EventSource::Tron) {
                return Ok(());
            }
            insert_msgport_message_sent(tx, MsgportMessageSentRow::from_event(event)).await
        }
        LegacyOrmPEvent::SignatureSubmittion { .. } => {
            insert_signature_submittion(tx, SignaturePubSignatureSubmittionRow::from_event(event))
                .await
        }
    }
}

fn event_source(event: &LegacyOrmPEvent) -> Option<EventSource> {
    match event {
        LegacyOrmPEvent::MsgportMessageRecv { metadata, .. }
        | LegacyOrmPEvent::MsgportMessageSent { metadata, .. } => Some(metadata.source),
        _ => None,
    }
}

async fn insert_hash_imported(
    tx: &mut Transaction<'_, Postgres>,
    row: OrmpHashImportedRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO ormp_hash_imported (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            src_chain_id, target_chain_id, oracle, channel, msg_index, hash
         )
         VALUES (
            $1, $2::NUMERIC, $3, $4::NUMERIC, $5::NUMERIC,
            $6::NUMERIC, $7::NUMERIC, $8, $9, $10::NUMERIC, $11
         )
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            chain_id = EXCLUDED.chain_id,
            src_chain_id = EXCLUDED.src_chain_id,
            target_chain_id = EXCLUDED.target_chain_id,
            oracle = EXCLUDED.oracle,
            channel = EXCLUDED.channel,
            msg_index = EXCLUDED.msg_index,
            hash = EXCLUDED.hash",
    )
    .bind(row.id)
    .bind(row.block_number.to_string())
    .bind(row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.chain_id.to_string())
    .bind(row.src_chain_id.to_string())
    .bind(row.target_chain_id.to_string())
    .bind(row.oracle)
    .bind(row.channel)
    .bind(row.msg_index.to_string())
    .bind(row.hash)
    .execute(&mut **tx)
    .await
    .context("upsert ormp_hash_imported row")?;

    Ok(())
}

async fn insert_message_accepted(
    tx: &mut Transaction<'_, Postgres>,
    row: OrmpMessageAcceptedRow,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"INSERT INTO ormp_message_accepted (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            log_index, msg_hash, channel, "index", from_chain_id, "from",
            to_chain_id, "to", gas_limit, encoded
         )
         VALUES (
            $1, $2::NUMERIC, $3, $4::NUMERIC, $5::NUMERIC,
            $6, $7, $8, $9::NUMERIC, $10::NUMERIC, $11,
            $12::NUMERIC, $13, $14::NUMERIC, $15
         )
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            chain_id = EXCLUDED.chain_id,
            log_index = EXCLUDED.log_index,
            msg_hash = EXCLUDED.msg_hash,
            channel = EXCLUDED.channel,
            "index" = EXCLUDED."index",
            from_chain_id = EXCLUDED.from_chain_id,
            "from" = EXCLUDED."from",
            to_chain_id = EXCLUDED.to_chain_id,
            "to" = EXCLUDED."to",
            gas_limit = EXCLUDED.gas_limit,
            encoded = EXCLUDED.encoded"#,
    )
    .bind(row.id)
    .bind(row.block_number.to_string())
    .bind(row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.chain_id.to_string())
    .bind(row.log_index)
    .bind(row.msg_hash)
    .bind(row.channel)
    .bind(row.index.to_string())
    .bind(row.from_chain_id.to_string())
    .bind(row.from)
    .bind(row.to_chain_id.to_string())
    .bind(row.to)
    .bind(row.gas_limit.to_string())
    .bind(row.encoded)
    .execute(&mut **tx)
    .await
    .context("upsert ormp_message_accepted row")?;

    Ok(())
}

async fn insert_message_assigned(
    tx: &mut Transaction<'_, Postgres>,
    row: &OrmpMessageAssignedRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO ormp_message_assigned (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            msg_hash, oracle, relayer, oracle_fee, relayer_fee, params
         )
         VALUES (
            $1, $2::NUMERIC, $3, $4::NUMERIC, $5::NUMERIC,
            $6, $7, $8, $9::NUMERIC, $10::NUMERIC, $11
         )
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            chain_id = EXCLUDED.chain_id,
            msg_hash = EXCLUDED.msg_hash,
            oracle = EXCLUDED.oracle,
            relayer = EXCLUDED.relayer,
            oracle_fee = EXCLUDED.oracle_fee,
            relayer_fee = EXCLUDED.relayer_fee,
            params = EXCLUDED.params",
    )
    .bind(&row.id)
    .bind(row.block_number.to_string())
    .bind(&row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.chain_id.to_string())
    .bind(&row.msg_hash)
    .bind(&row.oracle)
    .bind(&row.relayer)
    .bind(row.oracle_fee.to_string())
    .bind(row.relayer_fee.to_string())
    .bind(&row.params)
    .execute(&mut **tx)
    .await
    .context("upsert ormp_message_assigned row")?;

    Ok(())
}

async fn backfill_message_assignment(
    tx: &mut Transaction<'_, Postgres>,
    row: &OrmpMessageAssignedRow,
    assignment_config: &AssignmentConfig,
) -> anyhow::Result<()> {
    let legacy_b49e_oracle_match = row.oracle.eq_ignore_ascii_case(LEGACY_B49E_ORACLE);
    let oracle_match = contains_address(&assignment_config.oracle_addresses, &row.oracle)
        && !legacy_b49e_oracle_match;
    let relayer_match = contains_address(&assignment_config.relayer_addresses, &row.relayer);

    if !oracle_match && !legacy_b49e_oracle_match && !relayer_match {
        return Ok(());
    }

    sqlx::query(
        "UPDATE ormp_message_accepted
         SET
            oracle = CASE WHEN $2 OR ($8 AND chain_id = 1 AND from_chain_id = 1 AND to_chain_id = 46 AND block_number >= $9::NUMERIC) THEN $3 ELSE oracle END,
            oracle_assigned = CASE WHEN $2 OR ($8 AND chain_id = 1 AND from_chain_id = 1 AND to_chain_id = 46 AND block_number >= $9::NUMERIC) THEN TRUE ELSE oracle_assigned END,
            oracle_assigned_fee = CASE WHEN $2 OR ($8 AND chain_id = 1 AND from_chain_id = 1 AND to_chain_id = 46 AND block_number >= $9::NUMERIC) THEN $4::NUMERIC ELSE oracle_assigned_fee END,
            relayer = CASE WHEN $5 THEN $6 ELSE relayer END,
            relayer_assigned = CASE WHEN $5 THEN TRUE ELSE relayer_assigned END,
            relayer_assigned_fee = CASE WHEN $5 THEN $7::NUMERIC ELSE relayer_assigned_fee END
         WHERE id = $1",
    )
    .bind(&row.msg_hash)
    .bind(oracle_match)
    .bind(&row.oracle)
    .bind(row.oracle_fee.to_string())
    .bind(relayer_match)
    .bind(&row.relayer)
    .bind(row.relayer_fee.to_string())
    .bind(legacy_b49e_oracle_match)
    .bind(LEGACY_B49E_ORACLE_FROM_BLOCK.to_string())
    .execute(&mut **tx)
    .await
    .context("backfill ormp_message_accepted assignment fields")?;

    Ok(())
}

async fn backfill_accepted_assignment(
    tx: &mut Transaction<'_, Postgres>,
    row: &OrmpMessageAcceptedRow,
    assignment_config: &AssignmentConfig,
) -> anyhow::Result<()> {
    let assignments = sqlx::query_as::<_, OrmpMessageAssignedDbRow>(
        "SELECT
            id,
            block_number::TEXT AS block_number,
            transaction_hash,
            block_timestamp::TEXT AS block_timestamp,
            chain_id::TEXT AS chain_id,
            msg_hash,
            oracle,
            relayer,
            oracle_fee::TEXT AS oracle_fee,
            relayer_fee::TEXT AS relayer_fee,
            params
         FROM ormp_message_assigned
         WHERE msg_hash = $1
         ORDER BY block_number ASC, id ASC",
    )
    .bind(&row.msg_hash)
    .fetch_all(&mut **tx)
    .await
    .context("load existing ormp_message_assigned rows for accepted backfill")?;

    for assignment in assignments {
        let assignment = assignment.into_schema_row()?;
        backfill_message_assignment(tx, &assignment, assignment_config).await?;
    }

    Ok(())
}

#[derive(sqlx::FromRow)]
struct OrmpMessageAssignedDbRow {
    id: String,
    block_number: String,
    transaction_hash: String,
    block_timestamp: String,
    chain_id: String,
    msg_hash: String,
    oracle: String,
    relayer: String,
    oracle_fee: String,
    relayer_fee: String,
    params: String,
}

impl OrmpMessageAssignedDbRow {
    fn into_schema_row(self) -> anyhow::Result<OrmpMessageAssignedRow> {
        Ok(OrmpMessageAssignedRow {
            id: self.id,
            block_number: self.block_number.parse()?,
            transaction_hash: self.transaction_hash,
            block_timestamp: self.block_timestamp.parse()?,
            chain_id: self.chain_id.parse()?,
            msg_hash: self.msg_hash,
            oracle: self.oracle,
            relayer: self.relayer,
            oracle_fee: self.oracle_fee.parse()?,
            relayer_fee: self.relayer_fee.parse()?,
            params: self.params,
        })
    }
}

async fn insert_message_dispatched(
    tx: &mut Transaction<'_, Postgres>,
    row: OrmpMessageDispatchedRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO ormp_message_dispatched (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            target_chain_id, msg_hash, dispatch_result
         )
         VALUES ($1, $2::NUMERIC, $3, $4::NUMERIC, $5::NUMERIC, $6::NUMERIC, $7, $8)
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            chain_id = EXCLUDED.chain_id,
            target_chain_id = EXCLUDED.target_chain_id,
            msg_hash = EXCLUDED.msg_hash,
            dispatch_result = EXCLUDED.dispatch_result",
    )
    .bind(row.id)
    .bind(row.block_number.to_string())
    .bind(row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.chain_id.to_string())
    .bind(row.target_chain_id.to_string())
    .bind(row.msg_hash)
    .bind(row.dispatch_result)
    .execute(&mut **tx)
    .await
    .context("upsert ormp_message_dispatched row")?;

    Ok(())
}

async fn insert_msgport_message_recv(
    tx: &mut Transaction<'_, Postgres>,
    row: MsgportMessageRecvRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO msgport_message_recv (
            id, block_number, transaction_hash, block_timestamp, transaction_index,
            log_index, chain_id, port_address, msg_id, result, return_data
         )
         VALUES ($1, $2::NUMERIC, $3, $4::NUMERIC, $5, $6, $7::NUMERIC, $8, $9, $10, $11)
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            transaction_index = EXCLUDED.transaction_index,
            log_index = EXCLUDED.log_index,
            chain_id = EXCLUDED.chain_id,
            port_address = EXCLUDED.port_address,
            msg_id = EXCLUDED.msg_id,
            result = EXCLUDED.result,
            return_data = EXCLUDED.return_data",
    )
    .bind(row.id)
    .bind(row.block_number.to_string())
    .bind(row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.transaction_index)
    .bind(row.log_index)
    .bind(row.chain_id.to_string())
    .bind(row.port_address)
    .bind(row.msg_id)
    .bind(row.result)
    .bind(row.return_data)
    .execute(&mut **tx)
    .await
    .context("upsert msgport_message_recv row")?;

    Ok(())
}

async fn insert_msgport_message_sent(
    tx: &mut Transaction<'_, Postgres>,
    row: MsgportMessageSentRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO msgport_message_sent (
            id, block_number, transaction_hash, block_timestamp, transaction_index,
            log_index, chain_id, port_address, transaction_from, from_chain_id,
            msg_id, from_dapp, to_chain_id, to_dapp, message, params
         )
         VALUES (
            $1, $2::NUMERIC, $3, $4::NUMERIC, $5,
            $6, $7::NUMERIC, $8, $9, $10::NUMERIC,
            $11, $12, $13::NUMERIC, $14, $15, $16
         )
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            transaction_index = EXCLUDED.transaction_index,
            log_index = EXCLUDED.log_index,
            chain_id = EXCLUDED.chain_id,
            port_address = EXCLUDED.port_address,
            transaction_from = EXCLUDED.transaction_from,
            from_chain_id = EXCLUDED.from_chain_id,
            msg_id = EXCLUDED.msg_id,
            from_dapp = EXCLUDED.from_dapp,
            to_chain_id = EXCLUDED.to_chain_id,
            to_dapp = EXCLUDED.to_dapp,
            message = EXCLUDED.message,
            params = EXCLUDED.params",
    )
    .bind(row.id)
    .bind(row.block_number.to_string())
    .bind(row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.transaction_index)
    .bind(row.log_index)
    .bind(row.chain_id.to_string())
    .bind(row.port_address)
    .bind(row.transaction_from)
    .bind(row.from_chain_id.to_string())
    .bind(row.msg_id)
    .bind(row.from_dapp)
    .bind(row.to_chain_id.to_string())
    .bind(row.to_dapp)
    .bind(row.message)
    .bind(row.params)
    .execute(&mut **tx)
    .await
    .context("upsert msgport_message_sent row")?;

    Ok(())
}

async fn insert_signature_submittion(
    tx: &mut Transaction<'_, Postgres>,
    row: SignaturePubSignatureSubmittionRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO signature_pub_signature_submittion (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            channel, signer, msg_index, signature, data
         )
         VALUES ($1, $2::NUMERIC, $3, $4::NUMERIC, $5::NUMERIC, $6, $7, $8::NUMERIC, $9, $10)
         ON CONFLICT (id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_hash = EXCLUDED.transaction_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            chain_id = EXCLUDED.chain_id,
            channel = EXCLUDED.channel,
            signer = EXCLUDED.signer,
            msg_index = EXCLUDED.msg_index,
            signature = EXCLUDED.signature,
            data = EXCLUDED.data",
    )
    .bind(row.id)
    .bind(row.block_number.to_string())
    .bind(row.transaction_hash)
    .bind(row.block_timestamp.to_string())
    .bind(row.chain_id.to_string())
    .bind(row.channel)
    .bind(row.signer)
    .bind(row.msg_index.to_string())
    .bind(row.signature)
    .bind(row.data)
    .execute(&mut **tx)
    .await
    .context("upsert signature_pub_signature_submittion row")?;

    Ok(())
}

fn contains_address(addresses: &[String], candidate: &str) -> bool {
    addresses
        .iter()
        .any(|address| address.eq_ignore_ascii_case(candidate))
}
