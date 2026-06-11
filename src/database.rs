use anyhow::Context;
use sqlx::{PgPool, migrate::Migrator, postgres::PgPoolOptions};

use crate::{checkpoint::CheckpointStore, config::SecretString, schema::LegacyOrmPEvent};

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
    _pool: PgPool,
}

impl PostgresEventWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }
}

impl EventWriter for PostgresEventWriter {
    async fn write_events(&self, events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        if !events.is_empty() {
            anyhow::bail!("ORMP event database writes are not implemented yet");
        }
        Ok(0)
    }
}
