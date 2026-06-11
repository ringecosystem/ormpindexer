use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{Context, bail};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    pub chain_id: u64,
    pub dataset: String,
    pub next_block: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRange {
    pub from_block: u64,
    pub to_block: u64,
}

pub fn plan_next_range(checkpoint: &Checkpoint, batch_size: u64) -> anyhow::Result<BlockRange> {
    if batch_size == 0 {
        bail!("batch size must be greater than zero");
    }

    Ok(BlockRange {
        from_block: checkpoint.next_block,
        to_block: checkpoint
            .next_block
            .checked_add(batch_size - 1)
            .context("checkpoint range overflow")?,
    })
}

#[allow(async_fn_in_trait)]
pub trait CheckpointStore {
    async fn read_or_create(
        &self,
        chain_id: u64,
        dataset: &str,
        start_block: u64,
    ) -> anyhow::Result<Checkpoint>;

    async fn advance(&self, chain_id: u64, dataset: &str, next_block: u64) -> anyhow::Result<()>;
}

#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: Arc<Mutex<BTreeMap<(u64, String), u64>>>,
}

impl InMemoryCheckpointStore {
    pub async fn next_block(&self, chain_id: u64, dataset: &str) -> anyhow::Result<u64> {
        let checkpoint = self.read_or_create(chain_id, dataset, 0).await?;
        Ok(checkpoint.next_block)
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    async fn read_or_create(
        &self,
        chain_id: u64,
        dataset: &str,
        start_block: u64,
    ) -> anyhow::Result<Checkpoint> {
        let mut checkpoints = self
            .checkpoints
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint store lock poisoned"))?;
        let next_block = *checkpoints
            .entry((chain_id, dataset.to_owned()))
            .or_insert(start_block);

        Ok(Checkpoint {
            chain_id,
            dataset: dataset.to_owned(),
            next_block,
        })
    }

    async fn advance(&self, chain_id: u64, dataset: &str, next_block: u64) -> anyhow::Result<()> {
        let mut checkpoints = self
            .checkpoints
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint store lock poisoned"))?;
        checkpoints.insert((chain_id, dataset.to_owned()), next_block);
        Ok(())
    }
}
