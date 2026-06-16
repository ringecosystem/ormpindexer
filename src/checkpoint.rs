use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{Context, bail};

use crate::config::FinalityMode;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockAnchor {
    pub chain_id: u64,
    pub dataset: String,
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub finality: FinalityMode,
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

    async fn upsert_block_anchors(&self, _anchors: &[BlockAnchor]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn read_block_anchors(
        &self,
        _chain_id: u64,
        _dataset: &str,
        _from_block: u64,
        _to_block: u64,
    ) -> anyhow::Result<Vec<BlockAnchor>> {
        Ok(Vec::new())
    }

    async fn rollback_legacy_from(
        &self,
        chain_id: u64,
        dataset: &str,
        rollback_block: u64,
    ) -> anyhow::Result<()> {
        self.advance(chain_id, dataset, rollback_block).await
    }
}

#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: Arc<Mutex<InMemoryCheckpoints>>,
    anchors: Arc<Mutex<InMemoryBlockAnchors>>,
}

type InMemoryCheckpoints = BTreeMap<(u64, String), u64>;
type InMemoryBlockAnchors = BTreeMap<(u64, String, u64), BlockAnchor>;

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

    async fn upsert_block_anchors(&self, anchors: &[BlockAnchor]) -> anyhow::Result<()> {
        let mut stored = self
            .anchors
            .lock()
            .map_err(|_| anyhow::anyhow!("block anchor store lock poisoned"))?;
        for anchor in anchors {
            stored.insert(
                (anchor.chain_id, anchor.dataset.clone(), anchor.block_number),
                anchor.clone(),
            );
        }
        Ok(())
    }

    async fn read_block_anchors(
        &self,
        chain_id: u64,
        dataset: &str,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<BlockAnchor>> {
        let stored = self
            .anchors
            .lock()
            .map_err(|_| anyhow::anyhow!("block anchor store lock poisoned"))?;
        Ok(stored
            .range(
                (chain_id, dataset.to_owned(), from_block)
                    ..=(chain_id, dataset.to_owned(), to_block),
            )
            .map(|(_, anchor)| anchor.clone())
            .collect())
    }

    async fn rollback_legacy_from(
        &self,
        chain_id: u64,
        dataset: &str,
        rollback_block: u64,
    ) -> anyhow::Result<()> {
        {
            let mut stored = self
                .anchors
                .lock()
                .map_err(|_| anyhow::anyhow!("block anchor store lock poisoned"))?;
            stored.retain(|(anchor_chain_id, anchor_dataset, block_number), _| {
                *anchor_chain_id != chain_id
                    || anchor_dataset != dataset
                    || *block_number < rollback_block
            });
        }
        self.advance(chain_id, dataset, rollback_block).await
    }
}
