use tokio::time::sleep;

use anyhow::Context;

use crate::{
    checkpoint::{CheckpointStore, plan_next_range},
    config::RuntimeConfig,
    database::EventWriter,
    datalens::{DatalensLogQuery, DatalensLogReader},
    decoder::EventDecoder,
    planner::chain_dataset,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunnerReport {
    pub chains_processed: u64,
    pub ranges_queried: u64,
    pub records_read: u64,
    pub records_decoded: u64,
    pub records_written: u64,
    pub checkpoints_advanced: u64,
}

pub struct IndexerRunner<R, C, D, W> {
    config: RuntimeConfig,
    reader: R,
    checkpoints: C,
    decoder: D,
    writer: W,
}

impl<R, C, D, W> IndexerRunner<R, C, D, W> {
    pub fn new(config: RuntimeConfig, reader: R, checkpoints: C, decoder: D, writer: W) -> Self {
        Self {
            config,
            reader,
            checkpoints,
            decoder,
            writer,
        }
    }
}

impl<R, C, D, W> IndexerRunner<R, C, D, W>
where
    R: DatalensLogReader,
    C: CheckpointStore,
    D: EventDecoder,
    W: EventWriter,
{
    pub async fn run_loop(&self) -> anyhow::Result<()> {
        loop {
            self.run_once().await?;
            sleep(self.config.poll_interval).await;
        }
    }

    pub async fn run_once(&self) -> anyhow::Result<RunnerReport> {
        let mut report = RunnerReport::default();

        for chain in &self.config.enabled_chains {
            let dataset = chain_dataset(chain.chain_id)?;
            let checkpoint = self
                .checkpoints
                .read_or_create(chain.chain_id, dataset, chain.start_block)
                .await?;
            let range = plan_next_range(&checkpoint, self.config.batch_size)?;

            log::info!(
                "querying ORMP Datalens logs chain_id={} dataset={} from_block={} to_block={} batch_size={} contracts={} topics={} finality={}",
                chain.chain_id,
                dataset,
                range.from_block,
                range.to_block,
                self.config.batch_size,
                chain.contracts.len(),
                chain.topics.len(),
                self.config.finality_mode.as_str(),
            );

            let result = self
                .reader
                .query_logs(DatalensLogQuery {
                    chain_id: chain.chain_id,
                    from_block: range.from_block,
                    to_block: range.to_block,
                    contracts: chain.contracts.clone(),
                    topics: chain.topics.clone(),
                    finality_mode: self.config.finality_mode,
                })
                .await?;
            let mut events = Vec::new();
            for log in &result.logs {
                events.extend(self.decoder.decode(log).await?);
            }
            let written = self.writer.write_events(&events).await?;
            let next_block = range
                .to_block
                .checked_add(1)
                .context("checkpoint next block overflow")?;
            self.checkpoints
                .advance(chain.chain_id, dataset, next_block)
                .await?;

            log::info!(
                "ORMP Datalens batch completed chain_id={} dataset={} from_block={} to_block={} records_count={} decoded_count={} written_count={} checkpoint_next_block={} checkpoint_advanced=true",
                chain.chain_id,
                dataset,
                range.from_block,
                range.to_block,
                result.logs.len(),
                events.len(),
                written,
                next_block,
            );

            report.chains_processed += 1;
            report.ranges_queried += 1;
            report.records_read += result.logs.len() as u64;
            report.records_decoded += events.len() as u64;
            report.records_written += written as u64;
            report.checkpoints_advanced += 1;
        }

        Ok(report)
    }
}
