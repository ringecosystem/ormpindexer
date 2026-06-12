use tokio::time::sleep;

use anyhow::Context;
use futures_util::{StreamExt, stream::FuturesUnordered};

use crate::{
    checkpoint::{BlockRange, CheckpointStore, plan_next_range},
    config::{ChainConfig, RuntimeConfig},
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

impl RunnerReport {
    fn add_chain_report(&mut self, chain_report: ChainRunReport) {
        self.chains_processed += chain_report.chains_processed;
        self.ranges_queried += chain_report.ranges_queried;
        self.records_read += chain_report.records_read;
        self.records_decoded += chain_report.records_decoded;
        self.records_written += chain_report.records_written;
        self.checkpoints_advanced += chain_report.checkpoints_advanced;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ChainRunReport {
    chains_processed: u64,
    ranges_queried: u64,
    records_read: u64,
    records_decoded: u64,
    records_written: u64,
    checkpoints_advanced: u64,
}

trait RunReport {
    fn ranges_queried(&self) -> u64;
}

impl RunReport for RunnerReport {
    fn ranges_queried(&self) -> u64 {
        self.ranges_queried
    }
}

impl RunReport for ChainRunReport {
    fn ranges_queried(&self) -> u64 {
        self.ranges_queried
    }
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
        let mut runs = self
            .config
            .enabled_chains
            .iter()
            .cloned()
            .map(|chain| self.run_chain_loop(chain))
            .collect::<FuturesUnordered<_>>();

        while let Some(result) = runs.next().await {
            result?;
        }

        Ok(())
    }

    async fn run_chain_loop(&self, chain: ChainConfig) -> anyhow::Result<()> {
        loop {
            let report = self.run_chain_once(chain.clone()).await?;
            if should_sleep_after_run(&report) {
                sleep(self.config.poll_interval).await;
            }
        }
    }

    pub async fn run_once(&self) -> anyhow::Result<RunnerReport> {
        let mut report = RunnerReport::default();
        let mut runs = self
            .config
            .enabled_chains
            .iter()
            .cloned()
            .map(|chain| self.run_chain_once(chain))
            .collect::<FuturesUnordered<_>>();

        while let Some(chain_report) = runs.next().await {
            report.add_chain_report(chain_report?);
        }

        Ok(report)
    }

    async fn run_chain_once(&self, chain: ChainConfig) -> anyhow::Result<ChainRunReport> {
        let dataset = chain_dataset(chain.chain_id)?;
        let checkpoint = self
            .checkpoints
            .read_or_create(chain.chain_id, dataset, chain.start_block)
            .await
            .with_context(|| {
                format!(
                    "read or create ORMP checkpoint chain_id={} dataset={} start_block={}",
                    chain.chain_id, dataset, chain.start_block
                )
            })?;
        let target_block = self
            .reader
            .latest_block(chain.chain_id, self.config.finality_mode)
            .await
            .with_context(|| format!("query Datalens chain head for chain {}", chain.chain_id))?;
        if checkpoint.next_block > target_block {
            log::info!(
                "skipping ORMP Datalens chain_id={} dataset={} checkpoint_next_block={} target_block={} checkpoint_ahead_of_target=true",
                chain.chain_id,
                dataset,
                checkpoint.next_block,
                target_block,
            );
            return Ok(ChainRunReport::default());
        }

        let mut range = plan_next_range(&checkpoint, self.config.batch_size).with_context(|| {
            format!(
                "plan ORMP checkpoint range chain_id={} dataset={} checkpoint_next_block={} batch_size={}",
                chain.chain_id, dataset, checkpoint.next_block, self.config.batch_size
            )
        })?;
        range.to_block = range.to_block.min(target_block);

        log_query_start(&chain, dataset, range, target_block, &self.config);

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
            .await
            .with_context(|| {
                format!(
                    "query ORMP Datalens logs chain_id={} dataset={} from_block={} to_block={}",
                    chain.chain_id, dataset, range.from_block, range.to_block
                )
            })?;
        let mut events = Vec::new();
        for log in &result.logs {
            let topic0 = log
                .topics
                .first()
                .map(String::as_str)
                .unwrap_or("<missing>");
            events.extend(self.decoder.decode(log).await.with_context(|| {
                format!(
                    "decode ORMP Datalens log chain_id={} block_number={} transaction_hash={} log_index={} address={} topic0={}",
                    log.chain_id,
                    log.block_number,
                    log.transaction_hash,
                    log.log_index,
                    log.address,
                    topic0
                )
            })?);
        }
        let written = self.writer.write_events(&events).await.with_context(|| {
            format!(
                "write ORMP events chain_id={} dataset={} from_block={} to_block={}",
                chain.chain_id, dataset, range.from_block, range.to_block
            )
        })?;
        let next_block = range.to_block.checked_add(1).with_context(|| {
            format!(
                "ORMP checkpoint next block overflow chain_id={} dataset={} to_block={}",
                chain.chain_id, dataset, range.to_block
            )
        })?;
        self.checkpoints
            .advance(chain.chain_id, dataset, next_block)
            .await
            .with_context(|| {
                format!(
                    "advance ORMP checkpoint chain_id={} dataset={} next_block={}",
                    chain.chain_id, dataset, next_block
                )
            })?;

        log::info!(
            "ORMP Datalens batch completed chain_id={} dataset={} from_block={} to_block={} target_block={} records_count={} decoded_count={} written_count={} checkpoint_next_block={} checkpoint_advanced=true",
            chain.chain_id,
            dataset,
            range.from_block,
            range.to_block,
            target_block,
            result.logs.len(),
            events.len(),
            written,
            next_block,
        );

        Ok(ChainRunReport {
            chains_processed: 1,
            ranges_queried: 1,
            records_read: result.logs.len() as u64,
            records_decoded: events.len() as u64,
            records_written: written as u64,
            checkpoints_advanced: 1,
        })
    }
}

fn log_query_start(
    chain: &ChainConfig,
    dataset: &str,
    range: BlockRange,
    target_block: u64,
    config: &RuntimeConfig,
) {
    log::info!(
        "querying ORMP Datalens logs chain_id={} dataset={} from_block={} to_block={} target_block={} batch_size={} contracts={} topics={} finality={}",
        chain.chain_id,
        dataset,
        range.from_block,
        range.to_block,
        target_block,
        config.batch_size,
        chain.contracts.len(),
        chain.topics.len(),
        config.finality_mode.as_str(),
    );
}

fn should_sleep_after_run<R>(report: &R) -> bool
where
    R: RunReport,
{
    report.ranges_queried() == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_sleep_after_run_when_no_ranges_queried() {
        assert!(should_sleep_after_run(&RunnerReport::default()));
    }

    #[test]
    fn test_should_not_sleep_after_run_when_backlog_advanced() {
        let report = RunnerReport {
            ranges_queried: 1,
            ..RunnerReport::default()
        };

        assert!(!should_sleep_after_run(&report));
    }
}
