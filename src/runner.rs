use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};

use tokio::time::sleep;

use anyhow::Context;
use futures_util::{StreamExt, stream::FuturesUnordered};

use crate::{
    checkpoint::{BlockRange, CheckpointStore, plan_next_range},
    config::{ChainConfig, RuntimeConfig},
    database::EventWriter,
    datalens::{
        DatalensFailureKind, DatalensLogQuery, DatalensLogQueryResult, DatalensLogReader,
        DatalensTransactionQuery, classify_datalens_failure_message,
    },
    decoder::EventDecoder,
    planner::{MSGPORT_MESSAGE_SENT_TOPIC, TRON_CHAIN_ID, chain_dataset},
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
    caught_up: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ProcessedRangeReport {
    next_block: u64,
    ranges_queried: u64,
    records_read: u64,
    records_decoded: u64,
    records_written: u64,
    checkpoints_advanced: u64,
}

trait RunReport {
    fn ranges_queried(&self) -> u64;

    fn should_sleep_after_run(&self) -> bool {
        self.ranges_queried() == 0
    }
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

    fn should_sleep_after_run(&self) -> bool {
        self.caught_up || self.ranges_queried == 0
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
        let mut consecutive_failures = 0;
        loop {
            match self.run_chain_once(chain.clone()).await {
                Ok(report) => {
                    consecutive_failures = 0;
                    if should_sleep_after_report(&report) {
                        sleep(self.config.poll_interval).await;
                    }
                }
                Err(error) => {
                    consecutive_failures += 1;
                    let backoff = failure_backoff(self.config.poll_interval, consecutive_failures);
                    log::error!(
                        "ORMP Datalens chain pass failed chain_id={} start_block={} consecutive_failures={} backoff_ms={} error={:#}",
                        chain.chain_id,
                        chain.start_block,
                        consecutive_failures,
                        backoff.as_millis(),
                        error
                    );
                    sleep(backoff).await;
                }
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
        let mut checkpoint = self
            .checkpoints
            .read_or_create(chain.chain_id, dataset, chain.start_block)
            .await
            .with_context(|| {
                format!(
                    "read or create ORMP checkpoint chain_id={} dataset={} start_block={}",
                    chain.chain_id, dataset, chain.start_block
                )
            })?;
        let latest_block = self
            .reader
            .latest_block(chain.chain_id, self.config.finality_mode)
            .await
            .with_context(|| format!("query Datalens chain head for chain {}", chain.chain_id))?;
        let target_block = latest_block.saturating_sub(self.config.datalens.head_buffer_blocks);
        if checkpoint.next_block > target_block {
            log::info!(
                "skipping ORMP Datalens chain_id={} dataset={} checkpoint_next_block={} target_block={} latest_block={} head_buffer_blocks={} checkpoint_ahead_of_target=true",
                chain.chain_id,
                dataset,
                checkpoint.next_block,
                target_block,
                latest_block,
                self.config.datalens.head_buffer_blocks,
            );
            return Ok(ChainRunReport {
                caught_up: true,
                ..ChainRunReport::default()
            });
        }

        let mut report = ChainRunReport::default();
        while checkpoint.next_block <= target_block {
            let mut range = plan_next_range(&checkpoint, chain.batch_size).with_context(|| {
                format!(
                    "plan ORMP checkpoint range chain_id={} dataset={} checkpoint_next_block={} batch_size={}",
                    chain.chain_id, dataset, checkpoint.next_block, chain.batch_size
                )
            })?;
            range.to_block = range.to_block.min(target_block);

            let range_report = self
                .process_range_with_splitting(&chain, dataset, range, target_block)
                .await?;
            report.ranges_queried += range_report.ranges_queried;
            report.records_read += range_report.records_read;
            report.records_decoded += range_report.records_decoded;
            report.records_written += range_report.records_written;
            report.checkpoints_advanced += range_report.checkpoints_advanced;
            checkpoint.next_block = range_report.next_block;
        }

        if report.ranges_queried > 0 {
            report.chains_processed = 1;
        }
        report.caught_up = true;
        Ok(report)
    }

    async fn process_range_with_splitting(
        &self,
        chain: &ChainConfig,
        dataset: &str,
        range: BlockRange,
        target_block: u64,
    ) -> anyhow::Result<ProcessedRangeReport> {
        let mut pending = vec![range];
        let mut report = ProcessedRangeReport::default();

        while let Some(range) = pending.pop() {
            log_query_start(chain, dataset, range, target_block, &self.config);
            let batch_started = Instant::now();
            let result = match self.query_range_once(chain, dataset, range).await {
                Ok(result) => result,
                Err(error) => {
                    if can_split_datalens_query_failure(&error, range) {
                        let (left, right) = split_range(range);
                        log::warn!(
                            "splitting ORMP Datalens range after retryable failure chain_id={} dataset={} from_block={} to_block={} left_from_block={} left_to_block={} right_from_block={} right_to_block={} error={:#}",
                            chain.chain_id,
                            dataset,
                            range.from_block,
                            range.to_block,
                            left.from_block,
                            left.to_block,
                            right.from_block,
                            right.to_block,
                            error
                        );
                        pending.push(right);
                        pending.push(left);
                        continue;
                    }

                    return Err(error);
                }
            };

            let range_report = self
                .process_successful_range(
                    chain,
                    dataset,
                    range,
                    target_block,
                    batch_started,
                    result,
                )
                .await?;
            report.next_block = range_report.next_block;
            report.ranges_queried += range_report.ranges_queried;
            report.records_read += range_report.records_read;
            report.records_decoded += range_report.records_decoded;
            report.records_written += range_report.records_written;
            report.checkpoints_advanced += range_report.checkpoints_advanced;
        }

        Ok(report)
    }

    async fn query_range_once(
        &self,
        chain: &ChainConfig,
        dataset: &str,
        range: BlockRange,
    ) -> anyhow::Result<DatalensLogQueryResult> {
        self.reader
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
            })
    }

    async fn process_successful_range(
        &self,
        chain: &ChainConfig,
        dataset: &str,
        range: BlockRange,
        target_block: u64,
        batch_started: Instant,
        result: DatalensLogQueryResult,
    ) -> anyhow::Result<ProcessedRangeReport> {
        let records_read = result.logs.len();
        let logs = self
            .enrich_logs_with_transaction_senders(chain, result.logs)
            .await?;
        let mut events = Vec::new();
        for log in &logs {
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

        let progress = batch_progress(range, target_block, next_block, batch_started.elapsed());
        log::info!(
            "ORMP Datalens batch completed chain_id={} dataset={} from_block={} to_block={} target_block={} records_count={} decoded_count={} written_count={} checkpoint_next_block={} checkpoint_advanced=true batch_blocks={} remaining_blocks={} batch_duration_ms={} current_rate_blocks_per_second={:.2} eta_seconds={}",
            chain.chain_id,
            dataset,
            range.from_block,
            range.to_block,
            target_block,
            records_read,
            events.len(),
            written,
            next_block,
            progress.batch_blocks,
            progress.remaining_blocks,
            progress.batch_duration_ms,
            progress.current_rate_blocks_per_second,
            progress.eta_seconds,
        );

        Ok(ProcessedRangeReport {
            next_block,
            ranges_queried: 1,
            records_read: records_read as u64,
            records_decoded: events.len() as u64,
            records_written: written as u64,
            checkpoints_advanced: 1,
        })
    }

    async fn enrich_logs_with_transaction_senders(
        &self,
        chain: &ChainConfig,
        mut logs: Vec<crate::datalens::DatalensLog>,
    ) -> anyhow::Result<Vec<crate::datalens::DatalensLog>> {
        if chain.chain_id == TRON_CHAIN_ID || logs.iter().all(|log| log.transaction_from.is_some())
        {
            return Ok(logs);
        }

        let sender_blocks =
            logs.iter()
                .filter(|log| {
                    log.transaction_from.is_none()
                        && log.topics.first().is_some_and(|topic| {
                            topic.eq_ignore_ascii_case(MSGPORT_MESSAGE_SENT_TOPIC)
                        })
                })
                .map(|log| log.block_number)
                .collect::<BTreeSet<_>>();
        if sender_blocks.is_empty() {
            return Ok(logs);
        }

        let mut transactions = Vec::new();
        for block_number in sender_blocks {
            transactions.extend(
                self.reader
                    .query_transactions(DatalensTransactionQuery {
                        chain_id: chain.chain_id,
                        from_block: block_number,
                        to_block: block_number,
                        finality_mode: self.config.finality_mode,
                    })
                    .await
                    .with_context(|| {
                        format!(
                            "query ORMP Datalens transactions chain_id={} block_number={}",
                            chain.chain_id, block_number
                        )
                    })?
                    .transactions,
            );
        }
        let senders = transactions
            .into_iter()
            .map(|transaction| (sender_hash_key(&transaction.hash), transaction.from))
            .collect::<HashMap<_, _>>();

        for log in &mut logs {
            if log.transaction_from.is_none() {
                log.transaction_from = senders
                    .get(&sender_hash_key(&log.transaction_hash))
                    .cloned();
            }
        }

        Ok(logs)
    }
}

fn sender_hash_key(hash: &str) -> String {
    let hash = hash.trim();
    let hash = hash
        .strip_prefix("0x")
        .or_else(|| hash.strip_prefix("0X"))
        .unwrap_or(hash);
    format!("0x{}", hash.to_ascii_lowercase())
}

fn can_split_datalens_query_failure(error: &anyhow::Error, range: BlockRange) -> bool {
    if range.from_block >= range.to_block {
        return false;
    }

    let error = format!("{error:#}");
    let failure_kind = classify_datalens_failure_message(&error);
    if matches!(failure_kind, DatalensFailureKind::ProviderLimit) {
        return true;
    }

    matches!(failure_kind, DatalensFailureKind::Transient)
        && (error.contains("provider_failure") || error.contains("providerfailure"))
}

fn split_range(range: BlockRange) -> (BlockRange, BlockRange) {
    let midpoint = range.from_block + (range.to_block - range.from_block) / 2;
    (
        BlockRange {
            from_block: range.from_block,
            to_block: midpoint,
        },
        BlockRange {
            from_block: midpoint + 1,
            to_block: range.to_block,
        },
    )
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
        chain.batch_size,
        chain.contracts.len(),
        chain.topics.len(),
        config.finality_mode.as_str(),
    );
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BatchProgress {
    batch_blocks: u64,
    remaining_blocks: u64,
    batch_duration_ms: u128,
    current_rate_blocks_per_second: f64,
    eta_seconds: u64,
}

fn batch_progress(
    range: BlockRange,
    target_block: u64,
    next_block: u64,
    duration: Duration,
) -> BatchProgress {
    let batch_blocks = range
        .to_block
        .saturating_sub(range.from_block)
        .saturating_add(1);
    let remaining_blocks = if next_block > target_block {
        0
    } else {
        target_block.saturating_sub(next_block).saturating_add(1)
    };
    let elapsed_seconds = duration.as_secs_f64().max(0.001);
    let current_rate_blocks_per_second = batch_blocks as f64 / elapsed_seconds;
    let eta_seconds = if remaining_blocks == 0 {
        0
    } else {
        (remaining_blocks as f64 / current_rate_blocks_per_second).ceil() as u64
    };

    BatchProgress {
        batch_blocks,
        remaining_blocks,
        batch_duration_ms: duration.as_millis(),
        current_rate_blocks_per_second,
        eta_seconds,
    }
}

fn should_sleep_after_report<R>(report: &R) -> bool
where
    R: RunReport,
{
    report.should_sleep_after_run()
}

fn failure_backoff(poll_interval: Duration, consecutive_failures: u64) -> Duration {
    let multiplier = 1_u128 << consecutive_failures.saturating_sub(1).min(10);
    let millis = poll_interval.as_millis().max(1).saturating_mul(multiplier);
    Duration::from_millis(millis.min(60_000) as u64)
}
