use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use ormpindexer::{
    checkpoint::{Checkpoint, InMemoryCheckpointStore, plan_next_range},
    config::{FinalityMode, RuntimeConfig},
    database::{DryRunEventWriter, EventWriter},
    datalens::{DatalensLog, DatalensLogQuery, DatalensLogQueryResult, DatalensLogReader},
    decoder::NoopDecoder,
    runner::{IndexerRunner, RunnerReport},
    schema::LegacyOrmPEvent,
};

#[test]
fn test_runtime_config_from_env_map_reads_datalens_database_and_chain_settings() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_TOKEN".to_owned(),
            "secret-token".to_owned(),
        ),
        (
            "ORMPINDEXER_DATABASE_URL".to_owned(),
            "postgres://user:pass@localhost/ormp".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "1,46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "250".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "1000".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "durable".to_owned()),
        (
            "ORMPINDEXER_CHAIN_1_CONTRACTS".to_owned(),
            "0x111,0x222".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_1_TOPICS".to_owned(),
            "0xaaa,0xbbb".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_46_START_BLOCK".to_owned(),
            "2000".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_46_CONTRACTS".to_owned(),
            "0x333".to_owned(),
        ),
    ]);

    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert_eq!(config.datalens.endpoint, "https://datalens.example");
    assert_eq!(config.datalens.application, "ormp-production");
    assert!(config.datalens.token.is_some());
    assert!(config.database_url.is_some());
    assert_eq!(config.enabled_chains.len(), 2);
    assert_eq!(config.batch_size, 250);
    assert_eq!(config.start_block, 1000);
    assert_eq!(config.finality_mode, FinalityMode::Durable);
    assert_eq!(
        config.chain(1).expect("chain 1").contracts,
        vec!["0x111", "0x222"]
    );
    assert_eq!(
        config.chain(1).expect("chain 1").topics,
        vec!["0xaaa", "0xbbb"]
    );
    assert_eq!(config.chain(46).expect("chain 46").start_block, 2000);
}

#[test]
fn test_database_url_from_env_map_does_not_require_runtime_chain_settings() {
    let env = BTreeMap::from([(
        "ORMPINDEXER_DATABASE_URL".to_owned(),
        "postgres://user:pass@localhost/ormp".to_owned(),
    )]);

    let database_url =
        RuntimeConfig::database_url_from_env_map(&env).expect("database URL parses independently");

    assert_eq!(
        database_url.expose_secret(),
        "postgres://user:pass@localhost/ormp"
    );
}

#[test]
fn test_plan_next_range_uses_checkpoint_and_batch_size() {
    let checkpoint = Checkpoint {
        chain_id: 46,
        dataset: "datalens-native".to_owned(),
        next_block: 120,
    };

    let range = plan_next_range(&checkpoint, 25).expect("range is planned");

    assert_eq!(range.from_block, 120);
    assert_eq!(range.to_block, 144);
}

#[tokio::test]
async fn test_runner_successful_batch_advances_checkpoint_to_next_range() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
        (
            "ORMPINDEXER_CHAIN_46_CONTRACTS".to_owned(),
            "0x333".to_owned(),
        ),
        ("ORMPINDEXER_CHAIN_46_TOPICS".to_owned(), "0xaaa".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(vec![DatalensLog {
        id: Some("46-0xtx-1".to_owned()),
        chain_id: 46,
        block_number: 12,
        block_timestamp: Some(1_700_000_000_000),
        transaction_hash: "0xtx".to_owned(),
        transaction_index: Some(0),
        log_index: 1,
        address: "0x333".to_owned(),
        transaction_from: None,
        topics: vec!["0xaaa".to_owned()],
        data: "0x".to_owned(),
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    }]);
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("batch pass succeeds");

    assert_eq!(
        report,
        RunnerReport {
            chains_processed: 1,
            ranges_queried: 1,
            records_read: 1,
            records_decoded: 0,
            records_written: 0,
            checkpoints_advanced: 1,
        }
    );
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
}

#[tokio::test]
async fn test_runner_empty_logs_still_advance_after_successful_query_and_write() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        RecordingDatalensReader::new(Vec::new()),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("empty batch succeeds");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
}

#[tokio::test]
async fn test_runner_skips_chain_when_checkpoint_is_ahead_of_datalens_head() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new()).with_head(46, 9);
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("ahead checkpoint skips");

    assert_eq!(report, RunnerReport::default());
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
}

#[tokio::test]
async fn test_runner_caps_query_range_at_datalens_head() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new()).with_head(46, 12);
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("capped batch succeeds");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 13);
    assert_eq!(reader.queries.borrow()[0].from_block, 10);
    assert_eq!(reader.queries.borrow()[0].to_block, 12);
}

#[tokio::test]
async fn test_runner_writer_failure_does_not_advance_checkpoint() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        RecordingDatalensReader::new(Vec::new()),
        checkpoints.clone(),
        NoopDecoder,
        FailingEventWriter,
    );

    let error = runner
        .run_once()
        .await
        .expect_err("writer failure fails the batch");

    assert!(error.to_string().contains("write failed"));
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
}

#[tokio::test]
async fn test_runner_reports_and_advances_multiple_chains() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "1,46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
        (
            "ORMPINDEXER_CHAIN_46_START_BLOCK".to_owned(),
            "20".to_owned(),
        ),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        RecordingDatalensReader::new(Vec::new()),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("multi-chain pass succeeds");

    assert_eq!(
        report,
        RunnerReport {
            chains_processed: 2,
            ranges_queried: 2,
            records_read: 0,
            records_decoded: 0,
            records_written: 0,
            checkpoints_advanced: 2,
        }
    );
    assert_eq!(checkpoints.next_block(1, "evm.logs").await.unwrap(), 15);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 25);
}

#[derive(Clone)]
struct RecordingDatalensReader {
    logs: Vec<DatalensLog>,
    heads: BTreeMap<u64, u64>,
    queries: Rc<RefCell<Vec<DatalensLogQuery>>>,
}

impl RecordingDatalensReader {
    fn new(logs: Vec<DatalensLog>) -> Self {
        Self {
            logs,
            heads: BTreeMap::new(),
            queries: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn with_head(mut self, chain_id: u64, head: u64) -> Self {
        self.heads.insert(chain_id, head);
        self
    }
}

impl DatalensLogReader for RecordingDatalensReader {
    async fn latest_block(
        &self,
        chain_id: u64,
        _finality_mode: FinalityMode,
    ) -> anyhow::Result<u64> {
        Ok(*self.heads.get(&chain_id).unwrap_or(&u64::MAX))
    }

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult> {
        self.queries.borrow_mut().push(query);
        Ok(DatalensLogQueryResult {
            logs: self.logs.clone(),
        })
    }
}

struct FailingEventWriter;

impl EventWriter for FailingEventWriter {
    async fn write_events(&self, _events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        anyhow::bail!("write failed");
    }
}
