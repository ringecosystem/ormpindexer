use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use ormpindexer::{
    checkpoint::{CheckpointStore, InMemoryCheckpointStore},
    config::{FinalityMode, RuntimeConfig},
    database::DryRunEventWriter,
    datalens::{DatalensLog, DatalensTransaction, DatalensTransactionQuery},
    decoder::NoopDecoder,
    planner::MSGPORT_MESSAGE_SENT_TOPIC,
    runner::{IndexerRunner, RunnerReport},
    schema::LegacyOrmPEvent,
};

mod runtime_support;

use runtime_support::{
    EchoTransactionFromDecoder, FailingEventWriter, FailingEventWriterWithMessage,
    RecordingDatalensReader, RecordingEventWriter,
};

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
        block_hash: None,
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
    }])
    .with_head(46, 15);
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
async fn test_runner_enriches_evm_logs_with_transaction_senders() {
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
    let tx_hash = format!("0x{}", "aa".repeat(32));
    let reader = RecordingDatalensReader::new(vec![DatalensLog {
        id: Some("46-12-0".to_owned()),
        chain_id: 46,
        block_number: 12,
        block_hash: None,
        block_timestamp: Some(1_700_000_000_000),
        transaction_hash: tx_hash.trim_start_matches("0x").to_ascii_uppercase(),
        transaction_index: Some(0),
        log_index: 1,
        address: "0x333".to_owned(),
        transaction_from: None,
        topics: vec![MSGPORT_MESSAGE_SENT_TOPIC.to_owned()],
        data: "0x".to_owned(),
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    }])
    .with_head(46, 15)
    .with_transactions(vec![DatalensTransaction {
        hash: tx_hash,
        block_number: 12,
        from: "0xsender".to_owned(),
    }]);
    let tx_queries = reader.transaction_queries();
    let writer = RecordingEventWriter::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        InMemoryCheckpointStore::default(),
        EchoTransactionFromDecoder,
        writer.clone(),
    );

    runner.run_once().await.expect("batch pass succeeds");

    let events = writer.events();
    assert_eq!(events.len(), 1);
    match &events[0] {
        LegacyOrmPEvent::MsgportMessageSent { metadata, .. } => {
            assert_eq!(metadata.transaction_from.as_deref(), Some("0xsender"));
        }
        _ => panic!("expected MsgportMessageSent event"),
    }
    assert_eq!(
        tx_queries
            .lock()
            .expect("transaction queries lock")
            .as_slice(),
        &[DatalensTransactionQuery {
            chain_id: 46,
            from_block: 12,
            to_block: 12,
            finality_mode: FinalityMode::Finalized,
        }]
    );
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
        RecordingDatalensReader::new(Vec::new()).with_head(46, 15),
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
    let reader = RecordingDatalensReader::new(Vec::new()).with_head(46, 13);
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
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(queries[0].from_block, 10);
    assert_eq!(queries[0].to_block, 12);
}

#[tokio::test]
async fn test_runner_applies_default_datalens_head_buffer_to_follow_target() {
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
        ("ORMPINDEXER_START_BLOCK".to_owned(), "14".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new()).with_head(46, 15);
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("buffered batch succeeds");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(14, 14)]
    );
}

#[tokio::test]
async fn test_runner_processes_contiguous_ranges_until_caught_up() {
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
    let reader = RecordingDatalensReader::new(Vec::new()).with_head(46, 23);
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("backlog pass succeeds");

    assert_eq!(report.ranges_queried, 3);
    assert_eq!(report.checkpoints_advanced, 3);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 23);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(10, 14), (15, 19), (20, 22)]
    );
}

#[tokio::test]
async fn test_runner_splits_retryable_datalens_range_failure_and_advances_children_in_order() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "4".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 14)
        .with_range_query_failure(46, 10, 13, "provider_failure: upstream returned 524");
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("split batch succeeds");

    assert_eq!(report.ranges_queried, 2);
    assert_eq!(report.checkpoints_advanced, 2);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 14);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(10, 13), (10, 11), (12, 13)]
    );
}

#[tokio::test]
async fn test_runner_stops_checkpoint_at_retryable_single_block_failure_after_left_child_succeeds()
{
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "4".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 14)
        .with_range_query_failure(46, 10, 13, "provider_failure: upstream returned 524")
        .with_range_query_failure(46, 12, 13, "provider range limit exceeded")
        .with_range_query_failure(46, 12, 12, "provider_failure: upstream returned 524");
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let error = runner
        .run_once()
        .await
        .expect_err("single block failure stops the chain pass");

    let error_chain = format!("{error:#}");
    assert!(error_chain.contains("from_block=12 to_block=12"));
    assert!(error_chain.contains("provider_failure"));
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 12);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(10, 13), (10, 11), (12, 13), (12, 12)]
    );
}

#[tokio::test]
async fn test_runner_does_not_split_non_retryable_datalens_failure() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "4".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 14)
        .with_range_query_failure(46, 10, 13, "permission denied");
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let error = runner
        .run_once()
        .await
        .expect_err("non-retryable failure stops the chain pass");

    assert!(format!("{error:#}").contains("permission denied"));
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(10, 13)]
    );
}

#[tokio::test]
async fn test_runner_does_not_split_downstream_timeout_failure() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "4".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new()).with_head(46, 14);
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        FailingEventWriterWithMessage("database timeout"),
    );

    let error = runner
        .run_once()
        .await
        .expect_err("downstream timeout failure stops without split");

    assert!(format!("{error:#}").contains("database timeout"));
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(10, 13)]
    );
}

#[tokio::test]
async fn test_runner_does_not_split_generic_transient_datalens_failure() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "4".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 14)
        .with_range_query_failure(46, 10, 13, "request timed out after 60 seconds");
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let error = runner
        .run_once()
        .await
        .expect_err("generic transient failure stops without split");

    assert!(format!("{error:#}").contains("request timed out"));
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
    let queries = reader.queries.lock().expect("queries lock");
    assert_eq!(
        queries
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(10, 13)]
    );
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
        RecordingDatalensReader::new(Vec::new())
            .with_head(1, 15)
            .with_head(46, 25),
        checkpoints.clone(),
        NoopDecoder,
        FailingEventWriter,
    );

    let error = runner
        .run_once()
        .await
        .expect_err("writer failure fails the batch");

    let error_chain = format!("{error:#}");
    assert!(
        error_chain
            .contains("write ORMP events chain_id=46 dataset=evm.logs from_block=10 to_block=14")
    );
    assert!(error_chain.contains("write failed"));
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
        RecordingDatalensReader::new(Vec::new())
            .with_head(1, 15)
            .with_head(46, 25),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_runner_slow_chain_does_not_block_other_chain_checkpoint_progress() {
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
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    checkpoints
        .read_or_create(1, "evm.logs", 10)
        .await
        .expect("seed slow chain checkpoint");
    checkpoints
        .read_or_create(46, "evm.logs", 10)
        .await
        .expect("seed fast chain checkpoint");
    let runner = IndexerRunner::new(
        config,
        RecordingDatalensReader::new(Vec::new())
            .with_head(1, 15)
            .with_head(46, 15)
            .with_query_delay(1, Duration::from_millis(300)),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let run = tokio::spawn(async move { runner.run_once().await });
    let deadline = Instant::now() + Duration::from_millis(100);
    loop {
        if checkpoints.next_block(46, "evm.logs").await.unwrap() == 15 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fast chain checkpoint was not advanced before slow chain completed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let report = run
        .await
        .expect("runner task joins")
        .expect("multi-chain pass succeeds");

    assert_eq!(report.checkpoints_advanced, 2);
    assert_eq!(checkpoints.next_block(1, "evm.logs").await.unwrap(), 15);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_runner_loop_recovers_chain_errors_without_blocking_other_chains() {
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
        ("ORMPINDEXER_POLL_INTERVAL_SECS".to_owned(), "1".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    checkpoints
        .read_or_create(1, "evm.logs", 10)
        .await
        .expect("seed failing chain checkpoint");
    checkpoints
        .read_or_create(46, "evm.logs", 10)
        .await
        .expect("seed healthy chain checkpoint");
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 15)
        .with_query_failure(1, "provider failed");
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let run = tokio::spawn(async move { runner.run_loop().await });
    let deadline = Instant::now() + Duration::from_millis(100);
    loop {
        if checkpoints.next_block(46, "evm.logs").await.unwrap() == 15 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "healthy chain checkpoint was not advanced while another chain failed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(checkpoints.next_block(1, "evm.logs").await.unwrap(), 10);
    run.abort();
}
