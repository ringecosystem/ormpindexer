use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use ormpindexer::{
    checkpoint::{BlockAnchor, CheckpointStore, InMemoryCheckpointStore},
    config::{FinalityMode, RuntimeConfig},
    database::DryRunEventWriter,
    datalens::{DatalensBlock, DatalensLog, IncompleteReason},
    decoder::NoopDecoder,
    runner::{IndexerRunner, RunnerReport},
    schema::LegacyOrmPEvent,
};

mod runtime_support;

use runtime_support::{
    EchoTransactionFromDecoder, FailingEventWriter, RecordingDatalensReader, RecordingEventWriter,
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
        parent_hash: None,
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
async fn test_runner_keeps_transaction_from_supplied_on_the_log_row() {
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
    let reader = RecordingDatalensReader::new(vec![DatalensLog {
        id: Some("46-12-0".to_owned()),
        chain_id: 46,
        block_number: 12,
        block_hash: None,
        parent_hash: None,
        block_timestamp: Some(1_700_000_000_000),
        transaction_hash: format!("0x{}", "aa".repeat(32)),
        transaction_index: Some(0),
        log_index: 1,
        address: "0x333".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
        topics: vec!["0xaaa".to_owned()],
        data: "0x".to_owned(),
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    }])
    .with_head(46, 15);
    let queries = reader.queries.clone();
    let writer = RecordingEventWriter::default();
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
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
    assert_eq!(queries.lock().expect("queries lock").len(), 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
}

#[tokio::test]
async fn test_runner_stops_checkpoint_at_served_end_when_the_range_is_incomplete() {
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
    let reader = RecordingDatalensReader::new(vec![DatalensLog {
        id: Some("46-11-0".to_owned()),
        chain_id: 46,
        block_number: 11,
        block_hash: Some("0xblock".to_owned()),
        parent_hash: None,
        block_timestamp: Some(1_700_000_000_000),
        transaction_hash: "0xtx".to_owned(),
        transaction_index: Some(0),
        log_index: 1,
        address: "0x333".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
        topics: vec!["0xaaa".to_owned()],
        data: "0x".to_owned(),
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    }])
    .with_head(46, 15)
    .with_partial(46, 10, 14, Some(11), IncompleteReason::NotFollowedYet);
    let queries = reader.queries.clone();
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("partial batch succeeds");

    assert_eq!(report.ranges_queried, 1);
    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(queries.lock().expect("queries lock").len(), 1);
    assert_eq!(queries.lock().expect("queries lock")[0].from_block, 10);
    assert_eq!(queries.lock().expect("queries lock")[0].to_block, 14);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 12);
}

#[tokio::test]
async fn test_runner_rolls_back_first_mismatched_anchor_before_processing_safe_range() {
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
        ("ORMPINDEXER_START_BLOCK".to_owned(), "20".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "safe".to_owned()),
        (
            "ORMPINDEXER_REORG_WINDOW_BLOCKS".to_owned(),
            "10".to_owned(),
        ),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    checkpoints
        .read_or_create(46, "evm.logs", 20)
        .await
        .expect("seed checkpoint");
    checkpoints
        .advance(46, "evm.logs", 24)
        .await
        .expect("advance checkpoint");
    checkpoints
        .upsert_block_anchors(&[
            BlockAnchor {
                chain_id: 46,
                dataset: "evm.logs".to_owned(),
                block_number: 20,
                block_hash: "0xold-20".to_owned(),
                parent_hash: Some("0xparent-19".to_owned()),
                finality: FinalityMode::Safe,
            },
            BlockAnchor {
                chain_id: 46,
                dataset: "evm.logs".to_owned(),
                block_number: 21,
                block_hash: "0xhash-21".to_owned(),
                parent_hash: Some("0xnew-20".to_owned()),
                finality: FinalityMode::Safe,
            },
        ])
        .await
        .expect("seed anchors");
    let reader = RecordingDatalensReader::new(vec![
        DatalensLog {
            id: Some("46-20-0".to_owned()),
            chain_id: 46,
            block_number: 20,
            block_hash: Some("0xnew-20".to_owned()),
            parent_hash: Some("0xparent-19".to_owned()),
            block_timestamp: Some(1_700_000_000_000),
            transaction_hash: "0xtx20".to_owned(),
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
        },
        DatalensLog {
            id: Some("46-21-0".to_owned()),
            chain_id: 46,
            block_number: 21,
            block_hash: Some("0xhash-21".to_owned()),
            parent_hash: Some("0xnew-20".to_owned()),
            block_timestamp: Some(1_700_000_000_000),
            transaction_hash: "0xtx21".to_owned(),
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
        },
    ])
    .with_blocks(vec![
        datalens_block(20, "0xnew-20", "0xparent-19"),
        datalens_block(21, "0xhash-21", "0xnew-20"),
        datalens_block(22, "0xhash-22", "0xhash-21"),
        datalens_block(23, "0xhash-23", "0xhash-22"),
    ])
    .with_head(46, 24);
    let block_queries = reader.block_queries();
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("safe pass succeeds");

    assert_eq!(report.ranges_queried, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 24);
    {
        let queries = reader.queries.lock().expect("queries lock");
        assert_eq!(
            queries
                .iter()
                .map(|query| (query.from_block, query.to_block, query.finality_mode))
                .collect::<Vec<_>>(),
            vec![(20, 23, FinalityMode::Safe)]
        );
    }
    assert_eq!(
        block_queries
            .lock()
            .expect("block queries lock")
            .iter()
            .map(|query| (query.from_block, query.to_block, query.finality_mode))
            .collect::<Vec<_>>(),
        vec![(20, 23, FinalityMode::Safe), (20, 23, FinalityMode::Safe)]
    );
    let anchors = checkpoints
        .read_block_anchors(46, "evm.logs", 20, 21)
        .await
        .expect("read anchors");
    assert_eq!(anchors[0].block_hash, "0xnew-20");
}

#[tokio::test]
async fn test_runner_rolls_back_empty_block_that_later_gains_event_after_reorg() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "10".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "20".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "safe".to_owned()),
        ("ORMPINDEXER_REORG_WINDOW_BLOCKS".to_owned(), "5".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    checkpoints
        .read_or_create(46, "evm.logs", 20)
        .await
        .expect("seed checkpoint");
    checkpoints
        .advance(46, "evm.logs", 25)
        .await
        .expect("advance checkpoint");
    checkpoints
        .upsert_block_anchors(&[
            block_anchor(20, "0xhash-20", "0xparent-19", FinalityMode::Safe),
            block_anchor(21, "0xhash-21", "0xhash-20", FinalityMode::Safe),
            block_anchor(23, "0xold-23", "0xold-22", FinalityMode::Safe),
            block_anchor(24, "0xold-24", "0xold-23", FinalityMode::Safe),
        ])
        .await
        .expect("seed anchors");
    let reader = RecordingDatalensReader::new(vec![
        datalens_log(20, "0xhash-20", "0xparent-19"),
        datalens_log(21, "0xhash-21", "0xhash-20"),
        datalens_log(22, "0xnew-22", "0xhash-21"),
        datalens_log(23, "0xnew-23", "0xnew-22"),
        datalens_log(24, "0xnew-24", "0xnew-23"),
    ])
    .with_blocks(vec![
        datalens_block(20, "0xhash-20", "0xparent-19"),
        datalens_block(21, "0xhash-21", "0xhash-20"),
        datalens_block(22, "0xnew-22", "0xhash-21"),
        datalens_block(23, "0xnew-23", "0xnew-22"),
        datalens_block(24, "0xnew-24", "0xnew-23"),
    ])
    .with_head(46, 25);
    let block_queries = reader.block_queries();
    let runner = IndexerRunner::new(
        config,
        reader.clone(),
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("safe pass succeeds");

    assert_eq!(report.ranges_queried, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 25);
    {
        let queries = reader.queries.lock().expect("queries lock");
        assert_eq!(
            queries
                .iter()
                .map(|query| (query.from_block, query.to_block, query.finality_mode))
                .collect::<Vec<_>>(),
            vec![(22, 24, FinalityMode::Safe)]
        );
    }
    assert_eq!(
        block_queries
            .lock()
            .expect("block queries lock")
            .iter()
            .map(|query| (query.from_block, query.to_block, query.finality_mode))
            .collect::<Vec<_>>(),
        vec![(20, 24, FinalityMode::Safe), (22, 24, FinalityMode::Safe)]
    );
    let anchors = checkpoints
        .read_block_anchors(46, "evm.logs", 20, 24)
        .await
        .expect("read anchors");
    assert_eq!(
        anchors
            .iter()
            .map(|anchor| (anchor.block_number, anchor.block_hash.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (20, "0xhash-20"),
            (21, "0xhash-21"),
            (22, "0xnew-22"),
            (23, "0xnew-23"),
            (24, "0xnew-24"),
        ]
    );
}

#[tokio::test]
async fn test_runner_writes_anchors_for_empty_blocks_in_reorg_finality_modes() {
    for finality_mode in [FinalityMode::Safe, FinalityMode::Latest] {
        let finality = finality_mode.as_str().to_owned();
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
            ("ORMPINDEXER_START_BLOCK".to_owned(), "20".to_owned()),
            ("ORMPINDEXER_FINALITY_MODE".to_owned(), finality),
            ("ORMPINDEXER_REORG_WINDOW_BLOCKS".to_owned(), "5".to_owned()),
        ]);
        let config = RuntimeConfig::from_env_map(&env).expect("config parses");
        let checkpoints = InMemoryCheckpointStore::default();
        let reader = RecordingDatalensReader::new(vec![datalens_log(22, "0xhash-22", "0xhash-21")])
            .with_blocks(vec![
                datalens_block(20, "0xhash-20", "0xparent-19"),
                datalens_block(21, "0xhash-21", "0xhash-20"),
                datalens_block(22, "0xhash-22", "0xhash-21"),
                datalens_block(23, "0xhash-23", "0xhash-22"),
                datalens_block(24, "0xhash-24", "0xhash-23"),
            ])
            .with_head(46, 25);
        let runner = IndexerRunner::new(
            config,
            reader,
            checkpoints.clone(),
            NoopDecoder,
            DryRunEventWriter,
        );

        let report = runner.run_once().await.expect("batch pass succeeds");

        assert_eq!(report.checkpoints_advanced, 1);
        assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 25);
        let anchors = checkpoints
            .read_block_anchors(46, "evm.logs", 20, 24)
            .await
            .expect("read anchors");
        assert_eq!(
            anchors
                .iter()
                .map(|anchor| (anchor.block_number, anchor.finality))
                .collect::<Vec<_>>(),
            vec![
                (20, finality_mode),
                (21, finality_mode),
                (22, finality_mode),
                (23, finality_mode),
                (24, finality_mode),
            ]
        );
    }
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
async fn test_runner_query_failure_does_not_advance_checkpoint() {
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
        .with_query_failure(46, "permission denied");
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
        .expect_err("query failure stops the chain pass");

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
            .contains("write ORMP events chain_id=46 dataset=evm.logs from_block=10 served_end=14")
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

#[tokio::test]
async fn test_runner_keeps_checkpoint_when_the_server_serves_nothing() {
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
    let before = incomplete_serves(46, "provider_error");
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 15)
        .with_partial(46, 10, 14, None, IncompleteReason::ProviderError);
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("empty serve stops the poll");

    assert_eq!(report.ranges_queried, 1);
    assert_eq!(report.checkpoints_advanced, 0);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
    assert_eq!(incomplete_serves(46, "provider_error"), before + 1);
}

#[tokio::test]
async fn test_runner_rejects_a_served_range_that_does_not_start_at_the_request() {
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
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 15)
        .with_served_window(46, 10, 14, 12, 14);
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let error = runner
        .run_once()
        .await
        .expect_err("served start must match the request");

    assert!(format!("{error:#}").contains("served start 12 does not match from_block 10"));
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 10);
}

#[tokio::test]
async fn test_runner_clamps_checkpoint_to_the_requested_end() {
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
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 15)
        .with_served_window(46, 10, 14, 10, 99);
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("clamped serve advances");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
}

#[tokio::test]
async fn test_runner_queries_anchors_only_through_served_end_in_safe_mode() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "20".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "safe".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 30)
        .with_blocks(
            (10..=14)
                .map(|number| datalens_block(number, "0xhash", "0xparent"))
                .collect(),
        )
        .with_partial(46, 10, 29, Some(14), IncompleteReason::NotFollowedYet);
    let block_queries = reader.block_queries();
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("short safe prefix advances");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
    assert_eq!(
        block_queries
            .lock()
            .expect("block queries")
            .iter()
            .map(|query| (query.from_block, query.to_block, query.finality_mode))
            .collect::<Vec<_>>(),
        vec![(10, 14, FinalityMode::Safe)]
    );
}

#[tokio::test]
async fn test_runner_skips_block_headers_at_or_below_the_finalized_head() {
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
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "20".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "10".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "safe".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 30)
        .with_finalized_head(46, 20)
        .with_blocks(
            (21..=29)
                .map(|number| datalens_block(number, "0xhash", "0xparent"))
                .collect(),
        );
    let block_queries = reader.block_queries();
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner
        .run_once()
        .await
        .expect("unfinalized suffix is anchored");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 30);
    assert_eq!(
        block_queries
            .lock()
            .expect("block queries")
            .iter()
            .map(|query| (query.from_block, query.to_block))
            .collect::<Vec<_>>(),
        vec![(21, 29)]
    );
    let anchors = checkpoints
        .read_block_anchors(46, "evm.logs", 10, 29)
        .await
        .expect("anchors");
    assert_eq!(
        anchors
            .iter()
            .map(|anchor| anchor.block_number)
            .collect::<Vec<_>>(),
        (21..=29).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_runner_does_not_query_blocks_when_the_served_range_is_finalized() {
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
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "safe".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let reader = RecordingDatalensReader::new(Vec::new())
        .with_head(46, 15)
        .with_finalized_head(46, 100);
    let block_queries = reader.block_queries();
    let checkpoints = InMemoryCheckpointStore::default();
    let runner = IndexerRunner::new(
        config,
        reader,
        checkpoints.clone(),
        NoopDecoder,
        DryRunEventWriter,
    );

    let report = runner.run_once().await.expect("finalized history advances");

    assert_eq!(report.checkpoints_advanced, 1);
    assert_eq!(checkpoints.next_block(46, "evm.logs").await.unwrap(), 15);
    assert!(block_queries.lock().expect("block queries").is_empty());
    assert!(
        checkpoints
            .read_block_anchors(46, "evm.logs", 10, 14)
            .await
            .expect("anchors")
            .is_empty()
    );
}

fn incomplete_serves(chain_id: u64, reason: &str) -> u64 {
    ormpindexer::metrics::snapshot()
        .runtime_rows
        .into_iter()
        .find(|row| row.chain_id == chain_id && row.dataset == "evm.logs")
        .and_then(|row| row.incomplete_serves_total.get(reason).copied())
        .unwrap_or(0)
}

fn block_anchor(
    block_number: u64,
    block_hash: &str,
    parent_hash: &str,
    finality: FinalityMode,
) -> BlockAnchor {
    BlockAnchor {
        chain_id: 46,
        dataset: "evm.logs".to_owned(),
        block_number,
        block_hash: block_hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        finality,
    }
}

fn datalens_log(block_number: u64, block_hash: &str, parent_hash: &str) -> DatalensLog {
    DatalensLog {
        id: Some(format!("46-{block_number}-0")),
        chain_id: 46,
        block_number,
        block_hash: Some(block_hash.to_owned()),
        parent_hash: Some(parent_hash.to_owned()),
        block_timestamp: Some(1_700_000_000_000),
        transaction_hash: format!("0xtx{block_number}"),
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
    }
}

fn datalens_block(block_number: u64, block_hash: &str, parent_hash: &str) -> DatalensBlock {
    DatalensBlock {
        chain_id: 46,
        block_number,
        block_hash: block_hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
    }
}
