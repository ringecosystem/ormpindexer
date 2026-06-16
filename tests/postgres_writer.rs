use sqlx::PgPool;

use ormpindexer::{
    checkpoint::{BlockAnchor, CheckpointStore},
    config::FinalityMode,
    database::PostgresCheckpointStore,
    database::{EventWriter, PostgresEventWriter, apply_migrations},
    schema::{
        ADDRESS_ORACLE, ADDRESS_RELAYER, AssignmentConfig, ChainLogMetadata, EventSource,
        LegacyOrmPEvent,
    },
    schema::{LEGACY_MIXED_CASE_ACCEPTED_ID, LEGACY_MIXED_CASE_ACCEPTED_ORACLE},
};

#[tokio::test]
async fn test_postgres_writer_inserts_legacy_events_idempotently_and_backfills_assignments() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping Postgres writer test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let writer = PostgresEventWriter::new(pool.clone());
    let events = legacy_events();

    let written = writer
        .write_events(&events)
        .await
        .expect("write legacy events");
    let repeated = writer
        .write_events(&events)
        .await
        .expect("rewrite legacy events");

    assert_eq!(written, events.len());
    assert_eq!(repeated, events.len());
    assert_table_count(&pool, "ormp_hash_imported", 1).await;
    assert_table_count(&pool, "ormp_message_accepted", 8).await;
    assert_table_count(&pool, "ormp_message_assigned", 10).await;
    assert_table_count(&pool, "ormp_message_dispatched", 1).await;
    assert_table_count(&pool, "msgport_message_recv", 1).await;
    assert_table_count(&pool, "msgport_message_sent", 1).await;
    assert_table_count(&pool, "signature_pub_signature_submittion", 1).await;

    let accepted = sqlx::query_as::<
        _,
        (
            Option<String>,
            Option<bool>,
            Option<String>,
            Option<String>,
            Option<bool>,
            Option<String>,
        ),
    >(
        r#"SELECT oracle, oracle_assigned, oracle_assigned_fee::TEXT,
                  relayer, relayer_assigned, relayer_assigned_fee::TEXT
           FROM ormp_message_accepted
           WHERE id = $1"#,
    )
    .bind("0xaccepted")
    .fetch_one(&pool)
    .await
    .expect("fetch accepted row");

    assert_eq!(accepted.0.as_deref(), Some(ADDRESS_ORACLE[0]));
    assert_eq!(accepted.1, Some(true));
    assert_eq!(accepted.2.as_deref(), Some("11"));
    assert_eq!(accepted.3.as_deref(), Some(ADDRESS_RELAYER[0]));
    assert_eq!(accepted.4, Some(true));
    assert_eq!(accepted.5.as_deref(), Some("22"));

    let b49e_positive = assignment_fields(&pool, "0xb49e-positive").await;
    assert_eq!(
        b49e_positive.0.as_deref(),
        Some("0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e")
    );
    assert_eq!(b49e_positive.1, Some(true));
    assert_eq!(b49e_positive.2.as_deref(), Some("2000000000000"));

    let b49e_negative = assignment_fields(&pool, "0xb49e-negative").await;
    assert_eq!(b49e_negative.0, None);
    assert_eq!(b49e_negative.1, None);
    assert_eq!(b49e_negative.2, None);

    let b49e_darwinia_positive = assignment_fields(&pool, "0xb49e-darwinia-positive").await;
    assert_eq!(
        b49e_darwinia_positive.0.as_deref(),
        Some("0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e")
    );
    assert_eq!(b49e_darwinia_positive.1, Some(true));
    assert_eq!(
        b49e_darwinia_positive.2.as_deref(),
        Some("1000000000000000000")
    );

    let b49e_darwinia_negative = assignment_fields(&pool, "0xb49e-darwinia-negative").await;
    assert_eq!(b49e_darwinia_negative.0, None);
    assert_eq!(b49e_darwinia_negative.1, None);
    assert_eq!(b49e_darwinia_negative.2, None);

    let b49e_arbitrum_positive = assignment_fields(&pool, "0xb49e-arbitrum-positive").await;
    assert_eq!(
        b49e_arbitrum_positive.0.as_deref(),
        Some("0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e")
    );
    assert_eq!(b49e_arbitrum_positive.1, Some(true));
    assert_eq!(b49e_arbitrum_positive.2.as_deref(), Some("3000000000000"));

    let b49e_arbitrum_negative = assignment_fields(&pool, "0xb49e-arbitrum-negative").await;
    assert_eq!(b49e_arbitrum_negative.0, None);
    assert_eq!(b49e_arbitrum_negative.1, None);
    assert_eq!(b49e_arbitrum_negative.2, None);

    let mixed_case = assignment_fields(&pool, LEGACY_MIXED_CASE_ACCEPTED_ID).await;
    assert_eq!(
        mixed_case.0.as_deref(),
        Some(LEGACY_MIXED_CASE_ACCEPTED_ORACLE)
    );
    assert_eq!(mixed_case.1, Some(true));
    assert_eq!(mixed_case.2.as_deref(), Some("77"));

    let writer = PostgresEventWriter::new(pool.clone());
    writer
        .write_events(&[LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata("late-accepted-log"),
            msg_hash: "0xlate".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 9,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 600_000,
            encoded: "0xencoded".to_owned(),
        }])
        .await
        .expect("write accepted after assigned");
    let late = sqlx::query_as::<
        _,
        (
            Option<String>,
            Option<bool>,
            Option<String>,
            Option<String>,
            Option<bool>,
            Option<String>,
        ),
    >(
        r#"SELECT oracle, oracle_assigned, oracle_assigned_fee::TEXT,
                  relayer, relayer_assigned, relayer_assigned_fee::TEXT
           FROM ormp_message_accepted
           WHERE id = $1"#,
    )
    .bind("0xlate")
    .fetch_one(&pool)
    .await
    .expect("fetch late accepted row");
    assert_eq!(late.0.as_deref(), Some(ADDRESS_ORACLE[0]));
    assert_eq!(late.1, Some(true));
    assert_eq!(late.2.as_deref(), Some("55"));
    assert_eq!(late.3.as_deref(), Some(ADDRESS_RELAYER[0]));
    assert_eq!(late.4, Some(true));
    assert_eq!(late.5.as_deref(), Some("66"));
}

#[tokio::test]
async fn test_postgres_rollback_deletes_legacy_rows_anchors_and_resets_checkpoint() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping Postgres rollback test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let checkpoints = PostgresCheckpointStore::new(pool.clone());
    checkpoints
        .read_or_create(46, "evm.logs", 10)
        .await
        .expect("seed checkpoint");
    checkpoints
        .advance(46, "evm.logs", 130)
        .await
        .expect("advance checkpoint");
    checkpoints
        .upsert_block_anchors(&[
            BlockAnchor {
                chain_id: 46,
                dataset: "evm.logs".to_owned(),
                block_number: 122,
                block_hash: "0xold".to_owned(),
                parent_hash: Some("0xold-parent".to_owned()),
                finality: FinalityMode::Latest,
            },
            BlockAnchor {
                chain_id: 46,
                dataset: "evm.logs".to_owned(),
                block_number: 123,
                block_hash: "0xremoved".to_owned(),
                parent_hash: Some("0xremoved-parent".to_owned()),
                finality: FinalityMode::Latest,
            },
        ])
        .await
        .expect("seed anchors");

    let writer = PostgresEventWriter::new(pool.clone());
    writer
        .write_events(&rollback_legacy_events())
        .await
        .expect("write rollback legacy events");

    checkpoints
        .rollback_legacy_from(46, "evm.logs", 123)
        .await
        .expect("rollback legacy rows");

    assert_table_count(&pool, "ormp_hash_imported", 0).await;
    assert_table_count(&pool, "ormp_message_accepted", 0).await;
    assert_table_count(&pool, "ormp_message_assigned", 0).await;
    assert_table_count(&pool, "ormp_message_dispatched", 0).await;
    assert_table_count(&pool, "msgport_message_recv", 0).await;
    assert_table_count(&pool, "msgport_message_sent", 0).await;
    assert_table_count(&pool, "signature_pub_signature_submittion", 0).await;
    assert_table_count(&pool, "ormp_indexer_block_anchor", 1).await;
    assert_eq!(
        checkpoints
            .read_or_create(46, "evm.logs", 10)
            .await
            .expect("read checkpoint")
            .next_block,
        123
    );
}

#[tokio::test]
async fn test_postgres_rollback_recomputes_assignment_backfills_for_remaining_accepteds() {
    let Some(database_url) = test_database_url() else {
        eprintln!(
            "skipping Postgres assignment rollback test; ORMPINDEXER_TEST_DATABASE_URL is not set"
        );
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let writer = PostgresEventWriter::new(pool.clone());
    writer
        .write_events(&[
            LegacyOrmPEvent::MessageAccepted {
                metadata: evm_metadata_at("rollback-clear-accepted-log", 46, 122),
                msg_hash: "0xrollback-clear".to_owned(),
                channel: "0xchannel".to_owned(),
                index: 1,
                from_chain_id: 1,
                from: "0xfrom".to_owned(),
                to_chain_id: 46,
                to: "0xto".to_owned(),
                gas_limit: 500_000,
                encoded: "0xencoded".to_owned(),
            },
            LegacyOrmPEvent::MessageAssigned {
                metadata: evm_metadata_at("rollback-clear-assigned-log", 46, 123),
                msg_hash: "0xrollback-clear".to_owned(),
                oracle: ADDRESS_ORACLE[0].to_owned(),
                relayer: ADDRESS_RELAYER[0].to_owned(),
                oracle_fee: 11,
                relayer_fee: 22,
                params: "0xparams".to_owned(),
            },
            LegacyOrmPEvent::MessageAccepted {
                metadata: evm_metadata_at("rollback-restore-accepted-log", 46, 121),
                msg_hash: "0xrollback-restore".to_owned(),
                channel: "0xchannel".to_owned(),
                index: 2,
                from_chain_id: 1,
                from: "0xfrom".to_owned(),
                to_chain_id: 46,
                to: "0xto".to_owned(),
                gas_limit: 500_000,
                encoded: "0xencoded".to_owned(),
            },
            LegacyOrmPEvent::MessageAssigned {
                metadata: evm_metadata_at("rollback-restore-old-assigned-log", 46, 122),
                msg_hash: "0xrollback-restore".to_owned(),
                oracle: ADDRESS_ORACLE[0].to_owned(),
                relayer: ADDRESS_RELAYER[0].to_owned(),
                oracle_fee: 33,
                relayer_fee: 44,
                params: "0xparams".to_owned(),
            },
            LegacyOrmPEvent::MessageAssigned {
                metadata: evm_metadata_at("rollback-restore-new-assigned-log", 46, 123),
                msg_hash: "0xrollback-restore".to_owned(),
                oracle: ADDRESS_ORACLE[1].to_owned(),
                relayer: ADDRESS_RELAYER[1].to_owned(),
                oracle_fee: 55,
                relayer_fee: 66,
                params: "0xparams".to_owned(),
            },
        ])
        .await
        .expect("write rollback assignment events");

    let before_restore = all_assignment_fields(&pool, "0xrollback-restore").await;
    assert_eq!(before_restore.0.as_deref(), Some(ADDRESS_ORACLE[1]));
    assert_eq!(before_restore.2.as_deref(), Some("55"));
    assert_eq!(before_restore.3.as_deref(), Some(ADDRESS_RELAYER[1]));
    assert_eq!(before_restore.5.as_deref(), Some("66"));

    let checkpoints = PostgresCheckpointStore::new(pool.clone());
    checkpoints
        .rollback_legacy_from(46, "evm.logs", 123)
        .await
        .expect("rollback legacy rows");

    let remaining_assignments = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM ormp_message_assigned WHERE block_number >= 123",
    )
    .fetch_one(&pool)
    .await
    .expect("count rolled back assignments");
    assert_eq!(remaining_assignments.0, 0);

    let clear = all_assignment_fields(&pool, "0xrollback-clear").await;
    assert_eq!(clear, (None, None, None, None, None, None));

    let restored = all_assignment_fields(&pool, "0xrollback-restore").await;
    assert_eq!(restored.0.as_deref(), Some(ADDRESS_ORACLE[0]));
    assert_eq!(restored.1, Some(true));
    assert_eq!(restored.2.as_deref(), Some("33"));
    assert_eq!(restored.3.as_deref(), Some(ADDRESS_RELAYER[0]));
    assert_eq!(restored.4, Some(true));
    assert_eq!(restored.5.as_deref(), Some("44"));
}

#[tokio::test]
async fn test_postgres_rollback_recomputes_assignment_backfills_with_configured_policy() {
    let Some(database_url) = test_database_url() else {
        eprintln!(
            "skipping Postgres configured assignment rollback test; ORMPINDEXER_TEST_DATABASE_URL is not set"
        );
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let oracle = "0x00000000000000000000000000000000000000a1";
    let relayer = "0x00000000000000000000000000000000000000b2";
    let assignment_config = AssignmentConfig {
        oracle_addresses: vec![oracle.to_owned()],
        relayer_addresses: vec![relayer.to_owned()],
    };
    let writer =
        PostgresEventWriter::with_assignment_config(pool.clone(), assignment_config.clone());
    writer
        .write_events(&[
            LegacyOrmPEvent::MessageAccepted {
                metadata: evm_metadata_at("rollback-configured-accepted-log", 46, 121),
                msg_hash: "0xrollback-configured".to_owned(),
                channel: "0xchannel".to_owned(),
                index: 3,
                from_chain_id: 1,
                from: "0xfrom".to_owned(),
                to_chain_id: 46,
                to: "0xto".to_owned(),
                gas_limit: 500_000,
                encoded: "0xencoded".to_owned(),
            },
            LegacyOrmPEvent::MessageAssigned {
                metadata: evm_metadata_at("rollback-configured-old-assigned-log", 46, 122),
                msg_hash: "0xrollback-configured".to_owned(),
                oracle: oracle.to_owned(),
                relayer: relayer.to_owned(),
                oracle_fee: 33,
                relayer_fee: 44,
                params: "0xparams".to_owned(),
            },
            LegacyOrmPEvent::MessageAssigned {
                metadata: evm_metadata_at("rollback-configured-new-assigned-log", 46, 123),
                msg_hash: "0xrollback-configured".to_owned(),
                oracle: oracle.to_owned(),
                relayer: relayer.to_owned(),
                oracle_fee: 55,
                relayer_fee: 66,
                params: "0xparams".to_owned(),
            },
        ])
        .await
        .expect("write configured assignment events");

    let before = all_assignment_fields(&pool, "0xrollback-configured").await;
    assert_eq!(before.0.as_deref(), Some(oracle));
    assert_eq!(before.2.as_deref(), Some("55"));
    assert_eq!(before.3.as_deref(), Some(relayer));
    assert_eq!(before.5.as_deref(), Some("66"));

    let checkpoints =
        PostgresCheckpointStore::with_assignment_config(pool.clone(), assignment_config);
    checkpoints
        .rollback_legacy_from(46, "evm.logs", 123)
        .await
        .expect("rollback legacy rows");

    let restored = all_assignment_fields(&pool, "0xrollback-configured").await;
    assert_eq!(restored.0.as_deref(), Some(oracle));
    assert_eq!(restored.1, Some(true));
    assert_eq!(restored.2.as_deref(), Some("33"));
    assert_eq!(restored.3.as_deref(), Some(relayer));
    assert_eq!(restored.4, Some(true));
    assert_eq!(restored.5.as_deref(), Some("44"));
}

#[tokio::test]
async fn test_postgres_rollback_deletes_signature_rows_by_indexed_source_chain() {
    let Some(database_url) = test_database_url() else {
        eprintln!(
            "skipping Postgres signature rollback test; ORMPINDEXER_TEST_DATABASE_URL is not set"
        );
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let checkpoints = PostgresCheckpointStore::new(pool.clone());
    checkpoints
        .upsert_block_anchors(&[BlockAnchor {
            chain_id: 46,
            dataset: "evm.logs".to_owned(),
            block_number: 123,
            block_hash: "0xblock".to_owned(),
            parent_hash: Some("0xparent".to_owned()),
            finality: FinalityMode::Latest,
        }])
        .await
        .expect("seed source anchor");

    let writer = PostgresEventWriter::new(pool.clone());
    writer
        .write_events(&[LegacyOrmPEvent::SignatureSubmittion {
            metadata: evm_metadata_at("rollback-signature-payload-chain-log", 46, 123),
            chain_id: 1,
            channel: "0xchannel".to_owned(),
            signer: "0xsigner".to_owned(),
            msg_index: 9,
            signature: "0xsig".to_owned(),
            data: "0xdata".to_owned(),
        }])
        .await
        .expect("write signature submittion");
    assert_table_count(&pool, "signature_pub_signature_submittion", 1).await;

    checkpoints
        .rollback_legacy_from(46, "evm.logs", 123)
        .await
        .expect("rollback legacy rows");

    assert_table_count(&pool, "signature_pub_signature_submittion", 0).await;
}

fn legacy_events() -> Vec<LegacyOrmPEvent> {
    vec![
        LegacyOrmPEvent::HashImported {
            metadata: evm_metadata("hash-log"),
            src_chain_id: 1,
            target_chain_id: 46,
            oracle: ADDRESS_ORACLE[0].to_owned(),
            channel: "0xchannel".to_owned(),
            msg_index: 7,
            hash: "0xhash".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata("accepted-log"),
            msg_hash: "0xaccepted".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 8,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata_at("b49e-positive-log", 1, 22_474_070),
            msg_hash: "0xb49e-positive".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 9,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata_at("b49e-negative-log", 1, 22_336_887),
            msg_hash: "0xb49e-negative".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 10,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 42_161,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata_at("b49e-darwinia-positive-log", 46, 6_634_860),
            msg_hash: "0xb49e-darwinia-positive".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 11,
            from_chain_id: 46,
            from: "0xfrom".to_owned(),
            to_chain_id: 44,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata_at("b49e-darwinia-negative-log", 46, 6_614_836),
            msg_hash: "0xb49e-darwinia-negative".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 12,
            from_chain_id: 46,
            from: "0xfrom".to_owned(),
            to_chain_id: 1,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata_at("b49e-arbitrum-positive-log", 42_161, 334_644_126),
            msg_hash: "0xb49e-arbitrum-positive".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 13,
            from_chain_id: 42_161,
            from: "0xfrom".to_owned(),
            to_chain_id: 2_818,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata_at("b49e-arbitrum-negative-log", 42_161, 333_775_437),
            msg_hash: "0xb49e-arbitrum-negative".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 14,
            from_chain_id: 42_161,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata("mixed-case-accepted-log"),
            msg_hash: LEGACY_MIXED_CASE_ACCEPTED_ID.to_owned(),
            channel: "0xchannel".to_owned(),
            index: 15,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("assigned-log"),
            msg_hash: "0xaccepted".to_owned(),
            oracle: ADDRESS_ORACLE[0].to_owned(),
            relayer: ADDRESS_RELAYER[0].to_owned(),
            oracle_fee: 11,
            relayer_fee: 22,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("unmatched-assigned-log"),
            msg_hash: "0xaccepted".to_owned(),
            oracle: "0x0000000000000000000000000000000000000001".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 33,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("early-assigned-log"),
            msg_hash: "0xlate".to_owned(),
            oracle: ADDRESS_ORACLE[0].to_owned(),
            relayer: ADDRESS_RELAYER[0].to_owned(),
            oracle_fee: 55,
            relayer_fee: 66,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("b49e-positive-assigned-log"),
            msg_hash: "0xb49e-positive".to_owned(),
            oracle: "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 2_000_000_000_000,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("b49e-negative-assigned-log"),
            msg_hash: "0xb49e-negative".to_owned(),
            oracle: "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 40_000_000_000_000,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("b49e-darwinia-positive-assigned-log"),
            msg_hash: "0xb49e-darwinia-positive".to_owned(),
            oracle: "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 1_000_000_000_000_000_000,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("b49e-darwinia-negative-assigned-log"),
            msg_hash: "0xb49e-darwinia-negative".to_owned(),
            oracle: "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 1_000_000_000_000_000_000,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("b49e-arbitrum-positive-assigned-log"),
            msg_hash: "0xb49e-arbitrum-positive".to_owned(),
            oracle: "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 3_000_000_000_000,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("b49e-arbitrum-negative-assigned-log"),
            msg_hash: "0xb49e-arbitrum-negative".to_owned(),
            oracle: "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e".to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 3_000_000_000_000,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("mixed-case-assigned-log"),
            msg_hash: LEGACY_MIXED_CASE_ACCEPTED_ID.to_owned(),
            oracle: ADDRESS_ORACLE[0].to_owned(),
            relayer: "0x0000000000000000000000000000000000000002".to_owned(),
            oracle_fee: 77,
            relayer_fee: 44,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageDispatched {
            metadata: evm_metadata("dispatched-log"),
            target_chain_id: 46,
            msg_hash: "0xdispatched".to_owned(),
            dispatch_result: true,
        },
        LegacyOrmPEvent::MsgportMessageRecv {
            metadata: evm_metadata("recv-log"),
            msg_id: "0xmsgid".to_owned(),
            result: true,
            return_data: "0xreturn".to_owned(),
        },
        LegacyOrmPEvent::MsgportMessageSent {
            metadata: evm_metadata("sent-log"),
            msg_id: "0xmsgid".to_owned(),
            from_dapp: "0xfromdapp".to_owned(),
            to_chain_id: 728_126_428,
            to_dapp: "0xtodapp".to_owned(),
            message: "0xmessage".to_owned(),
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::SignatureSubmittion {
            metadata: evm_metadata("sig-log"),
            chain_id: 46,
            channel: "0xchannel".to_owned(),
            signer: "0xsigner".to_owned(),
            msg_index: 99,
            signature: "0xsig".to_owned(),
            data: "0xdata".to_owned(),
        },
    ]
}

fn rollback_legacy_events() -> Vec<LegacyOrmPEvent> {
    vec![
        LegacyOrmPEvent::HashImported {
            metadata: evm_metadata("rollback-hash-log"),
            src_chain_id: 1,
            target_chain_id: 46,
            oracle: ADDRESS_ORACLE[0].to_owned(),
            channel: "0xchannel".to_owned(),
            msg_index: 7,
            hash: "0xhash".to_owned(),
        },
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata("rollback-accepted-log"),
            msg_hash: "0xrollback-accepted".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 8,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("rollback-assigned-log"),
            msg_hash: "0xrollback-accepted".to_owned(),
            oracle: ADDRESS_ORACLE[0].to_owned(),
            relayer: ADDRESS_RELAYER[0].to_owned(),
            oracle_fee: 11,
            relayer_fee: 22,
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::MessageDispatched {
            metadata: evm_metadata("rollback-dispatched-log"),
            target_chain_id: 46,
            msg_hash: "0xrollback-accepted".to_owned(),
            dispatch_result: true,
        },
        LegacyOrmPEvent::MsgportMessageRecv {
            metadata: evm_metadata("rollback-recv-log"),
            msg_id: "0xmsgid".to_owned(),
            result: true,
            return_data: "0xreturn".to_owned(),
        },
        LegacyOrmPEvent::MsgportMessageSent {
            metadata: evm_metadata("rollback-sent-log"),
            msg_id: "0xmsgid".to_owned(),
            from_dapp: "0xfromdapp".to_owned(),
            to_chain_id: 46,
            to_dapp: "0xtodapp".to_owned(),
            message: "0xmessage".to_owned(),
            params: "0xparams".to_owned(),
        },
        LegacyOrmPEvent::SignatureSubmittion {
            metadata: evm_metadata("rollback-signature-log"),
            chain_id: 46,
            channel: "0xchannel".to_owned(),
            signer: "0xsigner".to_owned(),
            msg_index: 9,
            signature: "0xsig".to_owned(),
            data: "0xdata".to_owned(),
        },
    ]
}

fn evm_metadata(id: &str) -> ChainLogMetadata {
    evm_metadata_at(id, 46, 123)
}

fn evm_metadata_at(id: &str, chain_id: u128, block_number: u128) -> ChainLogMetadata {
    ChainLogMetadata {
        id: id.to_owned(),
        source: EventSource::Evm,
        chain_id,
        block_number,
        block_hash: Some("0xblock".to_owned()),
        block_timestamp: 456,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 2,
        log_index: 3,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    }
}

async fn assignment_fields(
    pool: &PgPool,
    msg_hash: &str,
) -> (Option<String>, Option<bool>, Option<String>) {
    sqlx::query_as::<_, (Option<String>, Option<bool>, Option<String>)>(
        r#"SELECT oracle, oracle_assigned, oracle_assigned_fee::TEXT
           FROM ormp_message_accepted
           WHERE id = $1"#,
    )
    .bind(msg_hash)
    .fetch_one(pool)
    .await
    .expect("fetch accepted assignment fields")
}

async fn all_assignment_fields(
    pool: &PgPool,
    msg_hash: &str,
) -> (
    Option<String>,
    Option<bool>,
    Option<String>,
    Option<String>,
    Option<bool>,
    Option<String>,
) {
    sqlx::query_as::<
        _,
        (
            Option<String>,
            Option<bool>,
            Option<String>,
            Option<String>,
            Option<bool>,
            Option<String>,
        ),
    >(
        r#"SELECT oracle, oracle_assigned, oracle_assigned_fee::TEXT,
                  relayer, relayer_assigned, relayer_assigned_fee::TEXT
           FROM ormp_message_accepted
           WHERE id = $1"#,
    )
    .bind(msg_hash)
    .fetch_one(pool)
    .await
    .expect("fetch accepted assignment fields")
}

async fn assert_table_count(pool: &PgPool, table: &str, expected: i64) {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let (count,) = sqlx::query_as::<_, (i64,)>(&sql)
        .fetch_one(pool)
        .await
        .expect("count table rows");
    assert_eq!(count, expected, "unexpected row count for {table}");
}

async fn truncate_legacy_tables(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE
            ormp_indexer_block_anchor,
            ormp_indexer_checkpoint,
            ormp_hash_imported,
            ormp_message_accepted,
            ormp_message_assigned,
            ormp_message_dispatched,
            msgport_message_recv,
            msgport_message_sent,
            signature_pub_signature_submittion",
    )
    .execute(pool)
    .await
    .expect("truncate legacy tables");
}

fn test_database_url() -> Option<String> {
    std::env::var("ORMPINDEXER_TEST_DATABASE_URL").ok()
}
