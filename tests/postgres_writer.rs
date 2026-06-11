use sqlx::PgPool;

use ormpindexer::{
    database::{EventWriter, PostgresEventWriter, apply_migrations},
    schema::{ADDRESS_ORACLE, ADDRESS_RELAYER, ChainLogMetadata, EventSource, LegacyOrmPEvent},
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
    assert_table_count(&pool, "ormp_message_accepted", 1).await;
    assert_table_count(&pool, "ormp_message_assigned", 3).await;
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
    let late = sqlx::query_as::<_, (Option<String>, Option<String>)>(
        "SELECT oracle, relayer FROM ormp_message_accepted WHERE id = $1",
    )
    .bind("0xlate")
    .fetch_one(&pool)
    .await
    .expect("fetch late accepted row");
    assert_eq!(late, (None, None));
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

fn evm_metadata(id: &str) -> ChainLogMetadata {
    ChainLogMetadata {
        id: id.to_owned(),
        source: EventSource::Evm,
        chain_id: 46,
        block_number: 123,
        block_timestamp: 456,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 2,
        log_index: 3,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    }
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
