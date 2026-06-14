use async_graphql::Request;
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};

use ormpindexer::{
    database::{EventWriter, PostgresEventWriter, apply_migrations},
    graphql::build_schema,
    schema::{ADDRESS_ORACLE, ADDRESS_RELAYER, ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

#[tokio::test]
async fn test_graphql_schema_exposes_pages_without_connections() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/ormpindexer")
        .expect("lazy postgres pool");
    let schema = build_schema(pool);
    let sdl = schema.sdl();

    for page_type in [
        "type ORMPHashImportedPage",
        "type ORMPMessageAcceptedPage",
        "type ORMPMessageAssignedPage",
        "type ORMPMessageDispatchedPage",
        "type MsgportMessageRecvPage",
        "type MsgportMessageSentPage",
        "type SignaturePubSignatureSubmittionPage",
    ] {
        assert!(sdl.contains(page_type), "schema is missing {page_type}");
    }
    assert!(
        !sdl.contains("Connection"),
        "schema must not expose connection fields or types"
    );
    assert!(sdl.contains("scalar BigInt"), "schema must expose BigInt");
    assert!(
        sdl.contains("blockNumber: BigInt!"),
        "legacy numeric fields must stay BigInt"
    );
    assert!(
        sdl.contains("where: LegacyWhereInput"),
        "list fields must accept OpenReader-style where filters"
    );
    assert!(
        sdl.contains("orderBy: [LegacyOrderByInput!]"),
        "list fields must accept OpenReader-style orderBy"
    );
    #[cfg(feature = "legacy-query-compat")]
    {
        for legacy_query_compat_name in [
            "index_ASC",
            "msgIndex_DESC",
            "index_gt",
            "oracleAssigned_eq",
            "relayerAssigned_eq",
            "signer_in",
        ] {
            assert!(
                sdl.contains(legacy_query_compat_name),
                "schema is missing legacy query compatibility name {legacy_query_compat_name}"
            );
        }
    }
}

#[tokio::test]
async fn test_graphql_queries_by_id_and_page_against_postgres() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping GraphQL Postgres test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;
    PostgresEventWriter::new(pool.clone())
        .write_events(&legacy_events())
        .await
        .expect("write legacy events");

    let schema = build_schema(pool);
    let response = schema
        .execute(Request::new(
            r#"
            query {
              ormpMessageAcceptedById(id: "0xaccepted") {
                id
                msgHash
                blockNumber
                gasLimit
                oracleAssignedFee
              }
              ormpMessageAcceptedsPage(offset: 0, limit: 10) {
                totalCount
                offset
                limit
                items {
                  id
                  msgHash
                  blockNumber
                  gasLimit
                  oracleAssignedFee
                }
              }
              filtered: ormpMessageAccepteds(
                where: { msgHash_eq: "0xaccepted", blockNumber_gte: "100" }
                orderBy: [blockNumber_DESC]
              ) {
                id
                blockNumber
              }
            }
            "#,
        ))
        .await;

    assert!(
        response.errors.is_empty(),
        "unexpected GraphQL errors: {:?}",
        response.errors
    );
    assert_eq!(
        response.data.into_json().expect("GraphQL response JSON"),
        json!({
            "ormpMessageAcceptedById": {
                "id": "0xaccepted",
                "msgHash": "0xaccepted",
                "blockNumber": "123",
                "gasLimit": "500000",
                "oracleAssignedFee": "11",
            },
            "ormpMessageAcceptedsPage": {
                "totalCount": 1,
                "offset": 0,
                "limit": 10,
                "items": [{
                    "id": "0xaccepted",
                    "msgHash": "0xaccepted",
                    "blockNumber": "123",
                    "gasLimit": "500000",
                    "oracleAssignedFee": "11",
                }],
            },
            "filtered": [{
                "id": "0xaccepted",
                "blockNumber": "123",
            }],
        })
    );
}

#[cfg(feature = "legacy-query-compat")]
#[tokio::test]
async fn test_graphql_accepts_ormpipe_legacy_query_compatibility_filters() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping GraphQL Postgres test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;
    seed_ormpipe_compatibility_rows(&pool).await;

    let schema = build_schema(pool);
    let response = schema
        .execute(Request::new(
            r#"
            query {
              nextOracle: ormpMessageAccepteds(
                limit: 1
                orderBy: [index_ASC]
                where: {
                  oracleAssigned_eq: true
                  index_gt: "8"
                  fromChainId_eq: "1"
                  toChainId_eq: "46"
                }
              ) {
                msgHash
                index
              }
              relayerHashes: ormpMessageAccepteds(
                limit: 10
                orderBy: [index_ASC]
                where: {
                  relayerAssigned_eq: true
                  index_lte: "10"
                  fromChainId_eq: "1"
                  toChainId_eq: "46"
                }
              ) {
                msgHash
                index
              }
              lastImported: ormpHashImporteds(
                limit: 1
                orderBy: [msgIndex_DESC]
                where: {
                  srcChainId_eq: "1"
                  targetChainId_eq: "46"
                }
              ) {
                msgIndex
                hash
              }
              topSignatures: signaturePubSignatureSubmittions(
                limit: 100
                orderBy: [blockNumber_DESC]
                where: {
                  chainId_eq: "1"
                  msgIndex_eq: "9"
                  signer_in: ["0xsigner-a", "0xsigner-c"]
                }
              ) {
                signer
                msgIndex
                blockNumber
              }
            }
            "#,
        ))
        .await;

    assert!(
        response.errors.is_empty(),
        "unexpected GraphQL errors: {:?}",
        response.errors
    );
    assert_eq!(
        response.data.into_json().expect("GraphQL response JSON"),
        json!({
            "nextOracle": [{
                "msgHash": "0xaccepted-9",
                "index": "9",
            }],
            "relayerHashes": [{
                "msgHash": "0xaccepted-8",
                "index": "8",
            }, {
                "msgHash": "0xaccepted-9",
                "index": "9",
            }],
            "lastImported": [{
                "msgIndex": "9",
                "hash": "0xaccepted-9",
            }],
            "topSignatures": [{
                "signer": "0xsigner-c",
                "msgIndex": "9",
                "blockNumber": "125",
            }, {
                "signer": "0xsigner-a",
                "msgIndex": "9",
                "blockNumber": "124",
            }],
        })
    );
}

fn legacy_events() -> Vec<LegacyOrmPEvent> {
    vec![
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
    ]
}

fn evm_metadata(id: &str) -> ChainLogMetadata {
    ChainLogMetadata {
        id: id.to_owned(),
        source: EventSource::Evm,
        chain_id: 46,
        block_number: 123,
        block_hash: None,
        block_timestamp: 456,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 2,
        log_index: 3,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    }
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

#[cfg(feature = "legacy-query-compat")]
async fn seed_ormpipe_compatibility_rows(pool: &PgPool) {
    sqlx::query(
        r#"INSERT INTO ormp_message_accepted (
            id, block_number, transaction_hash, block_timestamp, chain_id, log_index,
            msg_hash, channel, "index", from_chain_id, "from", to_chain_id, "to",
            gas_limit, encoded, oracle, oracle_assigned, oracle_assigned_fee,
            relayer, relayer_assigned, relayer_assigned_fee
        ) VALUES
            ('accepted-8', 123, '0xtx8', 456, 1, 3, '0xaccepted-8', '0xchannel', 8, 1, '0xfrom', 46, '0xto', 500000, '0xencoded', '0xoracle', true, 11, '0xrelayer', true, 22),
            ('accepted-9', 124, '0xtx9', 457, 1, 4, '0xaccepted-9', '0xchannel', 9, 1, '0xfrom', 46, '0xto', 500000, '0xencoded', '0xoracle', true, 11, '0xrelayer', true, 22),
            ('accepted-10', 125, '0xtx10', 458, 1, 5, '0xaccepted-10', '0xchannel', 10, 1, '0xfrom', 46, '0xto', 500000, '0xencoded', NULL, false, NULL, NULL, false, NULL)"#,
    )
    .execute(pool)
    .await
    .expect("insert ormpipe accepted rows");

    sqlx::query(
        r#"INSERT INTO ormp_hash_imported (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            src_chain_id, target_chain_id, oracle, channel, msg_index, hash
        ) VALUES
            ('imported-8', 123, '0xtx8', 456, 46, 1, 46, '0xoracle', '0xchannel', 8, '0xaccepted-8'),
            ('imported-9', 124, '0xtx9', 457, 46, 1, 46, '0xoracle', '0xchannel', 9, '0xaccepted-9')"#,
    )
    .execute(pool)
    .await
    .expect("insert ormpipe imported rows");

    sqlx::query(
        r#"INSERT INTO signature_pub_signature_submittion (
            id, block_number, transaction_hash, block_timestamp, chain_id,
            channel, signer, msg_index, signature, data
        ) VALUES
            ('signature-a', 124, '0xtx-sign-a', 459, 1, '0xchannel', '0xsigner-a', 9, '0xsiga', '0xdata'),
            ('signature-b', 126, '0xtx-sign-b', 460, 1, '0xchannel', '0xsigner-b', 9, '0xsigb', '0xdata'),
            ('signature-c', 125, '0xtx-sign-c', 461, 1, '0xchannel', '0xsigner-c', 9, '0xsigc', '0xdata')"#,
    )
    .execute(pool)
    .await
    .expect("insert ormpipe signature rows");
}

fn test_database_url() -> Option<String> {
    std::env::var("ORMPINDEXER_TEST_DATABASE_URL").ok()
}
