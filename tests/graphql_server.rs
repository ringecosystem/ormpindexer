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

fn test_database_url() -> Option<String> {
    std::env::var("ORMPINDEXER_TEST_DATABASE_URL").ok()
}
