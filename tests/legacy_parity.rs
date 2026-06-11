use ethabi::{
    Token, encode,
    ethereum_types::{H160, U256},
};
use sqlx::{PgPool, Row};

use ormpindexer::{
    database::{EventWriter, PostgresEventWriter, apply_migrations},
    datalens::DatalensLog,
    decoder::{decode_evm_log, decode_tron_event},
    planner::{
        MSGPORT_ADDRESS, MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_ADDRESS,
        ORMP_HASH_IMPORTED_TOPIC, ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC,
        ORMP_MESSAGE_DISPATCHED_TOPIC, SIGNATURE_PUB_ADDRESS,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC, TRON_CHAIN_ID,
    },
    schema::{
        AssignmentConfig, LegacyOrmPEvent, MsgportMessageRecvRow, MsgportMessageSentRow,
        OrmpHashImportedRow, OrmpMessageAcceptedRow, OrmpMessageAssignedRow,
        OrmpMessageDispatchedRow, SignaturePubSignatureSubmittionRow, apply_assignment_to_accepted,
    },
};

#[test]
fn test_evm_legacy_rows_match_subsquid_compatibility_expectations() {
    let decoded = evm_fixture_logs()
        .iter()
        .map(|log| decode_evm_log(log).expect("decode EVM parity fixture"))
        .collect::<Vec<_>>();

    assert_eq!(compatibility_rows(&decoded), evm_expected_rows());
}

#[test]
fn test_tron_structured_json_rows_match_subsquid_compatibility_expectations() {
    let decoded = tron_fixture_logs()
        .iter()
        .map(|log| decode_tron_event(log).expect("decode Tron parity fixture"))
        .collect::<Vec<_>>();

    assert_eq!(compatibility_rows(&decoded), tron_expected_rows());
}

#[tokio::test]
async fn test_postgres_writer_rows_match_subsquid_compatibility_expectations() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping parity DB test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };

    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let events = evm_fixture_logs()
        .iter()
        .map(|log| decode_evm_log(log).expect("decode EVM parity fixture"))
        .chain(
            tron_fixture_logs()
                .iter()
                .map(|log| decode_tron_event(log).expect("decode Tron parity fixture")),
        )
        .collect::<Vec<_>>();
    let expected = legacy_expected_rows();

    let writer = PostgresEventWriter::new(pool.clone());
    let written = writer
        .write_events(&events)
        .await
        .expect("write parity fixture events");

    assert_eq!(written, events.len());
    assert_eq!(fetch_compatibility_rows(&pool).await, expected);
}

fn compatibility_rows(events: &[LegacyOrmPEvent]) -> Vec<CompatibilityRow> {
    let assignment_config = AssignmentConfig::legacy_defaults();
    let mut rows = events
        .iter()
        .cloned()
        .map(|event| match event {
            LegacyOrmPEvent::HashImported { .. } => {
                CompatibilityRow::OrmpHashImported(OrmpHashImportedRow::from_event(event))
            }
            LegacyOrmPEvent::MessageAccepted { .. } => {
                CompatibilityRow::OrmpMessageAccepted(OrmpMessageAcceptedRow::from_event(event))
            }
            LegacyOrmPEvent::MessageAssigned { .. } => {
                CompatibilityRow::OrmpMessageAssigned(OrmpMessageAssignedRow::from_event(event))
            }
            LegacyOrmPEvent::MessageDispatched { .. } => {
                CompatibilityRow::OrmpMessageDispatched(OrmpMessageDispatchedRow::from_event(event))
            }
            LegacyOrmPEvent::MsgportMessageRecv { .. } => {
                CompatibilityRow::MsgportMessageRecv(MsgportMessageRecvRow::from_event(event))
            }
            LegacyOrmPEvent::MsgportMessageSent { .. } => {
                CompatibilityRow::MsgportMessageSent(MsgportMessageSentRow::from_event(event))
            }
            LegacyOrmPEvent::SignatureSubmittion { .. } => CompatibilityRow::SignatureSubmittion(
                SignaturePubSignatureSubmittionRow::from_event(event),
            ),
        })
        .collect::<Vec<_>>();

    let assigned_rows = rows
        .iter()
        .filter_map(|row| match row {
            CompatibilityRow::OrmpMessageAssigned(assigned) => Some(assigned.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for row in &mut rows {
        let CompatibilityRow::OrmpMessageAccepted(accepted) = row else {
            continue;
        };
        for assigned in &assigned_rows {
            apply_assignment_to_accepted(accepted, assigned, &assignment_config);
        }
    }

    sort_rows(&mut rows);
    rows
}

fn evm_expected_rows() -> Vec<CompatibilityRow> {
    let msg_hash = bytes_hex(0x11);
    let hash = bytes_hex(0x22);
    let msg_id = bytes_hex(0x33);
    let dispatch_hash = bytes_hex(0x44);

    let mut rows = vec![
        CompatibilityRow::OrmpHashImported(OrmpHashImportedRow {
            id: hash.clone(),
            block_number: 100,
            transaction_hash: bytes_hex(0xaa),
            block_timestamp: 1_700_000_000_000,
            chain_id: 1,
            src_chain_id: 46,
            target_chain_id: 1,
            oracle: address_hex(0x10),
            channel: address_hex(0x20),
            msg_index: 7,
            hash,
        }),
        CompatibilityRow::OrmpMessageAccepted(OrmpMessageAcceptedRow {
            id: msg_hash.clone(),
            block_number: 101,
            transaction_hash: bytes_hex(0xbb),
            block_timestamp: 1_700_000_000_001,
            chain_id: 1,
            log_index: 4,
            msg_hash: msg_hash.clone(),
            channel: address_hex(0x21),
            index: 8,
            from_chain_id: 46,
            from: address_hex(0x22),
            to_chain_id: 137,
            to: address_hex(0x23),
            gas_limit: 500_000,
            encoded: "0xabcd".to_owned(),
            oracle: Some(ormpindexer::schema::ADDRESS_ORACLE[0].to_owned()),
            oracle_assigned: Some(true),
            oracle_assigned_fee: Some(9),
            relayer: Some(ormpindexer::schema::ADDRESS_RELAYER[0].to_owned()),
            relayer_assigned: Some(true),
            relayer_assigned_fee: Some(10),
        }),
        CompatibilityRow::OrmpMessageAssigned(OrmpMessageAssignedRow {
            id: "1-102-5".to_owned(),
            block_number: 102,
            transaction_hash: bytes_hex(0xcc),
            block_timestamp: 1_700_000_000_002,
            chain_id: 1,
            msg_hash,
            oracle: ormpindexer::schema::ADDRESS_ORACLE[0].to_owned(),
            relayer: ormpindexer::schema::ADDRESS_RELAYER[0].to_owned(),
            oracle_fee: 9,
            relayer_fee: 10,
            params: "0x0102".to_owned(),
        }),
        CompatibilityRow::OrmpMessageDispatched(OrmpMessageDispatchedRow {
            id: dispatch_hash.clone(),
            block_number: 103,
            transaction_hash: bytes_hex(0xdd),
            block_timestamp: 1_700_000_000_003,
            chain_id: 1,
            target_chain_id: 1,
            msg_hash: dispatch_hash,
            dispatch_result: true,
        }),
        CompatibilityRow::MsgportMessageRecv(MsgportMessageRecvRow {
            id: "1-104-7".to_owned(),
            block_number: 104,
            transaction_hash: bytes_hex(0xee),
            block_timestamp: 1_700_000_000_004,
            transaction_index: 6,
            log_index: 7,
            chain_id: 1,
            port_address: MSGPORT_ADDRESS.to_ascii_lowercase(),
            msg_id: msg_id.clone(),
            result: false,
            return_data: "0xff".to_owned(),
        }),
        CompatibilityRow::MsgportMessageSent(MsgportMessageSentRow {
            id: "1-105-8".to_owned(),
            block_number: 105,
            transaction_hash: bytes_hex(0xef),
            block_timestamp: 1_700_000_000_005,
            transaction_index: 7,
            log_index: 8,
            chain_id: 1,
            port_address: MSGPORT_ADDRESS.to_ascii_lowercase(),
            transaction_from: Some(address_hex(0x50)),
            from_chain_id: 1,
            msg_id,
            from_dapp: address_hex(0x30),
            to_chain_id: 42161,
            to_dapp: address_hex(0x31),
            message: "0xaa".to_owned(),
            params: "0xbbcc".to_owned(),
        }),
        CompatibilityRow::SignatureSubmittion(SignaturePubSignatureSubmittionRow {
            id: "1-106-9".to_owned(),
            block_number: 106,
            transaction_hash: bytes_hex(0xf0),
            block_timestamp: 1_700_000_000_006,
            chain_id: 46,
            channel: address_hex(0x40),
            signer: address_hex(0x41),
            msg_index: 12,
            signature: "0xdead".to_owned(),
            data: "0xbeef".to_owned(),
        }),
    ];
    sort_rows(&mut rows);
    rows
}

fn tron_expected_rows() -> Vec<CompatibilityRow> {
    let msg_hash = bytes_hex(0x11);
    let hash = bytes_hex(0x22);
    let msg_id = bytes_hex(0x33);
    let dispatch_hash = bytes_hex(0x44);

    let mut rows = vec![
        CompatibilityRow::OrmpHashImported(OrmpHashImportedRow {
            id: hash.clone(),
            block_number: 120,
            transaction_hash: "tron-hash-imported".to_owned(),
            block_timestamp: 456_000,
            chain_id: TRON_CHAIN_ID.into(),
            src_chain_id: 46,
            target_chain_id: TRON_CHAIN_ID.into(),
            oracle: "41aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            channel: "41bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            msg_index: 7,
            hash,
        }),
        CompatibilityRow::OrmpMessageAccepted(OrmpMessageAcceptedRow {
            id: msg_hash.clone(),
            block_number: 121,
            transaction_hash: "tron-message-accepted".to_owned(),
            block_timestamp: 456_001,
            chain_id: TRON_CHAIN_ID.into(),
            log_index: 4,
            msg_hash: msg_hash.clone(),
            channel: "41cccccccccccccccccccccccccccccccccccccccc".to_owned(),
            index: 8,
            from_chain_id: 46,
            from: "41dddddddddddddddddddddddddddddddddddddddd".to_owned(),
            to_chain_id: 137,
            to: "41eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            gas_limit: 500_000,
            encoded: "0xabcd".to_owned(),
            oracle: Some(ormpindexer::schema::ADDRESS_ORACLE[0].to_owned()),
            oracle_assigned: Some(true),
            oracle_assigned_fee: Some(9),
            relayer: Some(ormpindexer::schema::ADDRESS_RELAYER[0].to_owned()),
            relayer_assigned: Some(true),
            relayer_assigned_fee: Some(10),
        }),
        CompatibilityRow::OrmpMessageAssigned(OrmpMessageAssignedRow {
            id: "728126428-122-tron-message-assigned-5".to_owned(),
            block_number: 122,
            transaction_hash: "tron-message-assigned".to_owned(),
            block_timestamp: 456_002,
            chain_id: TRON_CHAIN_ID.into(),
            msg_hash,
            oracle: ormpindexer::schema::ADDRESS_ORACLE[0].to_owned(),
            relayer: ormpindexer::schema::ADDRESS_RELAYER[0].to_owned(),
            oracle_fee: 9,
            relayer_fee: 10,
            params: "0x0102".to_owned(),
        }),
        CompatibilityRow::OrmpMessageDispatched(OrmpMessageDispatchedRow {
            id: dispatch_hash.clone(),
            block_number: 123,
            transaction_hash: "tron-message-dispatched".to_owned(),
            block_timestamp: 456_003,
            chain_id: TRON_CHAIN_ID.into(),
            target_chain_id: TRON_CHAIN_ID.into(),
            msg_hash: dispatch_hash,
            dispatch_result: true,
        }),
        CompatibilityRow::MsgportMessageRecv(MsgportMessageRecvRow {
            id: "728126428-124-tron-message-recv-7".to_owned(),
            block_number: 124,
            transaction_hash: "tron-message-recv".to_owned(),
            block_timestamp: 456_004,
            transaction_index: 6,
            log_index: 7,
            chain_id: TRON_CHAIN_ID.into(),
            port_address: "41abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
            msg_id: msg_id.clone(),
            result: false,
            return_data: "0xff".to_owned(),
        }),
        CompatibilityRow::MsgportMessageSent(MsgportMessageSentRow {
            id: "728126428-123-trontx-3".to_owned(),
            block_number: 123,
            transaction_hash: "trontx".to_owned(),
            block_timestamp: 456,
            transaction_index: 2,
            log_index: 3,
            chain_id: TRON_CHAIN_ID.into(),
            port_address: "41abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
            transaction_from: None,
            from_chain_id: TRON_CHAIN_ID.into(),
            msg_id: bytes_hex(0x33),
            from_dapp: "0x0000000000000000000000000000000000000030".to_owned(),
            to_chain_id: 42161,
            to_dapp: "0x0000000000000000000000000000000000000031".to_owned(),
            message: "0xaa".to_owned(),
            params: "0xbbcc".to_owned(),
        }),
        CompatibilityRow::SignatureSubmittion(SignaturePubSignatureSubmittionRow {
            id: "728126428-126-tron-signature-submittion-9".to_owned(),
            block_number: 126,
            transaction_hash: "tron-signature-submittion".to_owned(),
            block_timestamp: 456_006,
            chain_id: 46,
            channel: "41ffffffffffffffffffffffffffffffffffffffff".to_owned(),
            signer: "411111111111111111111111111111111111111111".to_owned(),
            msg_index: 12,
            signature: "0xdead".to_owned(),
            data: "0xbeef".to_owned(),
        }),
    ];
    sort_rows(&mut rows);
    rows
}

fn legacy_expected_rows() -> Vec<CompatibilityRow> {
    let mut rows = evm_expected_rows();
    rows.extend(tron_expected_rows());
    sort_rows(&mut rows);
    rows
}

fn evm_fixture_logs() -> Vec<DatalensLog> {
    let msg_hash = bytes32(0x11);
    let hash = bytes32(0x22);
    let msg_id = bytes32(0x33);
    let dispatch_hash = bytes32(0x44);

    vec![
        evm_log(
            "1-100-3",
            100,
            3,
            2,
            bytes_hex(0xaa).to_ascii_uppercase(),
            ORMP_HASH_IMPORTED_TOPIC,
            ORMP_ADDRESS.to_ascii_uppercase(),
            encode(&[
                Token::Address(address(0x10)),
                Token::Uint(U256::from(46)),
                Token::Address(address(0x20)),
                Token::Uint(U256::from(7)),
                Token::FixedBytes(hash),
            ]),
        ),
        evm_log(
            "1-101-4",
            101,
            4,
            3,
            bytes_hex(0xbb).to_ascii_uppercase(),
            ORMP_MESSAGE_ACCEPTED_TOPIC,
            ORMP_ADDRESS.to_ascii_uppercase(),
            encode(&[
                Token::FixedBytes(msg_hash.clone()),
                Token::Tuple(vec![
                    Token::Address(address(0x21)),
                    Token::Uint(U256::from(8)),
                    Token::Uint(U256::from(46)),
                    Token::Address(address(0x22)),
                    Token::Uint(U256::from(137)),
                    Token::Address(address(0x23)),
                    Token::Uint(U256::from(500_000)),
                    Token::Bytes(vec![0xab, 0xcd]),
                ]),
            ]),
        ),
        evm_log(
            "1-102-5",
            102,
            5,
            4,
            bytes_hex(0xcc).to_ascii_uppercase(),
            ORMP_MESSAGE_ASSIGNED_TOPIC,
            ORMP_ADDRESS.to_ascii_uppercase(),
            encode(&[
                Token::FixedBytes(msg_hash),
                Token::Address(
                    ormpindexer::schema::ADDRESS_ORACLE[0]
                        .parse()
                        .expect("oracle address"),
                ),
                Token::Address(
                    ormpindexer::schema::ADDRESS_RELAYER[0]
                        .parse()
                        .expect("relayer address"),
                ),
                Token::Uint(U256::from(9)),
                Token::Uint(U256::from(10)),
                Token::Bytes(vec![0x01, 0x02]),
            ]),
        ),
        evm_log(
            "1-103-6",
            103,
            6,
            5,
            bytes_hex(0xdd).to_ascii_uppercase(),
            ORMP_MESSAGE_DISPATCHED_TOPIC,
            ORMP_ADDRESS.to_ascii_uppercase(),
            encode(&[Token::FixedBytes(dispatch_hash), Token::Bool(true)]),
        ),
        evm_log(
            "1-104-7",
            104,
            7,
            6,
            bytes_hex(0xee).to_ascii_uppercase(),
            MSGPORT_MESSAGE_RECV_TOPIC,
            MSGPORT_ADDRESS.to_ascii_uppercase(),
            encode(&[
                Token::FixedBytes(msg_id.clone()),
                Token::Bool(false),
                Token::Bytes(vec![0xff]),
            ]),
        ),
        evm_log(
            "1-105-8",
            105,
            8,
            7,
            bytes_hex(0xef).to_ascii_uppercase(),
            MSGPORT_MESSAGE_SENT_TOPIC,
            MSGPORT_ADDRESS.to_ascii_uppercase(),
            encode(&[
                Token::FixedBytes(msg_id),
                Token::Address(address(0x30)),
                Token::Uint(U256::from(42161)),
                Token::Address(address(0x31)),
                Token::Bytes(vec![0xaa]),
                Token::Bytes(vec![0xbb, 0xcc]),
            ]),
        ),
        evm_log(
            "1-106-9",
            106,
            9,
            8,
            bytes_hex(0xf0).to_ascii_uppercase(),
            SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
            SIGNATURE_PUB_ADDRESS.to_ascii_uppercase(),
            encode(&[
                Token::Uint(U256::from(46)),
                Token::Address(address(0x40)),
                Token::Address(address(0x41)),
                Token::Uint(U256::from(12)),
                Token::Bytes(vec![0xde, 0xad]),
                Token::Bytes(vec![0xbe, 0xef]),
            ]),
        ),
    ]
}

fn tron_fixture_logs() -> Vec<DatalensLog> {
    vec![
        tron_log(
            "728126428-120-tron-hash-imported-3",
            120,
            3,
            2,
            "tron-hash-imported",
            "HashImported",
            serde_json::json!({
                "oracle": "41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "chainId": "46",
                "channel": "41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                "msgIndex": "7",
                "hash": bytes_hex(0x22)
            }),
        ),
        tron_log(
            "728126428-121-tron-message-accepted-4",
            121,
            4,
            3,
            "tron-message-accepted",
            "MessageAccepted",
            serde_json::json!({
                "msgHash": bytes_hex(0x11),
                "message": {
                    "channel": "41CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                    "index": "8",
                    "fromChainId": "46",
                    "from": "41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
                    "toChainId": "137",
                    "to": "41EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                    "gasLimit": "500000",
                    "encoded": "0xabcd"
                }
            }),
        ),
        tron_log(
            "728126428-122-tron-message-assigned-5",
            122,
            5,
            4,
            "tron-message-assigned",
            "MessageAssigned",
            serde_json::json!({
                "msgHash": bytes_hex(0x11),
                "oracle": ormpindexer::schema::ADDRESS_ORACLE[0],
                "relayer": ormpindexer::schema::ADDRESS_RELAYER[0],
                "oracleFee": "9",
                "relayerFee": "10",
                "params": "0x0102"
            }),
        ),
        tron_log(
            "728126428-123-tron-message-dispatched-6",
            123,
            6,
            5,
            "tron-message-dispatched",
            "MessageDispatched",
            serde_json::json!({
                "msgHash": bytes_hex(0x44),
                "dispatchResult": true
            }),
        ),
        tron_log(
            "728126428-124-tron-message-recv-7",
            124,
            7,
            6,
            "tron-message-recv",
            "MessageRecv",
            serde_json::json!({
                "msgId": bytes_hex(0x33),
                "result": false,
                "returnData": "0xff"
            }),
        ),
        serde_json::from_value(serde_json::json!({
            "id": "728126428-123-trontx-3",
            "chainId": TRON_CHAIN_ID,
            "blockNumber": 123,
            "blockTimestamp": 456,
            "transactionHash": "trontx",
            "transactionIndex": 2,
            "logIndex": 3,
            "address": "41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD",
            "eventName": "MessageSent",
            "eventSignature": "MessageSent(bytes32,address,uint256,address,bytes,bytes)",
            "indexedFields": [],
            "nonIndexedFields": {
                "msgId": bytes_hex(0x33),
                "fromDapp": "0x0000000000000000000000000000000000000030",
                "toChainId": "42161",
                "toDapp": "0x0000000000000000000000000000000000000031",
                "message": "0xaa",
                "params": "0xbbcc"
            },
            "topics": [],
            "data": "0x"
        }))
        .expect("decode Tron Datalens log"),
        tron_log(
            "728126428-126-tron-signature-submittion-9",
            126,
            9,
            8,
            "tron-signature-submittion",
            "SignatureSubmittion",
            serde_json::json!({
                "chainId": "46",
                "channel": "41FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
                "signer": "411111111111111111111111111111111111111111",
                "msgIndex": "12",
                "signature": "0xdead",
                "data": "0xbeef"
            }),
        ),
    ]
}

fn tron_log(
    id: &str,
    block_number: u64,
    log_index: u64,
    transaction_index: i32,
    transaction_hash: &str,
    event_name: &str,
    non_indexed_fields: serde_json::Value,
) -> DatalensLog {
    serde_json::from_value(serde_json::json!({
        "id": id,
        "chainId": TRON_CHAIN_ID,
        "blockNumber": block_number,
        "blockTimestamp": 456_000 + block_number - 120,
        "transactionHash": transaction_hash,
        "transactionIndex": transaction_index,
        "logIndex": log_index,
        "address": "41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD",
        "eventName": event_name,
        "eventSignature": event_name,
        "indexedFields": [],
        "nonIndexedFields": non_indexed_fields,
        "topics": [],
        "data": "0x"
    }))
    .expect("decode Tron Datalens log")
}

fn evm_log(
    id: &str,
    block_number: u64,
    log_index: u64,
    transaction_index: i32,
    transaction_hash: String,
    topic: &str,
    address: String,
    data: Vec<u8>,
) -> DatalensLog {
    DatalensLog {
        id: Some(id.to_owned()),
        chain_id: 1,
        block_number,
        block_timestamp: Some(1_700_000_000_000 + block_number - 100),
        transaction_hash,
        transaction_index: Some(transaction_index),
        log_index,
        address,
        transaction_from: Some(address_hex(0x50).to_ascii_uppercase()),
        topics: vec![topic.to_ascii_uppercase()],
        data: format!("0x{}", hex::encode(data)).to_ascii_uppercase(),
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompatibilityRow {
    OrmpHashImported(OrmpHashImportedRow),
    OrmpMessageAccepted(OrmpMessageAcceptedRow),
    OrmpMessageAssigned(OrmpMessageAssignedRow),
    OrmpMessageDispatched(OrmpMessageDispatchedRow),
    MsgportMessageRecv(MsgportMessageRecvRow),
    MsgportMessageSent(MsgportMessageSentRow),
    SignatureSubmittion(SignaturePubSignatureSubmittionRow),
}

impl CompatibilityRow {
    fn sort_key(&self) -> (&'static str, &str) {
        match self {
            Self::OrmpHashImported(row) => ("ormp_hash_imported", row.id.as_str()),
            Self::OrmpMessageAccepted(row) => ("ormp_message_accepted", row.id.as_str()),
            Self::OrmpMessageAssigned(row) => ("ormp_message_assigned", row.id.as_str()),
            Self::OrmpMessageDispatched(row) => ("ormp_message_dispatched", row.id.as_str()),
            Self::MsgportMessageRecv(row) => ("msgport_message_recv", row.id.as_str()),
            Self::MsgportMessageSent(row) => ("msgport_message_sent", row.id.as_str()),
            Self::SignatureSubmittion(row) => {
                ("signature_pub_signature_submittion", row.id.as_str())
            }
        }
    }
}

fn sort_rows(rows: &mut [CompatibilityRow]) {
    rows.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
}

async fn fetch_compatibility_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    let mut rows = Vec::new();
    rows.extend(fetch_hash_imported_rows(pool).await);
    rows.extend(fetch_message_accepted_rows(pool).await);
    rows.extend(fetch_message_assigned_rows(pool).await);
    rows.extend(fetch_message_dispatched_rows(pool).await);
    rows.extend(fetch_msgport_recv_rows(pool).await);
    rows.extend(fetch_msgport_sent_rows(pool).await);
    rows.extend(fetch_signature_submittion_rows(pool).await);
    sort_rows(&mut rows);
    rows
}

async fn fetch_hash_imported_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
    >(
        "SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, chain_id::TEXT,
                src_chain_id::TEXT, target_chain_id::TEXT, oracle, channel, msg_index::TEXT, hash
         FROM ormp_hash_imported",
    )
    .fetch_all(pool)
    .await
    .expect("fetch ormp_hash_imported")
    .into_iter()
    .map(|row| {
        CompatibilityRow::OrmpHashImported(OrmpHashImportedRow {
            id: row.0,
            block_number: parse_u128(&row.1),
            transaction_hash: row.2,
            block_timestamp: parse_u128(&row.3),
            chain_id: parse_u128(&row.4),
            src_chain_id: parse_u128(&row.5),
            target_chain_id: parse_u128(&row.6),
            oracle: row.7,
            channel: row.8,
            msg_index: parse_u128(&row.9),
            hash: row.10,
        })
    })
    .collect()
}

async fn fetch_message_accepted_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query(
        r#"SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, chain_id::TEXT,
                  log_index, msg_hash, channel, "index"::TEXT AS index_value,
                  from_chain_id::TEXT, "from" AS from_address, to_chain_id::TEXT,
                  "to" AS to_address, gas_limit::TEXT, encoded, oracle, oracle_assigned,
                  oracle_assigned_fee::TEXT, relayer, relayer_assigned, relayer_assigned_fee::TEXT
           FROM ormp_message_accepted"#,
    )
    .fetch_all(pool)
    .await
    .expect("fetch ormp_message_accepted")
    .into_iter()
    .map(|row| {
        CompatibilityRow::OrmpMessageAccepted(OrmpMessageAcceptedRow {
            id: row.get("id"),
            block_number: parse_u128(row.get("block_number")),
            transaction_hash: row.get("transaction_hash"),
            block_timestamp: parse_u128(row.get("block_timestamp")),
            chain_id: parse_u128(row.get("chain_id")),
            log_index: row.get("log_index"),
            msg_hash: row.get("msg_hash"),
            channel: row.get("channel"),
            index: parse_u128(row.get("index_value")),
            from_chain_id: parse_u128(row.get("from_chain_id")),
            from: row.get("from_address"),
            to_chain_id: parse_u128(row.get("to_chain_id")),
            to: row.get("to_address"),
            gas_limit: parse_u128(row.get("gas_limit")),
            encoded: row.get("encoded"),
            oracle: row.get("oracle"),
            oracle_assigned: row.get("oracle_assigned"),
            oracle_assigned_fee: row
                .get::<Option<String>, _>("oracle_assigned_fee")
                .as_deref()
                .map(parse_u128),
            relayer: row.get("relayer"),
            relayer_assigned: row.get("relayer_assigned"),
            relayer_assigned_fee: row
                .get::<Option<String>, _>("relayer_assigned_fee")
                .as_deref()
                .map(parse_u128),
        })
    })
    .collect()
}

async fn fetch_message_assigned_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
    >(
        "SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, chain_id::TEXT,
                msg_hash, oracle, relayer, oracle_fee::TEXT, relayer_fee::TEXT, params
         FROM ormp_message_assigned",
    )
    .fetch_all(pool)
    .await
    .expect("fetch ormp_message_assigned")
    .into_iter()
    .map(|row| {
        CompatibilityRow::OrmpMessageAssigned(OrmpMessageAssignedRow {
            id: row.0,
            block_number: parse_u128(&row.1),
            transaction_hash: row.2,
            block_timestamp: parse_u128(&row.3),
            chain_id: parse_u128(&row.4),
            msg_hash: row.5,
            oracle: row.6,
            relayer: row.7,
            oracle_fee: parse_u128(&row.8),
            relayer_fee: parse_u128(&row.9),
            params: row.10,
        })
    })
    .collect()
}

async fn fetch_message_dispatched_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query_as::<_, (String, String, String, String, String, String, String, bool)>(
        "SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, chain_id::TEXT,
                target_chain_id::TEXT, msg_hash, dispatch_result
         FROM ormp_message_dispatched",
    )
    .fetch_all(pool)
    .await
    .expect("fetch ormp_message_dispatched")
    .into_iter()
    .map(|row| {
        CompatibilityRow::OrmpMessageDispatched(OrmpMessageDispatchedRow {
            id: row.0,
            block_number: parse_u128(&row.1),
            transaction_hash: row.2,
            block_timestamp: parse_u128(&row.3),
            chain_id: parse_u128(&row.4),
            target_chain_id: parse_u128(&row.5),
            msg_hash: row.6,
            dispatch_result: row.7,
        })
    })
    .collect()
}

async fn fetch_msgport_recv_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            i32,
            i32,
            String,
            String,
            String,
            bool,
            String,
        ),
    >(
        "SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, transaction_index,
                log_index, chain_id::TEXT, port_address, msg_id, result, return_data
         FROM msgport_message_recv",
    )
    .fetch_all(pool)
    .await
    .expect("fetch msgport_message_recv")
    .into_iter()
    .map(|row| {
        CompatibilityRow::MsgportMessageRecv(MsgportMessageRecvRow {
            id: row.0,
            block_number: parse_u128(&row.1),
            transaction_hash: row.2,
            block_timestamp: parse_u128(&row.3),
            transaction_index: row.4,
            log_index: row.5,
            chain_id: parse_u128(&row.6),
            port_address: row.7,
            msg_id: row.8,
            result: row.9,
            return_data: row.10,
        })
    })
    .collect()
}

async fn fetch_msgport_sent_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            i32,
            i32,
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
    >(
        "SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, transaction_index,
                log_index, chain_id::TEXT, port_address, transaction_from, from_chain_id::TEXT,
                msg_id, from_dapp, to_chain_id::TEXT, to_dapp, message, params
         FROM msgport_message_sent",
    )
    .fetch_all(pool)
    .await
    .expect("fetch msgport_message_sent")
    .into_iter()
    .map(|row| {
        CompatibilityRow::MsgportMessageSent(MsgportMessageSentRow {
            id: row.0,
            block_number: parse_u128(&row.1),
            transaction_hash: row.2,
            block_timestamp: parse_u128(&row.3),
            transaction_index: row.4,
            log_index: row.5,
            chain_id: parse_u128(&row.6),
            port_address: row.7,
            transaction_from: row.8,
            from_chain_id: parse_u128(&row.9),
            msg_id: row.10,
            from_dapp: row.11,
            to_chain_id: parse_u128(&row.12),
            to_dapp: row.13,
            message: row.14,
            params: row.15,
        })
    })
    .collect()
}

async fn fetch_signature_submittion_rows(pool: &PgPool) -> Vec<CompatibilityRow> {
    sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
    >(
        "SELECT id, block_number::TEXT, transaction_hash, block_timestamp::TEXT, chain_id::TEXT,
                channel, signer, msg_index::TEXT, signature, data
         FROM signature_pub_signature_submittion",
    )
    .fetch_all(pool)
    .await
    .expect("fetch signature_pub_signature_submittion")
    .into_iter()
    .map(|row| {
        CompatibilityRow::SignatureSubmittion(SignaturePubSignatureSubmittionRow {
            id: row.0,
            block_number: parse_u128(&row.1),
            transaction_hash: row.2,
            block_timestamp: parse_u128(&row.3),
            chain_id: parse_u128(&row.4),
            channel: row.5,
            signer: row.6,
            msg_index: parse_u128(&row.7),
            signature: row.8,
            data: row.9,
        })
    })
    .collect()
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

fn parse_u128(value: &str) -> u128 {
    value.parse().expect("numeric database value")
}

fn address(value: u64) -> H160 {
    H160::from_low_u64_be(value)
}

fn address_hex(value: u64) -> String {
    format!("0x{}", hex::encode(address(value).as_bytes()))
}

fn bytes32(value: u8) -> Vec<u8> {
    vec![value; 32]
}

fn bytes_hex(value: u8) -> String {
    format!("0x{}", hex::encode(bytes32(value)))
}

fn test_database_url() -> Option<String> {
    std::env::var("ORMPINDEXER_TEST_DATABASE_URL").ok()
}
