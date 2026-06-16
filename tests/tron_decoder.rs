use ethabi::{
    Token, encode,
    ethereum_types::{H160, U256},
};
use ormpindexer::{
    datalens::DatalensLog,
    decoder::{EventDecoder, EvmEventDecoder, decode_tron_event},
    planner::{ORMP_MESSAGE_ACCEPTED_TOPIC, TRON_CHAIN_ID},
    schema::{EventSource, LegacyOrmPEvent, MsgportMessageSentRow},
};

#[test]
fn test_decode_tron_message_sent_preserves_legacy_fields() {
    let log: DatalensLog = serde_json::from_value(serde_json::json!({
        "id": "728126428-123-trontx-3",
        "chainId": TRON_CHAIN_ID,
        "blockNumber": 123,
        "blockHash": "0x9c64f37100000000000000000000000000000000000000000000000000000000",
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
    .expect("decode Tron Datalens log");

    let event = decode_tron_event(&log).expect("decode Tron event");

    assert_eq!(
        event,
        LegacyOrmPEvent::MsgportMessageSent {
            metadata: ormpindexer::schema::ChainLogMetadata {
                id: "728126428-123-trontx-3".to_owned(),
                source: EventSource::Tron,
                chain_id: TRON_CHAIN_ID.into(),
                block_number: 123,
                block_hash: Some(
                    "0x9c64f37100000000000000000000000000000000000000000000000000000000".to_owned(),
                ),
                block_timestamp: 456,
                transaction_hash: "trontx".to_owned(),
                transaction_index: 2,
                log_index: 3,
                contract_address: "41abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
                transaction_from: None,
            },
            msg_id: bytes_hex(0x33),
            from_dapp: "0x0000000000000000000000000000000000000030".to_owned(),
            to_chain_id: 42161,
            to_dapp: "0x0000000000000000000000000000000000000031".to_owned(),
            message: "0xaa".to_owned(),
            params: "0xbbcc".to_owned(),
        }
    );

    let row = MsgportMessageSentRow::from_event(event);
    assert_eq!(row.id, "0000000123-9c64f-000003");
    assert_eq!(row.chain_id, TRON_CHAIN_ID.into());
    assert_eq!(row.from_chain_id, TRON_CHAIN_ID.into());
    assert_eq!(row.transaction_from, None);
    assert_eq!(
        row.port_address,
        "41abcdefabcdefabcdefabcdefabcdefabcdefabcd"
    );
}

#[test]
fn test_decode_tron_raw_message_accepted_from_block_scan_fields() {
    let log = DatalensLog {
        id: Some("62254349-0xtrontx-7".to_owned()),
        chain_id: TRON_CHAIN_ID,
        block_number: 62_254_349,
        block_hash: Some(
            "0x9c64f37100000000000000000000000000000000000000000000000000000000".to_owned(),
        ),
        parent_hash: None,
        block_timestamp: Some(1_800_000_000_000),
        transaction_hash: "trontx".to_owned(),
        transaction_index: Some(4),
        log_index: 7,
        address: "415c5c383febe62f377f8c0ea1de97f2a2ba102e98".to_owned(),
        transaction_from: Some("41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD".to_owned()),
        topics: Vec::new(),
        data: "0x".to_owned(),
        event_name: Some("MessageAccepted".to_owned()),
        event_signature: Some(
            ORMP_MESSAGE_ACCEPTED_TOPIC
                .trim_start_matches("0x")
                .to_owned(),
        ),
        indexed_fields: vec![
            serde_json::json!(ORMP_MESSAGE_ACCEPTED_TOPIC.trim_start_matches("0x")),
            serde_json::json!(bytes_hex(0x11).trim_start_matches("0x")),
        ],
        non_indexed_fields: Some(serde_json::json!(hex::encode(encode(&[Token::Tuple(
            vec![
                Token::Address(address(0x21)),
                Token::Uint(U256::from(8)),
                Token::Uint(U256::from(46)),
                Token::Address(address(0x22)),
                Token::Uint(U256::from(137)),
                Token::Address(address(0x23)),
                Token::Uint(U256::from(500_000)),
                Token::Bytes(vec![0xab, 0xcd]),
            ]
        )])))),
    };

    let event = decode_tron_event(&log).expect("decode raw Tron block-scan event");

    assert_eq!(
        event,
        LegacyOrmPEvent::MessageAccepted {
            metadata: ormpindexer::schema::ChainLogMetadata {
                id: "62254349-0xtrontx-7".to_owned(),
                source: EventSource::Tron,
                chain_id: TRON_CHAIN_ID.into(),
                block_number: 62_254_349,
                block_hash: Some(
                    "0x9c64f37100000000000000000000000000000000000000000000000000000000".to_owned(),
                ),
                block_timestamp: 1_800_000_000_000,
                transaction_hash: "trontx".to_owned(),
                transaction_index: 4,
                log_index: 7,
                contract_address: "415c5c383febe62f377f8c0ea1de97f2a2ba102e98".to_owned(),
                transaction_from: Some("41abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()),
            },
            msg_hash: bytes_hex(0x11),
            channel: address_hex(0x21),
            index: 8,
            from_chain_id: 46,
            from: address_hex(0x22),
            to_chain_id: 137,
            to: address_hex(0x23),
            gas_limit: 500_000,
            encoded: "0xabcd".to_owned(),
        }
    );
}

#[test]
fn test_decode_tron_event_normalizes_timestamp_and_transaction_hash() {
    let log = DatalensLog {
        block_timestamp: Some(1_800_000_000),
        transaction_hash: "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD"
            .to_owned(),
        ..tron_log()
    };

    let event = decode_tron_event(&DatalensLog {
        event_name: Some("MessageAccepted".to_owned()),
        non_indexed_fields: Some(serde_json::json!({
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
        })),
        ..log
    })
    .expect("decode Tron event");

    match event {
        LegacyOrmPEvent::MessageAccepted { metadata, .. } => {
            assert_eq!(metadata.block_timestamp, 1_800_000_000_000);
            assert_eq!(
                metadata.transaction_hash,
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            );
        }
        _ => panic!("expected MessageAccepted event"),
    }
}

#[tokio::test]
async fn test_evm_event_decoder_suppresses_tron_msgport_events() {
    let events = EvmEventDecoder
        .decode(&tron_log())
        .await
        .expect("decode Tron MessageSent through production decoder");

    assert!(events.is_empty());
}

#[test]
fn test_decode_tron_errors_are_explicit() {
    let unsupported = DatalensLog {
        event_name: Some("Transfer".to_owned()),
        non_indexed_fields: Some(serde_json::json!({"value": "1"})),
        ..tron_log()
    };
    assert!(
        decode_tron_event(&unsupported)
            .expect_err("unsupported event should fail")
            .to_string()
            .contains("unsupported ORMP Tron event name Transfer")
    );

    let malformed_raw_payload = DatalensLog {
        event_name: Some("MessageSent".to_owned()),
        non_indexed_fields: Some(serde_json::json!("0x1234")),
        ..tron_log()
    };
    assert!(
        decode_tron_event(&malformed_raw_payload)
            .expect_err("malformed raw payload should fail")
            .to_string()
            .contains("decode ABI event data")
    );

    let missing_field = DatalensLog {
        event_name: Some("MessageSent".to_owned()),
        non_indexed_fields: Some(serde_json::json!({"msgId": bytes_hex(0x33)})),
        ..tron_log()
    };
    assert!(
        decode_tron_event(&missing_field)
            .expect_err("missing field should fail")
            .to_string()
            .contains("Tron event field fromDapp is missing")
    );
}

fn tron_log() -> DatalensLog {
    DatalensLog {
        id: Some("728126428-123-trontx-3".to_owned()),
        chain_id: TRON_CHAIN_ID,
        block_number: 123,
        block_hash: None,
        parent_hash: None,
        block_timestamp: Some(456),
        transaction_hash: "trontx".to_owned(),
        transaction_index: Some(2),
        log_index: 3,
        address: "41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD".to_owned(),
        transaction_from: None,
        topics: Vec::new(),
        data: "0x".to_owned(),
        event_name: Some("MessageSent".to_owned()),
        event_signature: Some(
            "MessageSent(bytes32,address,uint256,address,bytes,bytes)".to_owned(),
        ),
        indexed_fields: Vec::new(),
        non_indexed_fields: Some(serde_json::json!({
            "msgId": bytes_hex(0x33),
            "fromDapp": "0x0000000000000000000000000000000000000030",
            "toChainId": "42161",
            "toDapp": "0x0000000000000000000000000000000000000031",
            "message": "0xaa",
            "params": "0xbbcc"
        })),
    }
}

fn bytes_hex(value: u8) -> String {
    format!("0x{}", hex::encode(vec![value; 32]))
}

fn address(value: u8) -> H160 {
    H160::from_slice(&[value; 20])
}

fn address_hex(value: u8) -> String {
    format!("0x{}", hex::encode([value; 20]))
}
