use ormpindexer::{
    datalens::DatalensLog,
    decoder::decode_tron_event,
    planner::TRON_CHAIN_ID,
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

    let raw_payload = DatalensLog {
        event_name: Some("MessageSent".to_owned()),
        non_indexed_fields: Some(serde_json::json!("0x1234")),
        ..tron_log()
    };
    assert!(
        decode_tron_event(&raw_payload)
            .expect_err("raw payload should fail")
            .to_string()
            .contains("Tron event payload must be an object")
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
