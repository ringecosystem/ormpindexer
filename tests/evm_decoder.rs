use ethabi::{
    Token, encode,
    ethereum_types::{H160, U256},
};
use ormpindexer::{
    datalens::DatalensLog,
    decoder::decode_evm_log,
    planner::{
        MSGPORT_ADDRESS, MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_ADDRESS,
        ORMP_HASH_IMPORTED_TOPIC, ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC,
        ORMP_MESSAGE_DISPATCHED_TOPIC, SIGNATURE_PUB_ADDRESS,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
    },
    schema::{EventSource, LegacyOrmPEvent, MsgportMessageSentRow},
};

#[test]
fn test_decode_ormp_events_preserves_legacy_fields() {
    let msg_hash = bytes32(0x11);
    let hash_imported = decode_evm_log(&log(
        ORMP_HASH_IMPORTED_TOPIC,
        ORMP_ADDRESS,
        encode(&[
            Token::Address(address(0x10)),
            Token::Uint(U256::from(46)),
            Token::Address(address(0x20)),
            Token::Uint(U256::from(7)),
            Token::FixedBytes(msg_hash.clone()),
        ]),
    ))
    .expect("HashImported decodes");
    assert_eq!(
        hash_imported,
        LegacyOrmPEvent::HashImported {
            metadata: metadata(ORMP_ADDRESS),
            src_chain_id: 46,
            target_chain_id: 1,
            oracle: address_hex(0x10),
            channel: address_hex(0x20),
            msg_index: 7,
            hash: bytes_hex(0x11),
        }
    );

    let accepted = decode_evm_log(&log(
        ORMP_MESSAGE_ACCEPTED_TOPIC,
        ORMP_ADDRESS,
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
    ))
    .expect("MessageAccepted decodes");
    assert_eq!(
        accepted,
        LegacyOrmPEvent::MessageAccepted {
            metadata: metadata(ORMP_ADDRESS),
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

    let assigned = decode_evm_log(&log(
        ORMP_MESSAGE_ASSIGNED_TOPIC,
        ORMP_ADDRESS,
        encode(&[
            Token::FixedBytes(msg_hash.clone()),
            Token::Address(address(0x24)),
            Token::Address(address(0x25)),
            Token::Uint(U256::from(9)),
            Token::Uint(U256::from(10)),
            Token::Bytes(vec![0x01, 0x02]),
        ]),
    ))
    .expect("MessageAssigned decodes");
    assert_eq!(
        assigned,
        LegacyOrmPEvent::MessageAssigned {
            metadata: metadata(ORMP_ADDRESS),
            msg_hash: bytes_hex(0x11),
            oracle: address_hex(0x24),
            relayer: address_hex(0x25),
            oracle_fee: 9,
            relayer_fee: 10,
            params: "0x0102".to_owned(),
        }
    );

    let dispatched = decode_evm_log(&log(
        ORMP_MESSAGE_DISPATCHED_TOPIC,
        ORMP_ADDRESS,
        encode(&[Token::FixedBytes(msg_hash), Token::Bool(true)]),
    ))
    .expect("MessageDispatched decodes");
    assert_eq!(
        dispatched,
        LegacyOrmPEvent::MessageDispatched {
            metadata: metadata(ORMP_ADDRESS),
            target_chain_id: 1,
            msg_hash: bytes_hex(0x11),
            dispatch_result: true,
        }
    );
}

#[test]
fn test_decode_msgport_and_signature_events_preserves_legacy_fields() {
    let msg_id = bytes32(0x33);

    let recv = decode_evm_log(&log(
        MSGPORT_MESSAGE_RECV_TOPIC,
        MSGPORT_ADDRESS,
        encode(&[
            Token::FixedBytes(msg_id.clone()),
            Token::Bool(false),
            Token::Bytes(vec![0xff]),
        ]),
    ))
    .expect("MessageRecv decodes");
    assert_eq!(
        recv,
        LegacyOrmPEvent::MsgportMessageRecv {
            metadata: metadata(MSGPORT_ADDRESS),
            msg_id: bytes_hex(0x33),
            result: false,
            return_data: "0xff".to_owned(),
        }
    );

    let sent = decode_evm_log(&log(
        MSGPORT_MESSAGE_SENT_TOPIC,
        MSGPORT_ADDRESS,
        encode(&[
            Token::FixedBytes(msg_id),
            Token::Address(address(0x30)),
            Token::Uint(U256::from(42161)),
            Token::Address(address(0x31)),
            Token::Bytes(vec![0xaa]),
            Token::Bytes(vec![0xbb, 0xcc]),
        ]),
    ))
    .expect("MessageSent decodes");
    assert_eq!(
        sent,
        LegacyOrmPEvent::MsgportMessageSent {
            metadata: metadata(MSGPORT_ADDRESS),
            msg_id: bytes_hex(0x33),
            from_dapp: address_hex(0x30),
            to_chain_id: 42161,
            to_dapp: address_hex(0x31),
            message: "0xaa".to_owned(),
            params: "0xbbcc".to_owned(),
        }
    );

    let submittion = decode_evm_log(&log(
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
        SIGNATURE_PUB_ADDRESS,
        encode(&[
            Token::Uint(U256::from(46)),
            Token::Address(address(0x40)),
            Token::Address(address(0x41)),
            Token::Uint(U256::from(12)),
            Token::Bytes(vec![0xde, 0xad]),
            Token::Bytes(vec![0xbe, 0xef]),
        ]),
    ))
    .expect("SignatureSubmittion decodes");
    assert_eq!(
        submittion,
        LegacyOrmPEvent::SignatureSubmittion {
            metadata: metadata(SIGNATURE_PUB_ADDRESS),
            chain_id: 46,
            channel: address_hex(0x40),
            signer: address_hex(0x41),
            msg_index: 12,
            signature: "0xdead".to_owned(),
            data: "0xbeef".to_owned(),
        }
    );
}

#[test]
fn test_decode_native_graphql_log_preserves_metadata_in_legacy_row() {
    let log: DatalensLog = serde_json::from_value(serde_json::json!({
        "id": "46-888-7",
        "chainId": 46,
        "blockNumber": 888,
        "blockHash": "0x6de65e4800000000000000000000000000000000000000000000000000000000",
        "blockTimestamp": 1_800_000_000_000_u64,
        "transactionHash": bytes_hex(0xaa).to_ascii_uppercase(),
        "transactionIndex": 9,
        "logIndex": 7,
        "address": MSGPORT_ADDRESS.to_ascii_uppercase(),
        "transactionFrom": address_hex(0x51).to_ascii_uppercase(),
        "topics": [MSGPORT_MESSAGE_SENT_TOPIC],
        "data": format!(
            "0x{}",
            hex::encode(encode(&[
                Token::FixedBytes(bytes32(0x33)),
                Token::Address(address(0x30)),
                Token::Uint(U256::from(42161)),
                Token::Address(address(0x31)),
                Token::Bytes(vec![0xaa]),
                Token::Bytes(vec![0xbb, 0xcc]),
            ]))
        )
    }))
    .expect("decode native GraphQL log");

    let row = MsgportMessageSentRow::from_event(decode_evm_log(&log).expect("decode EVM event"));

    assert_eq!(row.id, "0000000888-6de65-000007");
    assert_eq!(row.chain_id, 46);
    assert_eq!(row.block_number, 888);
    assert_eq!(row.block_timestamp, 1_800_000_000_000);
    assert_eq!(row.transaction_hash, bytes_hex(0xaa));
    assert_eq!(row.transaction_index, 9);
    assert_eq!(row.log_index, 7);
    assert_eq!(row.port_address, MSGPORT_ADDRESS.to_ascii_lowercase());
    assert_eq!(
        row.transaction_from.as_deref(),
        Some(address_hex(0x51).as_str())
    );
    assert_eq!(row.from_chain_id, 46);
    assert_eq!(row.msg_id, bytes_hex(0x33));
}

#[test]
fn test_decode_evm_log_normalizes_second_timestamps_to_milliseconds() {
    let seconds_log = DatalensLog {
        block_timestamp: Some(1_700_000_000),
        ..log(
            MSGPORT_MESSAGE_RECV_TOPIC,
            MSGPORT_ADDRESS,
            encode(&[
                Token::FixedBytes(bytes32(0x33)),
                Token::Bool(false),
                Token::Bytes(vec![0xff]),
            ]),
        )
    };
    let millis_log = DatalensLog {
        block_timestamp: Some(1_700_000_000_000),
        ..log(
            MSGPORT_MESSAGE_RECV_TOPIC,
            MSGPORT_ADDRESS,
            encode(&[
                Token::FixedBytes(bytes32(0x33)),
                Token::Bool(false),
                Token::Bytes(vec![0xff]),
            ]),
        )
    };

    let seconds_row = MsgportMessageSentRow::from_event(
        decode_evm_log(&DatalensLog {
            topics: vec![MSGPORT_MESSAGE_SENT_TOPIC.to_owned()],
            data: format!(
                "0x{}",
                hex::encode(encode(&[
                    Token::FixedBytes(bytes32(0x33)),
                    Token::Address(address(0x30)),
                    Token::Uint(U256::from(42161)),
                    Token::Address(address(0x31)),
                    Token::Bytes(vec![0xaa]),
                    Token::Bytes(vec![0xbb, 0xcc]),
                ]))
            ),
            ..seconds_log
        })
        .expect("decode seconds timestamp"),
    );
    let millis_row = MsgportMessageSentRow::from_event(
        decode_evm_log(&DatalensLog {
            topics: vec![MSGPORT_MESSAGE_SENT_TOPIC.to_owned()],
            data: format!(
                "0x{}",
                hex::encode(encode(&[
                    Token::FixedBytes(bytes32(0x33)),
                    Token::Address(address(0x30)),
                    Token::Uint(U256::from(42161)),
                    Token::Address(address(0x31)),
                    Token::Bytes(vec![0xaa]),
                    Token::Bytes(vec![0xbb, 0xcc]),
                ]))
            ),
            ..millis_log
        })
        .expect("decode millis timestamp"),
    );

    assert_eq!(seconds_row.block_timestamp, 1_700_000_000_000);
    assert_eq!(millis_row.block_timestamp, 1_700_000_000_000);
}

#[test]
fn test_decode_evm_indexed_topics_preserves_legacy_fields() {
    let msg_hash = bytes32(0x44);
    let hash_imported = DatalensLog {
        topics: vec![ORMP_HASH_IMPORTED_TOPIC.to_owned(), address_topic(0x24)],
        data: format!(
            "0x{}",
            hex::encode(encode(&[
                Token::Uint(U256::from(46)),
                Token::Address(address(0x21)),
                Token::Uint(U256::from(7)),
                Token::FixedBytes(msg_hash.clone()),
            ]))
        ),
        ..log(ORMP_HASH_IMPORTED_TOPIC, ORMP_ADDRESS, Vec::new())
    };
    assert_eq!(
        decode_evm_log(&hash_imported).expect("indexed HashImported decodes"),
        LegacyOrmPEvent::HashImported {
            metadata: metadata(ORMP_ADDRESS),
            src_chain_id: 46,
            target_chain_id: 1,
            oracle: address_hex(0x24),
            channel: address_hex(0x21),
            msg_index: 7,
            hash: bytes_hex(0x44),
        }
    );

    let accepted = DatalensLog {
        topics: vec![ORMP_MESSAGE_ACCEPTED_TOPIC.to_owned(), bytes_hex(0x44)],
        data: format!(
            "0x{}",
            hex::encode(encode(&[Token::Tuple(vec![
                Token::Address(address(0x21)),
                Token::Uint(U256::from(8)),
                Token::Uint(U256::from(46)),
                Token::Address(address(0x22)),
                Token::Uint(U256::from(137)),
                Token::Address(address(0x23)),
                Token::Uint(U256::from(500_000)),
                Token::Bytes(vec![0xab, 0xcd]),
            ])]))
        ),
        ..log(ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_ADDRESS, Vec::new())
    };
    assert_eq!(
        decode_evm_log(&accepted).expect("indexed MessageAccepted decodes"),
        LegacyOrmPEvent::MessageAccepted {
            metadata: metadata(ORMP_ADDRESS),
            msg_hash: bytes_hex(0x44),
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

    let assigned = DatalensLog {
        topics: vec![
            ORMP_MESSAGE_ASSIGNED_TOPIC.to_owned(),
            bytes_hex(0x44),
            address_topic(0x24),
            address_topic(0x25),
        ],
        data: format!(
            "0x{}",
            hex::encode(encode(&[
                Token::Uint(U256::from(9)),
                Token::Uint(U256::from(10)),
                Token::Bytes(vec![0x01, 0x02]),
            ]))
        ),
        ..log(ORMP_MESSAGE_ASSIGNED_TOPIC, ORMP_ADDRESS, Vec::new())
    };
    assert_eq!(
        decode_evm_log(&assigned).expect("indexed MessageAssigned decodes"),
        LegacyOrmPEvent::MessageAssigned {
            metadata: metadata(ORMP_ADDRESS),
            msg_hash: format!("0x{}", hex::encode(msg_hash)),
            oracle: address_hex(0x24),
            relayer: address_hex(0x25),
            oracle_fee: 9,
            relayer_fee: 10,
            params: "0x0102".to_owned(),
        }
    );

    let sent = DatalensLog {
        topics: vec![MSGPORT_MESSAGE_SENT_TOPIC.to_owned(), bytes_hex(0x55)],
        data: format!(
            "0x{}",
            hex::encode(encode(&[
                Token::Address(address(0x30)),
                Token::Uint(U256::from(42161)),
                Token::Address(address(0x31)),
                Token::Bytes(vec![0xaa]),
                Token::Bytes(vec![0xbb, 0xcc]),
            ]))
        ),
        ..log(MSGPORT_MESSAGE_SENT_TOPIC, MSGPORT_ADDRESS, Vec::new())
    };
    assert_eq!(
        decode_evm_log(&sent).expect("indexed MessageSent decodes"),
        LegacyOrmPEvent::MsgportMessageSent {
            metadata: metadata(MSGPORT_ADDRESS),
            msg_id: bytes_hex(0x55),
            from_dapp: address_hex(0x30),
            to_chain_id: 42161,
            to_dapp: address_hex(0x31),
            message: "0xaa".to_owned(),
            params: "0xbbcc".to_owned(),
        }
    );

    let submittion = DatalensLog {
        topics: vec![
            SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC.to_owned(),
            uint_topic(46),
            address_topic(0x40),
            address_topic(0x41),
        ],
        data: format!(
            "0x{}",
            hex::encode(encode(&[
                Token::Uint(U256::from(12)),
                Token::Bytes(vec![0xde, 0xad]),
                Token::Bytes(vec![0xbe, 0xef]),
            ]))
        ),
        ..log(
            SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
            SIGNATURE_PUB_ADDRESS,
            Vec::new(),
        )
    };
    assert_eq!(
        decode_evm_log(&submittion).expect("indexed SignatureSubmittion decodes"),
        LegacyOrmPEvent::SignatureSubmittion {
            metadata: metadata(SIGNATURE_PUB_ADDRESS),
            chain_id: 46,
            channel: address_hex(0x40),
            signer: address_hex(0x41),
            msg_index: 12,
            signature: "0xdead".to_owned(),
            data: "0xbeef".to_owned(),
        }
    );
}

#[test]
fn test_decode_errors_are_explicit() {
    let missing_topic = DatalensLog {
        topics: Vec::new(),
        ..log(MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_ADDRESS, Vec::new())
    };
    assert!(
        decode_evm_log(&missing_topic)
            .expect_err("missing topic should fail")
            .to_string()
            .contains("missing topic0")
    );

    let unknown = DatalensLog {
        topics: vec![bytes_hex(0x99)],
        ..log(MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_ADDRESS, Vec::new())
    };
    assert!(
        decode_evm_log(&unknown)
            .expect_err("unknown topic should fail")
            .to_string()
            .contains("unsupported ORMP EVM event topic0")
    );

    let malformed = log(
        MSGPORT_MESSAGE_RECV_TOPIC,
        MSGPORT_ADDRESS,
        vec![0x01, 0x02],
    );
    assert!(
        decode_evm_log(&malformed)
            .expect_err("malformed ABI should fail")
            .to_string()
            .contains("decode ABI event data")
    );

    let missing_metadata = DatalensLog {
        block_timestamp: None,
        ..log(MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_ADDRESS, Vec::new())
    };
    assert!(
        decode_evm_log(&missing_metadata)
            .expect_err("missing metadata should fail")
            .to_string()
            .contains("missing block timestamp")
    );
}

fn log(topic: &str, address: &str, data: Vec<u8>) -> DatalensLog {
    DatalensLog {
        id: Some("1-100-0".to_owned()),
        chain_id: 1,
        block_number: 100,
        block_hash: None,
        parent_hash: None,
        block_timestamp: Some(1_700_000_000_000),
        transaction_hash: bytes_hex(0xaa),
        transaction_index: Some(2),
        log_index: 3,
        address: address.to_owned(),
        transaction_from: Some(address_hex(0x50)),
        topics: vec![topic.to_owned()],
        data: format!("0x{}", hex::encode(data)),
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    }
}

fn metadata(address: &str) -> ormpindexer::schema::ChainLogMetadata {
    ormpindexer::schema::ChainLogMetadata {
        id: "1-100-0".to_owned(),
        source: EventSource::Evm,
        chain_id: 1,
        block_number: 100,
        block_hash: None,
        block_timestamp: 1_700_000_000_000,
        transaction_hash: bytes_hex(0xaa),
        transaction_index: 2,
        log_index: 3,
        contract_address: address.to_ascii_lowercase(),
        transaction_from: Some(address_hex(0x50)),
    }
}

fn address(value: u64) -> H160 {
    H160::from_low_u64_be(value)
}

fn address_hex(value: u64) -> String {
    format!("0x{}", hex::encode(address(value).as_bytes()))
}

fn address_topic(value: u64) -> String {
    format!("0x{:0>64}", &address_hex(value)[2..])
}

fn uint_topic(value: u64) -> String {
    format!("0x{value:0>64x}")
}

fn bytes32(value: u8) -> Vec<u8> {
    vec![value; 32]
}

fn bytes_hex(value: u8) -> String {
    format!("0x{}", hex::encode(bytes32(value)))
}
