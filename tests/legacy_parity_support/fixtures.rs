use ethabi::{Token, encode, ethereum_types::U256};

use ormpindexer::{
    datalens::DatalensLog,
    planner::{
        MSGPORT_ADDRESS, MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_ADDRESS,
        ORMP_HASH_IMPORTED_TOPIC, ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC,
        ORMP_MESSAGE_DISPATCHED_TOPIC, SIGNATURE_PUB_ADDRESS,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC, TRON_CHAIN_ID,
    },
};

use super::values::{address, address_hex, bytes_hex, bytes32};

pub fn evm_fixture_logs() -> Vec<DatalensLog> {
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

pub fn tron_fixture_logs() -> Vec<DatalensLog> {
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
                "hash": bytes_hex(0x66)
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
                "msgHash": bytes_hex(0x55),
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
                "msgHash": bytes_hex(0x55),
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
                "msgHash": bytes_hex(0x77),
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
            "blockHash": "trontx",
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
        "blockHash": transaction_hash,
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

#[allow(clippy::too_many_arguments)]
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
        block_hash: Some(transaction_hash.clone()),
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
