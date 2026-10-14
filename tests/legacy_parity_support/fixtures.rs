use ethabi::{Token, encode, ethereum_types::U256};

use ormpindexer::{
    datalens::DatalensLog,
    planner::{
        MSGPORT_ADDRESS, MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_ADDRESS,
        ORMP_HASH_IMPORTED_TOPIC, ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC,
        ORMP_MESSAGE_DISPATCHED_TOPIC, SIGNATURE_PUB_ADDRESS,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
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
        parent_hash: None,
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
