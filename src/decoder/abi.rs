use anyhow::{Context, bail, ensure};
use ethabi::{ParamType, Token, decode, ethereum_types::U256};

use crate::schema::{ChainLogMetadata, LegacyOrmPEvent};

pub(super) fn decode_hash_imported(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 1 {
        let mut tokens = decode_event(
            &[
                ParamType::Uint(256),
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::FixedBytes(32),
            ],
            data,
        )?;
        return Ok(LegacyOrmPEvent::HashImported {
            target_chain_id: metadata.chain_id,
            metadata,
            oracle: topic_address(topics, 1, "oracle")?,
            src_chain_id: token_uint(take(&mut tokens, "chainId")?)?,
            channel: token_address(take(&mut tokens, "channel")?)?,
            msg_index: token_uint(take(&mut tokens, "msgIndex")?)?,
            hash: token_fixed_bytes(take(&mut tokens, "hash")?)?,
        });
    }

    let mut tokens = decode_event(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::FixedBytes(32),
        ],
        data,
    )?;
    Ok(LegacyOrmPEvent::HashImported {
        target_chain_id: metadata.chain_id,
        metadata,
        oracle: token_address(take(&mut tokens, "oracle")?)?,
        src_chain_id: token_uint(take(&mut tokens, "chainId")?)?,
        channel: token_address(take(&mut tokens, "channel")?)?,
        msg_index: token_uint(take(&mut tokens, "msgIndex")?)?,
        hash: token_fixed_bytes(take(&mut tokens, "hash")?)?,
    })
}

pub(super) fn decode_message_accepted(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 1 {
        let mut tokens = decode_event(
            &[ParamType::Tuple(vec![
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Uint(256),
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Bytes,
            ])],
            data,
        )?;
        let message = take(&mut tokens, "message")?;
        let Token::Tuple(mut message) = message else {
            bail!("message is not an ABI tuple");
        };
        ensure!(message.len() == 8, "message tuple must contain 8 fields");

        return Ok(LegacyOrmPEvent::MessageAccepted {
            metadata,
            msg_hash: topic_fixed_bytes(topics, 1, "msgHash")?,
            channel: token_address(take(&mut message, "message.channel")?)?,
            index: token_uint(take(&mut message, "message.index")?)?,
            from_chain_id: token_uint(take(&mut message, "message.fromChainId")?)?,
            from: token_address(take(&mut message, "message.from")?)?,
            to_chain_id: token_uint(take(&mut message, "message.toChainId")?)?,
            to: token_address(take(&mut message, "message.to")?)?,
            gas_limit: token_uint(take(&mut message, "message.gasLimit")?)?,
            encoded: token_bytes(take(&mut message, "message.encoded")?)?,
        });
    }

    let mut tokens = decode_event(
        &[
            ParamType::FixedBytes(32),
            ParamType::Tuple(vec![
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Uint(256),
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Bytes,
            ]),
        ],
        data,
    )?;
    let msg_hash = token_fixed_bytes(take(&mut tokens, "msgHash")?)?;
    let message = take(&mut tokens, "message")?;
    let Token::Tuple(mut message) = message else {
        bail!("message is not an ABI tuple");
    };
    ensure!(message.len() == 8, "message tuple must contain 8 fields");

    Ok(LegacyOrmPEvent::MessageAccepted {
        metadata,
        msg_hash,
        channel: token_address(take(&mut message, "message.channel")?)?,
        index: token_uint(take(&mut message, "message.index")?)?,
        from_chain_id: token_uint(take(&mut message, "message.fromChainId")?)?,
        from: token_address(take(&mut message, "message.from")?)?,
        to_chain_id: token_uint(take(&mut message, "message.toChainId")?)?,
        to: token_address(take(&mut message, "message.to")?)?,
        gas_limit: token_uint(take(&mut message, "message.gasLimit")?)?,
        encoded: token_bytes(take(&mut message, "message.encoded")?)?,
    })
}

pub(super) fn decode_message_assigned(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 3 {
        let mut tokens = decode_event(
            &[ParamType::Uint(256), ParamType::Uint(256), ParamType::Bytes],
            data,
        )?;
        return Ok(LegacyOrmPEvent::MessageAssigned {
            metadata,
            msg_hash: topic_fixed_bytes(topics, 1, "msgHash")?,
            oracle: topic_address(topics, 2, "oracle")?,
            relayer: topic_address(topics, 3, "relayer")?,
            oracle_fee: token_uint(take(&mut tokens, "oracleFee")?)?,
            relayer_fee: token_uint(take(&mut tokens, "relayerFee")?)?,
            params: token_bytes(take(&mut tokens, "params")?)?,
        });
    }

    let mut tokens = decode_event(
        &[
            ParamType::FixedBytes(32),
            ParamType::Address,
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
        ],
        data,
    )?;
    Ok(LegacyOrmPEvent::MessageAssigned {
        metadata,
        msg_hash: token_fixed_bytes(take(&mut tokens, "msgHash")?)?,
        oracle: token_address(take(&mut tokens, "oracle")?)?,
        relayer: token_address(take(&mut tokens, "relayer")?)?,
        oracle_fee: token_uint(take(&mut tokens, "oracleFee")?)?,
        relayer_fee: token_uint(take(&mut tokens, "relayerFee")?)?,
        params: token_bytes(take(&mut tokens, "params")?)?,
    })
}

pub(super) fn decode_message_dispatched(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 1 {
        let mut tokens = decode_event(&[ParamType::Bool], data)?;
        return Ok(LegacyOrmPEvent::MessageDispatched {
            target_chain_id: metadata.chain_id,
            metadata,
            msg_hash: topic_fixed_bytes(topics, 1, "msgHash")?,
            dispatch_result: token_bool(take(&mut tokens, "dispatchResult")?)?,
        });
    }

    let mut tokens = decode_event(&[ParamType::FixedBytes(32), ParamType::Bool], data)?;
    Ok(LegacyOrmPEvent::MessageDispatched {
        target_chain_id: metadata.chain_id,
        metadata,
        msg_hash: token_fixed_bytes(take(&mut tokens, "msgHash")?)?,
        dispatch_result: token_bool(take(&mut tokens, "dispatchResult")?)?,
    })
}

pub(super) fn decode_msgport_message_recv(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 1 {
        let mut tokens = decode_event(&[ParamType::Bool, ParamType::Bytes], data)?;
        return Ok(LegacyOrmPEvent::MsgportMessageRecv {
            metadata,
            msg_id: topic_fixed_bytes(topics, 1, "msgId")?,
            result: token_bool(take(&mut tokens, "result")?)?,
            return_data: token_bytes(take(&mut tokens, "returnData")?)?,
        });
    }

    let mut tokens = decode_event(
        &[ParamType::FixedBytes(32), ParamType::Bool, ParamType::Bytes],
        data,
    )?;
    Ok(LegacyOrmPEvent::MsgportMessageRecv {
        metadata,
        msg_id: token_fixed_bytes(take(&mut tokens, "msgId")?)?,
        result: token_bool(take(&mut tokens, "result")?)?,
        return_data: token_bytes(take(&mut tokens, "returnData")?)?,
    })
}

pub(super) fn decode_msgport_message_sent(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 1 {
        let mut tokens = decode_event(
            &[
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Address,
                ParamType::Bytes,
                ParamType::Bytes,
            ],
            data,
        )?;
        return Ok(LegacyOrmPEvent::MsgportMessageSent {
            metadata,
            msg_id: topic_fixed_bytes(topics, 1, "msgId")?,
            from_dapp: token_address(take(&mut tokens, "fromDapp")?)?,
            to_chain_id: token_uint(take(&mut tokens, "toChainId")?)?,
            to_dapp: token_address(take(&mut tokens, "toDapp")?)?,
            message: token_bytes(take(&mut tokens, "message")?)?,
            params: token_bytes(take(&mut tokens, "params")?)?,
        });
    }

    let mut tokens = decode_event(
        &[
            ParamType::FixedBytes(32),
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Address,
            ParamType::Bytes,
            ParamType::Bytes,
        ],
        data,
    )?;
    Ok(LegacyOrmPEvent::MsgportMessageSent {
        metadata,
        msg_id: token_fixed_bytes(take(&mut tokens, "msgId")?)?,
        from_dapp: token_address(take(&mut tokens, "fromDapp")?)?,
        to_chain_id: token_uint(take(&mut tokens, "toChainId")?)?,
        to_dapp: token_address(take(&mut tokens, "toDapp")?)?,
        message: token_bytes(take(&mut tokens, "message")?)?,
        params: token_bytes(take(&mut tokens, "params")?)?,
    })
}

pub(super) fn decode_signature_submittion(
    metadata: ChainLogMetadata,
    topics: &[String],
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
    if topics.len() > 3 {
        let mut tokens = decode_event(
            &[ParamType::Uint(256), ParamType::Bytes, ParamType::Bytes],
            data,
        )?;
        return Ok(LegacyOrmPEvent::SignatureSubmittion {
            metadata,
            chain_id: topic_uint(topics, 1, "chainId")?,
            channel: topic_address(topics, 2, "channel")?,
            signer: topic_address(topics, 3, "signer")?,
            msg_index: token_uint(take(&mut tokens, "msgIndex")?)?,
            signature: token_bytes(take(&mut tokens, "signature")?)?,
            data: token_bytes(take(&mut tokens, "data")?)?,
        });
    }

    let mut tokens = decode_event(
        &[
            ParamType::Uint(256),
            ParamType::Address,
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Bytes,
            ParamType::Bytes,
        ],
        data,
    )?;
    Ok(LegacyOrmPEvent::SignatureSubmittion {
        metadata,
        chain_id: token_uint(take(&mut tokens, "chainId")?)?,
        channel: token_address(take(&mut tokens, "channel")?)?,
        signer: token_address(take(&mut tokens, "signer")?)?,
        msg_index: token_uint(take(&mut tokens, "msgIndex")?)?,
        signature: token_bytes(take(&mut tokens, "signature")?)?,
        data: token_bytes(take(&mut tokens, "data")?)?,
    })
}

fn decode_event(types: &[ParamType], data: &[u8]) -> anyhow::Result<Vec<Token>> {
    decode(types, data).context("decode ABI event data")
}

fn take(tokens: &mut Vec<Token>, name: &str) -> anyhow::Result<Token> {
    if tokens.is_empty() {
        bail!("ABI token {name} is missing");
    }
    Ok(tokens.remove(0))
}

fn token_address(token: Token) -> anyhow::Result<String> {
    match token {
        Token::Address(value) => Ok(format!("0x{}", hex::encode(value.as_bytes()))),
        _ => bail!("ABI token is not an address"),
    }
}

fn token_fixed_bytes(token: Token) -> anyhow::Result<String> {
    match token {
        Token::FixedBytes(value) => Ok(format!("0x{}", hex::encode(value))),
        _ => bail!("ABI token is not fixed bytes"),
    }
}

fn token_bytes(token: Token) -> anyhow::Result<String> {
    match token {
        Token::Bytes(value) => Ok(format!("0x{}", hex::encode(value))),
        _ => bail!("ABI token is not bytes"),
    }
}

fn token_bool(token: Token) -> anyhow::Result<bool> {
    match token {
        Token::Bool(value) => Ok(value),
        _ => bail!("ABI token is not bool"),
    }
}

fn token_uint(token: Token) -> anyhow::Result<u128> {
    match token {
        Token::Uint(value) => {
            ensure!(value.bits() <= 128, "ABI uint overflows u128");
            Ok(value.as_u128())
        }
        _ => bail!("ABI token is not uint"),
    }
}

fn topic_fixed_bytes(topics: &[String], index: usize, name: &str) -> anyhow::Result<String> {
    let topic = topics
        .get(index)
        .with_context(|| format!("EVM indexed topic {name} is missing"))?;
    let topic = normalize_hex(topic)?;
    ensure!(
        topic.len() == 66,
        "EVM indexed topic {name} must be 32 bytes"
    );
    Ok(topic)
}

fn topic_address(topics: &[String], index: usize, name: &str) -> anyhow::Result<String> {
    let topic = topics
        .get(index)
        .with_context(|| format!("EVM indexed topic {name} is missing"))?;
    let topic = normalize_hex(topic)?;
    let address = topic
        .strip_prefix("0x")
        .context("normalized topic is missing 0x prefix")?;
    ensure!(
        address.len() == 64,
        "EVM indexed topic {name} must be 32 bytes"
    );
    Ok(format!("0x{}", &address[24..64]))
}

fn topic_uint(topics: &[String], index: usize, name: &str) -> anyhow::Result<u128> {
    let topic = topics
        .get(index)
        .with_context(|| format!("EVM indexed topic {name} is missing"))?;
    let topic = normalize_hex(topic)?;
    let value = U256::from_str_radix(
        topic
            .strip_prefix("0x")
            .context("normalized topic is missing 0x prefix")?,
        16,
    )?;
    ensure!(
        value.bits() <= 128,
        "EVM indexed topic {name} overflows u128"
    );
    Ok(value.as_u128())
}

pub(super) fn normalize_hex(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid hex value"
    );
    Ok(format!("0x{}", value.to_ascii_lowercase()))
}

pub(super) fn normalize_block_timestamp(value: u64) -> Option<u64> {
    if (1_000_000_000..10_000_000_000).contains(&value) {
        value.checked_mul(1_000)
    } else {
        Some(value)
    }
}

pub(super) fn decode_hex(value: &str) -> anyhow::Result<Vec<u8>> {
    let value = value.trim();
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    Ok(hex::decode(value)?)
}
