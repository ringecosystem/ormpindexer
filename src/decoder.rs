use anyhow::{Context, bail, ensure};
use ethabi::{ParamType, Token, decode, ethereum_types::U256};
use serde_json::{Map, Value};

use crate::{
    datalens::DatalensLog,
    planner::{
        MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_HASH_IMPORTED_TOPIC,
        ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC, ORMP_MESSAGE_DISPATCHED_TOPIC,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC, TRON_CHAIN_ID, TRON_HASH_IMPORTED_EVENT,
        TRON_MESSAGE_ACCEPTED_EVENT, TRON_MESSAGE_ASSIGNED_EVENT, TRON_MESSAGE_DISPATCHED_EVENT,
        TRON_MESSAGE_RECV_EVENT, TRON_MESSAGE_SENT_EVENT, TRON_SIGNATURE_SUBMITTION_EVENT,
    },
    schema::{ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

#[allow(async_fn_in_trait)]
pub trait EventDecoder {
    async fn decode(&self, log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDecoder;

impl EventDecoder for NoopDecoder {
    async fn decode(&self, _log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EvmEventDecoder;

impl EventDecoder for EvmEventDecoder {
    async fn decode(&self, log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>> {
        if log.chain_id == TRON_CHAIN_ID {
            if matches!(
                log.event_name.as_deref(),
                Some(TRON_MESSAGE_RECV_EVENT | TRON_MESSAGE_SENT_EVENT)
            ) {
                return Ok(Vec::new());
            }
            return decode_tron_event(log).map(|event| vec![event]);
        }

        decode_evm_log(log).map(|event| vec![event])
    }
}

pub fn decode_evm_log(log: &DatalensLog) -> anyhow::Result<LegacyOrmPEvent> {
    let topic0 = log
        .topics
        .first()
        .map(|topic| normalize_hex(topic))
        .transpose()?
        .context("EVM log is missing topic0")?;
    let metadata = evm_metadata(log)?;
    let data = decode_hex(&log.data).context("decode EVM log data")?;

    match topic0.as_str() {
        ORMP_HASH_IMPORTED_TOPIC => decode_hash_imported(metadata, &log.topics, &data),
        ORMP_MESSAGE_ACCEPTED_TOPIC => decode_message_accepted(metadata, &log.topics, &data),
        ORMP_MESSAGE_ASSIGNED_TOPIC => decode_message_assigned(metadata, &log.topics, &data),
        ORMP_MESSAGE_DISPATCHED_TOPIC => decode_message_dispatched(metadata, &log.topics, &data),
        MSGPORT_MESSAGE_RECV_TOPIC => decode_msgport_message_recv(metadata, &log.topics, &data),
        MSGPORT_MESSAGE_SENT_TOPIC => decode_msgport_message_sent(metadata, &log.topics, &data),
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC => {
            decode_signature_submittion(metadata, &log.topics, &data)
        }
        _ => bail!("unsupported ORMP EVM event topic0 {topic0}"),
    }
}

fn evm_metadata(log: &DatalensLog) -> anyhow::Result<ChainLogMetadata> {
    let block_timestamp = normalize_block_timestamp(
        log.block_timestamp
            .context("EVM log is missing block timestamp")?,
    )
    .context("EVM block timestamp overflows u64")?;

    Ok(ChainLogMetadata {
        id: log
            .id
            .clone()
            .context("EVM log is missing legacy event id")?,
        source: EventSource::Evm,
        chain_id: log.chain_id.into(),
        block_number: log.block_number.into(),
        block_hash: log.block_hash.clone(),
        block_timestamp: block_timestamp.into(),
        transaction_hash: normalize_hex(&log.transaction_hash)?,
        transaction_index: log
            .transaction_index
            .context("EVM log is missing transaction index")?,
        log_index: i32::try_from(log.log_index).context("EVM log index overflows i32")?,
        contract_address: normalize_hex(&log.address)?,
        transaction_from: log
            .transaction_from
            .as_deref()
            .map(normalize_hex)
            .transpose()?,
    })
}

pub fn decode_tron_event(log: &DatalensLog) -> anyhow::Result<LegacyOrmPEvent> {
    let event_name = log
        .event_name
        .as_deref()
        .context("Tron event is missing event_name")?;
    let metadata = tron_metadata(log)?;
    let payload = log
        .non_indexed_fields
        .as_ref()
        .context("Tron event is missing non_indexed_fields")?;

    if let Some(data) = payload.as_str() {
        return decode_tron_raw_event(metadata, event_name, log, data);
    }

    let fields = payload
        .as_object()
        .context("Tron event payload must be an object")?;

    match event_name {
        TRON_HASH_IMPORTED_EVENT => decode_tron_hash_imported(metadata, fields),
        TRON_MESSAGE_ACCEPTED_EVENT => decode_tron_message_accepted(metadata, fields),
        TRON_MESSAGE_ASSIGNED_EVENT => decode_tron_message_assigned(metadata, fields),
        TRON_MESSAGE_DISPATCHED_EVENT => decode_tron_message_dispatched(metadata, fields),
        TRON_MESSAGE_RECV_EVENT => decode_tron_msgport_message_recv(metadata, fields),
        TRON_MESSAGE_SENT_EVENT => decode_tron_msgport_message_sent(metadata, fields),
        TRON_SIGNATURE_SUBMITTION_EVENT | "SignatureSubmission" => {
            decode_tron_signature_submittion(metadata, fields)
        }
        _ => bail!("unsupported ORMP Tron event name {event_name}"),
    }
}

fn decode_tron_raw_event(
    metadata: ChainLogMetadata,
    event_name: &str,
    log: &DatalensLog,
    data: &str,
) -> anyhow::Result<LegacyOrmPEvent> {
    let topics = tron_raw_topics(log)?;
    let data = decode_hex(data).context("decode Tron raw event data")?;

    match event_name {
        TRON_HASH_IMPORTED_EVENT => decode_hash_imported(metadata, &topics, &data),
        TRON_MESSAGE_ACCEPTED_EVENT => decode_message_accepted(metadata, &topics, &data),
        TRON_MESSAGE_ASSIGNED_EVENT => decode_message_assigned(metadata, &topics, &data),
        TRON_MESSAGE_DISPATCHED_EVENT => decode_message_dispatched(metadata, &topics, &data),
        TRON_MESSAGE_RECV_EVENT => decode_msgport_message_recv(metadata, &topics, &data),
        TRON_MESSAGE_SENT_EVENT => decode_msgport_message_sent(metadata, &topics, &data),
        TRON_SIGNATURE_SUBMITTION_EVENT | "SignatureSubmission" => {
            decode_signature_submittion(metadata, &topics, &data)
        }
        _ => bail!("unsupported ORMP Tron event name {event_name}"),
    }
}

fn tron_raw_topics(log: &DatalensLog) -> anyhow::Result<Vec<String>> {
    if !log.indexed_fields.is_empty() {
        return log
            .indexed_fields
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .as_str()
                    .map(ToOwned::to_owned)
                    .with_context(|| format!("Tron indexed field {index} must be a hex string"))
            })
            .collect();
    }

    Ok(log.topics.clone())
}

fn tron_metadata(log: &DatalensLog) -> anyhow::Result<ChainLogMetadata> {
    let block_timestamp = normalize_block_timestamp(
        log.block_timestamp
            .context("Tron event is missing block timestamp")?,
    )
    .context("Tron block timestamp overflows u64")?;

    Ok(ChainLogMetadata {
        id: log
            .id
            .clone()
            .context("Tron event is missing legacy event id")?,
        source: EventSource::Tron,
        chain_id: log.chain_id.into(),
        block_number: log.block_number.into(),
        block_hash: log.block_hash.clone(),
        block_timestamp: block_timestamp.into(),
        transaction_hash: normalize_tron_transaction_hash(&log.transaction_hash),
        transaction_index: log
            .transaction_index
            .context("Tron event is missing transaction index")?,
        log_index: i32::try_from(log.log_index).context("Tron event index overflows i32")?,
        contract_address: normalize_tron_address(&log.address)?,
        transaction_from: log
            .transaction_from
            .as_deref()
            .map(normalize_tron_address)
            .transpose()?,
    })
}

fn decode_tron_hash_imported(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    Ok(LegacyOrmPEvent::HashImported {
        target_chain_id: metadata.chain_id,
        metadata,
        oracle: tron_address_field(fields, &["oracle"])?,
        src_chain_id: tron_uint_field(fields, &["chainId", "srcChainId"])?,
        channel: tron_address_field(fields, &["channel"])?,
        msg_index: tron_uint_field(fields, &["msgIndex"])?,
        hash: tron_hex_field(fields, &["hash"])?,
    })
}

fn decode_tron_message_accepted(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    let message = match optional_field(fields, &["message"])? {
        Some(Value::Object(message)) => Some(message),
        Some(_) => bail!("Tron event field message must be an object"),
        None => None,
    };
    let message_fields = message.unwrap_or(fields);

    Ok(LegacyOrmPEvent::MessageAccepted {
        metadata,
        msg_hash: tron_hex_field(fields, &["msgHash", "messageHash"])?,
        channel: tron_address_field(message_fields, &["channel"])?,
        index: tron_uint_field(message_fields, &["index"])?,
        from_chain_id: tron_uint_field(message_fields, &["fromChainId"])?,
        from: tron_address_field(message_fields, &["from"])?,
        to_chain_id: tron_uint_field(message_fields, &["toChainId"])?,
        to: tron_address_field(message_fields, &["to"])?,
        gas_limit: tron_uint_field(message_fields, &["gasLimit"])?,
        encoded: tron_hex_field(message_fields, &["encoded"])?,
    })
}

fn decode_tron_message_assigned(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    Ok(LegacyOrmPEvent::MessageAssigned {
        metadata,
        msg_hash: tron_hex_field(fields, &["msgHash", "messageHash"])?,
        oracle: tron_address_field(fields, &["oracle"])?,
        relayer: tron_address_field(fields, &["relayer"])?,
        oracle_fee: tron_uint_field(fields, &["oracleFee"])?,
        relayer_fee: tron_uint_field(fields, &["relayerFee"])?,
        params: tron_hex_field(fields, &["params"])?,
    })
}

fn decode_tron_message_dispatched(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    Ok(LegacyOrmPEvent::MessageDispatched {
        target_chain_id: metadata.chain_id,
        metadata,
        msg_hash: tron_hex_field(fields, &["msgHash", "messageHash"])?,
        dispatch_result: tron_bool_field(fields, &["dispatchResult"])?,
    })
}

fn decode_tron_msgport_message_recv(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    Ok(LegacyOrmPEvent::MsgportMessageRecv {
        metadata,
        msg_id: tron_hex_field(fields, &["msgId", "messageId"])?,
        result: tron_bool_field(fields, &["result"])?,
        return_data: tron_hex_field(fields, &["returnData"])?,
    })
}

fn decode_tron_msgport_message_sent(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    Ok(LegacyOrmPEvent::MsgportMessageSent {
        metadata,
        msg_id: tron_hex_field(fields, &["msgId", "messageId"])?,
        from_dapp: tron_address_field(fields, &["fromDapp"])?,
        to_chain_id: tron_uint_field(fields, &["toChainId"])?,
        to_dapp: tron_address_field(fields, &["toDapp"])?,
        message: tron_hex_field(fields, &["message"])?,
        params: tron_hex_field(fields, &["params"])?,
    })
}

fn decode_tron_signature_submittion(
    metadata: ChainLogMetadata,
    fields: &Map<String, Value>,
) -> anyhow::Result<LegacyOrmPEvent> {
    Ok(LegacyOrmPEvent::SignatureSubmittion {
        metadata,
        chain_id: tron_uint_field(fields, &["chainId"])?,
        channel: tron_address_field(fields, &["channel"])?,
        signer: tron_address_field(fields, &["signer"])?,
        msg_index: tron_uint_field(fields, &["msgIndex"])?,
        signature: tron_hex_field(fields, &["signature"])?,
        data: tron_hex_field(fields, &["data"])?,
    })
}

fn decode_hash_imported(
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

fn decode_message_accepted(
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

fn decode_message_assigned(
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

fn decode_message_dispatched(
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

fn decode_msgport_message_recv(
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

fn decode_msgport_message_sent(
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

fn decode_signature_submittion(
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

fn normalize_hex(value: &str) -> anyhow::Result<String> {
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

fn normalize_block_timestamp(value: u64) -> Option<u64> {
    if (1_000_000_000..10_000_000_000).contains(&value) {
        value.checked_mul(1_000)
    } else {
        Some(value)
    }
}

fn normalize_tron_transaction_hash(value: &str) -> String {
    let trimmed = value.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        format!("0x{}", hex.to_ascii_lowercase())
    } else {
        trimmed.to_owned()
    }
}

fn optional_field<'a>(
    fields: &'a Map<String, Value>,
    names: &[&str],
) -> anyhow::Result<Option<&'a Value>> {
    for name in names {
        if let Some(value) = fields.get(*name) {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn required_field<'a>(fields: &'a Map<String, Value>, names: &[&str]) -> anyhow::Result<&'a Value> {
    optional_field(fields, names)?.with_context(|| {
        let name = names.first().copied().unwrap_or("unknown");
        format!("Tron event field {name} is missing")
    })
}

fn tron_hex_field(fields: &Map<String, Value>, names: &[&str]) -> anyhow::Result<String> {
    let name = names.first().copied().unwrap_or("unknown");
    let value = required_field(fields, names)?;
    let value = value
        .as_str()
        .with_context(|| format!("Tron event field {name} must be a hex string"))?;
    normalize_hex(value).with_context(|| format!("Tron event field {name} is invalid hex"))
}

fn tron_address_field(fields: &Map<String, Value>, names: &[&str]) -> anyhow::Result<String> {
    let name = names.first().copied().unwrap_or("unknown");
    let value = required_field(fields, names)?;
    let value = value
        .as_str()
        .with_context(|| format!("Tron event field {name} must be an address string"))?;
    normalize_tron_address(value)
}

fn tron_bool_field(fields: &Map<String, Value>, names: &[&str]) -> anyhow::Result<bool> {
    let name = names.first().copied().unwrap_or("unknown");
    match required_field(fields, names)? {
        Value::Bool(value) => Ok(*value),
        Value::String(value) if value == "true" => Ok(true),
        Value::String(value) if value == "false" => Ok(false),
        _ => bail!("Tron event field {name} must be a bool"),
    }
}

fn tron_uint_field(fields: &Map<String, Value>, names: &[&str]) -> anyhow::Result<u128> {
    let name = names.first().copied().unwrap_or("unknown");
    match required_field(fields, names)? {
        Value::Number(value) => value
            .as_u64()
            .map(u128::from)
            .with_context(|| format!("Tron event field {name} must be an unsigned integer")),
        Value::String(value) => value
            .parse::<u128>()
            .with_context(|| format!("Tron event field {name} must be an unsigned integer")),
        _ => bail!("Tron event field {name} must be an unsigned integer"),
    }
}

fn normalize_tron_address(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    ensure!(!value.is_empty(), "Tron address must not be empty");
    if value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .map(|hex| hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .unwrap_or(false)
        || (value.len() == 42
            && value.starts_with("41")
            && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Ok(value.to_ascii_lowercase());
    }

    Ok(value.to_owned())
}

fn decode_hex(value: &str) -> anyhow::Result<Vec<u8>> {
    let value = value.trim();
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    Ok(hex::decode(value)?)
}
