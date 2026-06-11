use anyhow::{Context, bail, ensure};
use ethabi::{ParamType, Token, decode};

use crate::{
    datalens::DatalensLog,
    planner::{
        MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_HASH_IMPORTED_TOPIC,
        ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC, ORMP_MESSAGE_DISPATCHED_TOPIC,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
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
        ORMP_HASH_IMPORTED_TOPIC => decode_hash_imported(metadata, &data),
        ORMP_MESSAGE_ACCEPTED_TOPIC => decode_message_accepted(metadata, &data),
        ORMP_MESSAGE_ASSIGNED_TOPIC => decode_message_assigned(metadata, &data),
        ORMP_MESSAGE_DISPATCHED_TOPIC => decode_message_dispatched(metadata, &data),
        MSGPORT_MESSAGE_RECV_TOPIC => decode_msgport_message_recv(metadata, &data),
        MSGPORT_MESSAGE_SENT_TOPIC => decode_msgport_message_sent(metadata, &data),
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC => decode_signature_submittion(metadata, &data),
        _ => bail!("unsupported ORMP EVM event topic0 {topic0}"),
    }
}

fn evm_metadata(log: &DatalensLog) -> anyhow::Result<ChainLogMetadata> {
    Ok(ChainLogMetadata {
        id: log
            .id
            .clone()
            .context("EVM log is missing legacy event id")?,
        source: EventSource::Evm,
        chain_id: log.chain_id.into(),
        block_number: log.block_number.into(),
        block_timestamp: log
            .block_timestamp
            .context("EVM log is missing block timestamp")?
            .into(),
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

fn decode_hash_imported(
    metadata: ChainLogMetadata,
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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
    data: &[u8],
) -> anyhow::Result<LegacyOrmPEvent> {
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

fn decode_hex(value: &str) -> anyhow::Result<Vec<u8>> {
    let value = value.trim();
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    Ok(hex::decode(value)?)
}
