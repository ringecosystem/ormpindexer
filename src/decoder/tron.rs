use anyhow::{Context, bail, ensure};
use serde_json::{Map, Value};

use crate::{
    datalens::DatalensLog,
    decoder::abi::{
        decode_hash_imported, decode_hex, decode_message_accepted, decode_message_assigned,
        decode_message_dispatched, decode_msgport_message_recv, decode_msgport_message_sent,
        decode_signature_submittion, normalize_block_timestamp, normalize_hex,
    },
    planner::{
        TRON_HASH_IMPORTED_EVENT, TRON_MESSAGE_ACCEPTED_EVENT, TRON_MESSAGE_ASSIGNED_EVENT,
        TRON_MESSAGE_DISPATCHED_EVENT, TRON_MESSAGE_RECV_EVENT, TRON_MESSAGE_SENT_EVENT,
        TRON_SIGNATURE_SUBMITTION_EVENT,
    },
    schema::{ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

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
