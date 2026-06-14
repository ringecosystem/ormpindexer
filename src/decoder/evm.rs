use anyhow::{Context, bail};

use crate::{
    datalens::DatalensLog,
    decoder::abi::{
        decode_hash_imported, decode_hex, decode_message_accepted, decode_message_assigned,
        decode_message_dispatched, decode_msgport_message_recv, decode_msgport_message_sent,
        decode_signature_submittion, normalize_block_timestamp, normalize_hex,
    },
    planner::{
        MSGPORT_MESSAGE_RECV_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC, ORMP_HASH_IMPORTED_TOPIC,
        ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC, ORMP_MESSAGE_DISPATCHED_TOPIC,
        SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
    },
    schema::{ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

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
