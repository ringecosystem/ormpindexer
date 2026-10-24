use std::collections::{HashMap, HashSet};

use anyhow::{Context, ensure};
use serde_json::{Value, json};

use crate::{
    datalens::DatalensLog,
    decoder::decode_evm_log,
    planner::{
        MSGPORT_MESSAGE_SENT_TOPIC, ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC,
    },
    schema::LegacyOrmPEvent,
};

use super::{
    rpc::{CHAIN_ID, MSGPORT, ORMP, RpcLog, TronRpc, hex_u64, quantity, text},
    storage::RecoveryStore,
};

pub(super) async fn recover(
    rpc: &TronRpc,
    store: Option<&RecoveryStore>,
    start: u64,
    end: u64,
    batch_size: u64,
) -> anyhow::Result<(usize, usize)> {
    let mut cursor = start;
    let mut ranges = 0;
    let mut count = 0;
    while cursor <= end {
        let mut range_end = cursor.saturating_add(batch_size - 1).min(end);
        let mut logs = loop {
            match rpc.logs(cursor, range_end).await {
                Ok(logs) => break logs,
                Err(error) if range_end == cursor => {
                    return Err(error).context("Tron range failed; progress unchanged");
                }
                Err(_) => {
                    range_end = cursor + (range_end - cursor) / 2;
                    log::warn!("retry Tron range with start={cursor} end={range_end}");
                }
            }
        };
        logs.sort_by_key(|log| {
            (
                hex_u64(&log.block_number).unwrap_or(u64::MAX),
                hex_u64(&log.transaction_index).unwrap_or(u64::MAX),
                hex_u64(&log.log_index).unwrap_or(u64::MAX),
            )
        });
        let events = decode_range(rpc, logs, cursor, range_end).await?;
        let next = range_end
            .checked_add(1)
            .context("Tron recovery height overflow")?;
        if let Some(store) = store {
            store.commit(&events, next).await?;
        }
        for event in &events {
            if let LegacyOrmPEvent::MessageAccepted {
                msg_hash,
                index,
                from_chain_id,
                to_chain_id,
                ..
            } = event
            {
                log::info!(
                    "Tron message hash={msg_hash} index={index} from_chain={from_chain_id} to_chain={to_chain_id}"
                );
            }
        }
        log::info!(
            "Tron recovery batch start={cursor} end={range_end} events={} written={} next_block={next}",
            events.len(),
            store.is_some()
        );
        cursor = next;
        ranges += 1;
        count += events.len();
    }
    Ok((ranges, count))
}

async fn decode_range(
    rpc: &TronRpc,
    logs: Vec<RpcLog>,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<LegacyOrmPEvent>> {
    let mut blocks = HashMap::<u64, Value>::new();
    let mut transactions = HashMap::<String, Value>::new();
    let mut seen = HashSet::new();
    let mut events = Vec::new();
    for log in logs {
        let number = hex_u64(&log.block_number)?;
        ensure!(
            (start..=end).contains(&number) && !log.removed,
            "Tron returned an unrequested or removed log"
        );
        let address = log.address.to_ascii_lowercase();
        let topic = log.topics.first().context("Tron log has no topic0")?;
        ensure!(
            (address == ORMP
                && matches!(
                    topic.as_str(),
                    ORMP_MESSAGE_ACCEPTED_TOPIC | ORMP_MESSAGE_ASSIGNED_TOPIC
                ))
                || (address == MSGPORT && topic == MSGPORT_MESSAGE_SENT_TOPIC),
            "Tron returned an unexpected contract/event"
        );
        let index = hex_u64(&log.log_index)?;
        ensure!(
            seen.insert((number, index)),
            "Tron returned duplicate log positions"
        );
        if let std::collections::hash_map::Entry::Vacant(entry) = blocks.entry(number) {
            let block = rpc
                .call(
                    "eth_getBlockByNumber",
                    json!([format!("0x{number:x}"), false]),
                )
                .await?;
            ensure!(
                quantity(&block, "number")? == number,
                "Tron block number mismatch"
            );
            entry.insert(block);
        }
        let block = &blocks[&number];
        ensure!(
            text(block, "hash")?.eq_ignore_ascii_case(&log.block_hash),
            "Tron log block hash mismatch"
        );
        if !transactions.contains_key(&log.transaction_hash) {
            let tx = rpc
                .call("eth_getTransactionByHash", json!([log.transaction_hash]))
                .await?;
            transactions.insert(log.transaction_hash.clone(), tx);
        }
        let tx = &transactions[&log.transaction_hash];
        ensure!(
            text(tx, "hash")?.eq_ignore_ascii_case(&log.transaction_hash)
                && text(tx, "blockHash")?.eq_ignore_ascii_case(&log.block_hash)
                && quantity(tx, "blockNumber")? == number
                && quantity(tx, "transactionIndex")? == hex_u64(&log.transaction_index)?,
            "Tron transaction metadata mismatch"
        );
        let normalized = DatalensLog {
            id: Some(format!(
                "tron-{number}-{}-{index}",
                log.transaction_hash.to_ascii_lowercase()
            )),
            chain_id: CHAIN_ID,
            block_number: number,
            block_hash: Some(log.block_hash),
            parent_hash: Some(text(block, "parentHash")?.to_owned()),
            block_timestamp: Some(quantity(block, "timestamp")?),
            transaction_hash: log.transaction_hash,
            transaction_index: Some(i32::try_from(hex_u64(&log.transaction_index)?)?),
            log_index: index,
            address,
            transaction_from: Some(text(tx, "from")?.to_ascii_lowercase()),
            topics: log.topics,
            data: log.data,
            event_name: None,
            event_signature: None,
            indexed_fields: Vec::new(),
            non_indexed_fields: None,
        };
        // Tron JSON-RPC returns the same topic/data ABI as EVM. No old Datalens protocol is needed.
        let event = decode_evm_log(&normalized)?;
        if let LegacyOrmPEvent::MessageAccepted { from_chain_id, .. } = &event {
            ensure!(
                *from_chain_id == u128::from(CHAIN_ID),
                "Tron message source chain mismatch"
            );
        }
        events.push(event);
    }
    Ok(events)
}
