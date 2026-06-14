use anyhow::Context;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    config::FinalityMode,
    datalens::types::{
        DatalensLog, DatalensLogQuery, DatalensTransaction, DatalensTransactionQuery,
    },
    planner::TRON_CHAIN_ID,
};

pub fn native_graphql_request(query: &DatalensLogQuery) -> anyhow::Result<serde_json::Value> {
    let input = if query.chain_id == TRON_CHAIN_ID {
        tron_query_input(query)?
    } else {
        evm_query_input(query)?
    };

    Ok(json!({
        "query": r#"
            query OrmpIndexerLogs($input: QueryInput!) {
              query(input: $input) {
                rows
              }
            }
        "#,
        "variables": {
            "input": input
        }
    }))
}

pub fn native_graphql_transaction_request(
    query: &DatalensTransactionQuery,
) -> anyhow::Result<serde_json::Value> {
    let input = evm_transaction_query_input(query)?;

    Ok(json!({
        "query": r#"
            query OrmpIndexerTransactions($input: QueryInput!) {
              query(input: $input) {
                rows
              }
            }
        "#,
        "variables": {
            "input": input
        }
    }))
}

pub fn logs_from_native_query_payload(
    payload: &serde_json::Value,
    chain_id: u64,
) -> anyhow::Result<Vec<DatalensLog>> {
    let rows = payload
        .pointer("/data/query/rows")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    if chain_id == TRON_CHAIN_ID {
        let logs = native_tron_event_rows(&rows)?;
        return logs
            .into_iter()
            .map(|row| row.into_datalens_log(chain_id))
            .collect();
    }

    let logs = native_log_rows(&rows)?;
    logs.into_iter()
        .map(|row| row.into_datalens_log(chain_id))
        .collect()
}

pub fn transactions_from_native_query_payload(
    payload: &serde_json::Value,
    chain_id: u64,
) -> anyhow::Result<Vec<DatalensTransaction>> {
    if chain_id == TRON_CHAIN_ID {
        anyhow::bail!("Datalens EVM transaction query does not support Tron chain {chain_id}");
    }

    let rows = payload
        .pointer("/data/query/rows")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    native_transaction_rows(&rows)
}

fn native_log_rows(rows: &serde_json::Value) -> anyhow::Result<Vec<NativeLogRow>> {
    let rows = native_rows(rows).context("Datalens native query response missing evm log rows")?;
    Ok(serde_json::from_value(rows.clone())?)
}

fn native_transaction_rows(rows: &serde_json::Value) -> anyhow::Result<Vec<DatalensTransaction>> {
    let rows =
        native_rows(rows).context("Datalens native query response missing evm transaction rows")?;
    Ok(serde_json::from_value(rows.clone())?)
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
struct NativeLogRow {
    #[serde(default)]
    id: Option<String>,
    block_number: u64,
    #[serde(default)]
    block_hash: Option<String>,
    #[serde(default)]
    block_timestamp: Option<u64>,
    transaction_hash: String,
    transaction_index: i32,
    log_index: u64,
    address: String,
    #[serde(default)]
    transaction_from: Option<String>,
    topics: Vec<String>,
    data: String,
}

impl NativeLogRow {
    fn into_datalens_log(self, chain_id: u64) -> anyhow::Result<DatalensLog> {
        let id = self.id.unwrap_or_else(|| {
            format!(
                "{}-{}-{}-{}",
                chain_id, self.block_number, self.transaction_hash, self.log_index
            )
        });

        Ok(DatalensLog {
            id: Some(id),
            chain_id,
            block_number: self.block_number,
            block_hash: self.block_hash,
            block_timestamp: self.block_timestamp,
            transaction_hash: self.transaction_hash,
            transaction_index: Some(self.transaction_index),
            log_index: self.log_index,
            address: self.address,
            transaction_from: self.transaction_from,
            topics: self.topics,
            data: self.data,
            event_name: None,
            event_signature: None,
            indexed_fields: Vec::new(),
            non_indexed_fields: None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
struct NativeTronEventRow {
    #[serde(default)]
    id: Option<String>,
    contract_address: String,
    #[serde(default)]
    event_name: Option<String>,
    #[serde(default)]
    event_signature: Option<String>,
    #[serde(default)]
    indexed_fields: Vec<serde_json::Value>,
    #[serde(default)]
    non_indexed_fields: Option<serde_json::Value>,
    transaction_id: String,
    block_number: u64,
    #[serde(default)]
    block_hash: Option<String>,
    block_timestamp: u64,
    transaction_index: i32,
    event_index: u64,
}

impl NativeTronEventRow {
    fn into_datalens_log(self, chain_id: u64) -> anyhow::Result<DatalensLog> {
        let id = self.id.unwrap_or_else(|| {
            format!(
                "{}-{}-{}-{}",
                chain_id, self.block_number, self.transaction_id, self.event_index
            )
        });
        let topics = self
            .indexed_fields
            .iter()
            .filter_map(|field| field.as_str().map(ToOwned::to_owned))
            .collect::<Vec<_>>();
        let data = self
            .non_indexed_fields
            .as_ref()
            .map(|fields| {
                fields
                    .as_str()
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| fields.to_string())
            })
            .unwrap_or_default();

        Ok(DatalensLog {
            id: Some(id),
            chain_id,
            block_number: self.block_number,
            block_hash: self.block_hash,
            block_timestamp: Some(self.block_timestamp),
            transaction_hash: self.transaction_id,
            transaction_index: Some(self.transaction_index),
            log_index: self.event_index,
            address: self.contract_address,
            transaction_from: None,
            topics,
            data,
            event_name: self.event_name,
            event_signature: self.event_signature,
            indexed_fields: self.indexed_fields,
            non_indexed_fields: self.non_indexed_fields,
        })
    }
}

fn native_tron_event_rows(rows: &serde_json::Value) -> anyhow::Result<Vec<NativeTronEventRow>> {
    let rows =
        native_rows(rows).context("Datalens native query response missing Tron event rows")?;
    Ok(serde_json::from_value(rows.clone())?)
}

fn native_rows(rows: &serde_json::Value) -> Option<&serde_json::Value> {
    if rows.is_array() {
        return Some(rows);
    }

    rows.get("rows").and_then(native_rows)
}

fn evm_query_input(query: &DatalensLogQuery) -> anyhow::Result<serde_json::Value> {
    let chain_name = evm_chain_name(query.chain_id)?;
    Ok(json!({
        "chain": {
            "family": { "kind": "evm" },
            "configuredName": chain_name,
            "networkId": { "numeric": query.chain_id },
        },
        "datasetKey": {
            "family": "evm",
            "name": "logs",
        },
        "selector": {
            "kind": "evm_logs",
            "evmLogs": {
                "addresses": query.contracts,
                "topics": topic_filters(&query.topics),
            },
        },
        "range": {
            "kind": "block",
            "start": query.from_block,
            "end": query.to_block,
        },
        "finality": native_finality(query.finality_mode),
        "fields": {},
    }))
}

fn evm_transaction_query_input(
    query: &DatalensTransactionQuery,
) -> anyhow::Result<serde_json::Value> {
    let chain_name = evm_chain_name(query.chain_id)?;
    Ok(json!({
        "chain": {
            "family": { "kind": "evm" },
            "configuredName": chain_name,
            "networkId": { "numeric": query.chain_id },
        },
        "datasetKey": {
            "family": "evm",
            "name": "transactions",
        },
        "selector": {
            "kind": "all",
        },
        "range": {
            "kind": "block",
            "start": query.from_block,
            "end": query.to_block,
        },
        "finality": native_finality(query.finality_mode),
        "fields": {},
    }))
}

fn tron_query_input(query: &DatalensLogQuery) -> anyhow::Result<serde_json::Value> {
    let selector = tron_event_selector(&query.contracts, &query.topics)?;
    Ok(json!({
        "chain": {
            "family": { "kind": "other", "other": "tron" },
            "configuredName": tron_chain_name(query.chain_id)?,
            "networkId": { "numeric": query.chain_id },
        },
        "datasetKey": {
            "family": "tron",
            "name": "events",
        },
        "selector": {
            "kind": "other",
            "other": selector,
        },
        "range": {
            "kind": "block",
            "start": query.from_block,
            "end": query.to_block,
        },
        "finality": native_finality(query.finality_mode),
        "fields": {},
    }))
}

fn tron_event_selector(
    contracts: &[String],
    event_names: &[String],
) -> anyhow::Result<serde_json::Value> {
    let mut contracts = contracts
        .iter()
        .map(|address| normalize_tron_contract_address(address))
        .collect::<anyhow::Result<Vec<_>>>()?;
    contracts.sort();
    contracts.dedup();
    if contracts.is_empty() {
        anyhow::bail!("Tron event selector requires at least one contract address");
    }

    let mut event_names = event_names
        .iter()
        .map(|name| normalize_tron_event_name(name))
        .collect::<anyhow::Result<Vec<_>>>()?;
    event_names.sort();
    event_names.dedup();

    let canonical_key = format!(
        "contracts/{}/events/{}",
        contracts.join("+"),
        if event_names.is_empty() {
            "all".to_owned()
        } else {
            event_names.join("+")
        }
    );

    Ok(json!({
        "kind": "tron_events",
        "fingerprint": format!("tron-events/ormp-v3/{}", digest_prefix(&canonical_key, 12)),
        "canonicalKey": canonical_key,
    }))
}

fn normalize_tron_contract_address(address: &str) -> anyhow::Result<String> {
    let address = address.trim();
    let hex = address.strip_prefix("0x").unwrap_or(address);
    if hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(format!("41{}", hex.to_ascii_lowercase()));
    }
    if hex.len() == 42 && hex.starts_with("41") && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(hex.to_ascii_lowercase());
    }
    if address.len() == 34
        && address.starts_with('T')
        && address.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Ok(address.to_owned());
    }

    anyhow::bail!("Tron contract address must be hex, 41-prefixed hex, or base58")
}

fn normalize_tron_event_name(name: &str) -> anyhow::Result<String> {
    let name = name.trim();
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        anyhow::bail!("Tron event name must be a non-empty identifier");
    }
    Ok(name.to_owned())
}

fn digest_prefix(value: &str, bytes: usize) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex::encode(&digest[..bytes])
}

fn topic_filters(topics: &[String]) -> Vec<Vec<String>> {
    if topics.is_empty() {
        Vec::new()
    } else {
        vec![topics.to_vec()]
    }
}

fn native_finality(finality_mode: FinalityMode) -> &'static str {
    match finality_mode {
        FinalityMode::Finalized => "durable_only",
        FinalityMode::Durable => "durable_only",
    }
}

pub(super) fn chain_head_finality(finality_mode: FinalityMode) -> &'static str {
    match finality_mode {
        FinalityMode::Finalized => "finalized",
        FinalityMode::Durable => "finalized",
    }
}

pub(super) fn chain_name(chain_id: u64) -> anyhow::Result<&'static str> {
    if chain_id == TRON_CHAIN_ID {
        tron_chain_name(chain_id)
    } else {
        evm_chain_name(chain_id)
    }
}

pub fn evm_chain_name(chain_id: u64) -> anyhow::Result<&'static str> {
    Ok(match chain_id {
        1 => "ethereum",
        44 => "crab",
        46 => "darwinia",
        137 => "polygon",
        1284 => "moonbeam",
        8453 => "base",
        42161 => "arbitrum",
        81457 => "blast",
        2818 => "morph",
        _ => anyhow::bail!("unsupported EVM Datalens chain id: {chain_id}"),
    })
}

pub fn tron_chain_name(chain_id: u64) -> anyhow::Result<&'static str> {
    Ok(match chain_id {
        TRON_CHAIN_ID => "tron-mainnet",
        _ => anyhow::bail!("unsupported Tron Datalens chain id: {chain_id}"),
    })
}
