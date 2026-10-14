use anyhow::Context;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    config::FinalityMode,
    datalens::types::{
        DatalensBlock, DatalensBlockQuery, DatalensLog, DatalensLogQuery, IncompleteReason,
    },
};

const LOG_FIELDS: [&str; 10] = [
    "block_number",
    "block_hash",
    "transaction_hash",
    "transaction_index",
    "log_index",
    "address",
    "topics",
    "data",
    "block_timestamp",
    "transaction_from",
];

pub(super) fn log_query_body(query: &DatalensLogQuery) -> anyhow::Result<Value> {
    let mut body = json!({
        "chain": evm_chain_name(query.chain_id)?,
        "dataset": "evm.logs",
        "range": {"start": query.from_block, "end": query.to_block},
        "finality": query_finality(query.finality_mode),
        "fields": LOG_FIELDS,
    });
    if !query.contracts.is_empty() || !query.topics.is_empty() {
        let mut selector = serde_json::Map::new();
        if !query.contracts.is_empty() {
            selector.insert("addresses".to_owned(), json!(query.contracts));
        }
        if !query.topics.is_empty() {
            selector.insert("topics".to_owned(), json!(vec![query.topics.clone()]));
        }
        body["selector"] = Value::Object(selector);
    }
    Ok(body)
}

pub(super) fn block_query_body(query: &DatalensBlockQuery) -> anyhow::Result<Value> {
    Ok(json!({
        "chain": evm_chain_name(query.chain_id)?,
        "dataset": "evm.blocks",
        "range": {"start": query.from_block, "end": query.to_block},
        "finality": query_finality(query.finality_mode),
    }))
}

pub(super) fn query_finality(finality_mode: FinalityMode) -> &'static str {
    match finality_mode {
        FinalityMode::Finalized | FinalityMode::Durable => "durable_only",
        FinalityMode::Safe | FinalityMode::Latest => "safe_to_latest",
    }
}

pub(super) fn head_height(finality_mode: FinalityMode, heads: &WireHeads) -> u64 {
    match finality_mode {
        FinalityMode::Finalized => heads.finalized,
        FinalityMode::Durable | FinalityMode::Safe => heads.safe,
        FinalityMode::Latest => heads.latest,
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

#[derive(Clone, Debug, Deserialize)]
pub(super) struct WireHeads {
    pub latest: u64,
    pub safe: u64,
    pub finalized: u64,
}

#[derive(Clone, Debug)]
pub(super) struct QueryPage {
    pub served_start: Option<u64>,
    pub served_end: Option<u64>,
    pub incomplete_reason: Option<IncompleteReason>,
    pub next_cursor: Option<String>,
    pub rows: Vec<Value>,
}

pub(super) fn parse_query_page(body: &str) -> anyhow::Result<QueryPage> {
    let wire: WireQuery = serde_json::from_str(body).context("decode Datalens query response")?;
    Ok(QueryPage {
        served_start: wire.served.as_ref().map(|range| range.start),
        served_end: wire.served.as_ref().map(|range| range.end),
        incomplete_reason: wire.incomplete_reason,
        next_cursor: wire.page.next_cursor,
        rows: wire.rows,
    })
}

pub(super) fn logs_from_page(page: &QueryPage, chain_id: u64) -> anyhow::Result<Vec<DatalensLog>> {
    let Some(served_start) = page.served_start else {
        return Ok(Vec::new());
    };
    let served_end = page.served_end.unwrap_or(served_start);
    page.rows
        .iter()
        .map(|row| log_from_row(row, chain_id))
        .filter(|log| match log {
            Ok(log) => log.block_number >= served_start && log.block_number <= served_end,
            Err(_) => true,
        })
        .collect()
}

pub(super) fn blocks_from_page(
    page: &QueryPage,
    chain_id: u64,
) -> anyhow::Result<Vec<DatalensBlock>> {
    page.rows
        .iter()
        .map(|row| block_from_row(row, chain_id))
        .collect()
}

fn log_from_row(row: &Value, chain_id: u64) -> anyhow::Result<DatalensLog> {
    let row: WireLog = serde_json::from_value(row.clone()).context("decode Datalens log row")?;
    let transaction_index = i32::try_from(row.transaction_index)
        .context("Datalens log transaction_index overflows i32")?;
    Ok(DatalensLog {
        id: Some(format!(
            "{chain_id}-{}-{}-{}",
            row.block_number, row.transaction_hash, row.log_index
        )),
        chain_id,
        block_number: row.block_number,
        block_hash: Some(row.block_hash),
        parent_hash: None,
        block_timestamp: row.block_timestamp,
        transaction_hash: row.transaction_hash,
        transaction_index: Some(transaction_index),
        log_index: u64::from(row.log_index),
        address: row.address,
        transaction_from: row.transaction_from,
        topics: row.topics,
        data: row.data,
        event_name: None,
        event_signature: None,
        indexed_fields: Vec::new(),
        non_indexed_fields: None,
    })
}

fn block_from_row(row: &Value, chain_id: u64) -> anyhow::Result<DatalensBlock> {
    let row: WireBlock =
        serde_json::from_value(row.clone()).context("decode Datalens block row")?;
    Ok(DatalensBlock {
        chain_id,
        block_number: row.number,
        block_hash: row.hash,
        parent_hash: Some(row.parent_hash),
    })
}

#[derive(Deserialize)]
struct WireQuery {
    served: Option<WireRange>,
    incomplete_reason: Option<IncompleteReason>,
    #[serde(default)]
    rows: Vec<Value>,
    #[serde(default)]
    page: WirePage,
}

#[derive(Deserialize)]
struct WireRange {
    start: u64,
    end: u64,
}

#[derive(Default, Deserialize)]
struct WirePage {
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct WireLog {
    block_number: u64,
    block_hash: String,
    #[serde(default)]
    block_timestamp: Option<u64>,
    transaction_hash: String,
    transaction_index: u32,
    log_index: u32,
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(default)]
    transaction_from: Option<String>,
}

#[derive(Deserialize)]
struct WireBlock {
    number: u64,
    hash: String,
    parent_hash: String,
}
