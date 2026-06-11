use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config::{DatalensConfig, FinalityMode};
use crate::planner::TRON_CHAIN_ID;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensLogQuery {
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: u64,
    pub contracts: Vec<String>,
    pub topics: Vec<String>,
    pub finality_mode: FinalityMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatalensLog {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(alias = "chain_id")]
    pub chain_id: u64,
    #[serde(alias = "block_number")]
    pub block_number: u64,
    #[serde(default, alias = "block_timestamp")]
    pub block_timestamp: Option<u64>,
    #[serde(alias = "transaction_hash")]
    pub transaction_hash: String,
    #[serde(default, alias = "transaction_index")]
    pub transaction_index: Option<i32>,
    #[serde(alias = "log_index", alias = "eventIndex", alias = "event_index")]
    pub log_index: u64,
    #[serde(alias = "contractAddress", alias = "contract_address")]
    pub address: String,
    #[serde(default, alias = "transaction_from")]
    pub transaction_from: Option<String>,
    pub topics: Vec<String>,
    pub data: String,
    #[serde(default, alias = "event_name")]
    pub event_name: Option<String>,
    #[serde(default, alias = "event_signature")]
    pub event_signature: Option<String>,
    #[serde(default, alias = "indexed_fields")]
    pub indexed_fields: Vec<serde_json::Value>,
    #[serde(default, alias = "non_indexed_fields")]
    pub non_indexed_fields: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensLogQueryResult {
    pub logs: Vec<DatalensLog>,
}

#[allow(async_fn_in_trait)]
pub trait DatalensLogReader {
    async fn latest_block(&self, chain_id: u64, finality_mode: FinalityMode)
    -> anyhow::Result<u64>;

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult>;
}

#[derive(Clone)]
pub struct DatalensHttpClient {
    config: DatalensConfig,
    http: reqwest::Client,
}

impl DatalensHttpClient {
    pub fn new(config: DatalensConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    fn native_graphql_endpoint(&self) -> String {
        format!(
            "{}/native/graphql",
            self.config.endpoint.trim_end_matches('/')
        )
    }

    fn chain_head_endpoint(
        &self,
        chain_id: u64,
        finality_mode: FinalityMode,
    ) -> anyhow::Result<String> {
        let chain_name = chain_name(chain_id)?;
        Ok(format!(
            "{}/v1/chains/{chain_name}/head?finality={}",
            self.config.endpoint.trim_end_matches('/'),
            chain_head_finality(finality_mode),
        ))
    }
}

impl DatalensLogReader for DatalensHttpClient {
    async fn latest_block(
        &self,
        chain_id: u64,
        finality_mode: FinalityMode,
    ) -> anyhow::Result<u64> {
        let mut builder = self
            .http
            .get(self.chain_head_endpoint(chain_id, finality_mode)?)
            .header("x-datalens-application", &self.config.application);
        if let Some(token) = &self.config.token {
            builder = builder.bearer_auth(token.expose_secret());
        }

        let response = builder.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Datalens chain head query failed with status {status}: {body}");
        }

        let payload: ChainHeadResponse = response.json().await?;
        Ok(payload.height)
    }

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult> {
        let request = native_graphql_request(&query)?;
        let mut builder = self
            .http
            .post(self.native_graphql_endpoint())
            .header("x-datalens-application", &self.config.application)
            .json(&request);
        if let Some(token) = &self.config.token {
            builder = builder.bearer_auth(token.expose_secret());
        }

        let response = builder.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Datalens log query failed with status {status}: {body}");
        }
        let payload: serde_json::Value = response.json().await?;
        if let Some(errors) = payload.get("errors") {
            anyhow::bail!("Datalens log query returned errors: {errors}");
        }

        let logs = logs_from_native_query_payload(&payload, query.chain_id)?;
        Ok(DatalensLogQueryResult { logs })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
struct ChainHeadResponse {
    height: u64,
}

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

fn native_log_rows(rows: &serde_json::Value) -> anyhow::Result<Vec<NativeLogRow>> {
    if rows.is_array() {
        return Ok(serde_json::from_value(rows.clone())?);
    }

    if let Some(value) = rows.pointer("/rows/rows") {
        return Ok(serde_json::from_value(value.clone())?);
    }

    if let Some(value) = rows.pointer("/rows") {
        return Ok(serde_json::from_value(value.clone())?);
    }

    anyhow::bail!("Datalens native query response missing evm log rows")
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
struct NativeLogRow {
    #[serde(default)]
    id: Option<String>,
    block_number: u64,
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
    if rows.is_array() {
        return Ok(serde_json::from_value(rows.clone())?);
    }

    if let Some(value) = rows.pointer("/rows/rows") {
        return Ok(serde_json::from_value(value.clone())?);
    }

    if let Some(value) = rows.pointer("/rows") {
        return Ok(serde_json::from_value(value.clone())?);
    }

    anyhow::bail!("Datalens native query response missing Tron event rows")
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
        "fingerprint": format!("tron-events/{}", digest_prefix(&canonical_key, 12)),
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

fn chain_head_finality(finality_mode: FinalityMode) -> &'static str {
    match finality_mode {
        FinalityMode::Finalized => "finalized",
        FinalityMode::Durable => "finalized",
    }
}

fn chain_name(chain_id: u64) -> anyhow::Result<&'static str> {
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
