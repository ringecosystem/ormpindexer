use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::{DatalensConfig, FinalityMode};

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensLogQueryResult {
    pub logs: Vec<DatalensLog>,
}

#[allow(async_fn_in_trait)]
pub trait DatalensLogReader {
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
}

impl DatalensLogReader for DatalensHttpClient {
    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult> {
        let chain_name = evm_chain_name(query.chain_id)?;
        let request = json!({
            "query": r#"
                query OrmpIndexerLogs($input: QueryInput!) {
                  query(input: $input) {
                    rows
                  }
                }
            "#,
            "variables": {
                "input": {
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
                }
            }
        });
        let mut builder = self
            .http
            .post(self.native_graphql_endpoint())
            .json(&request);
        if let Some(token) = &self.config.token {
            builder = builder.bearer_auth(token.expose_secret());
        }

        let response = builder.send().await?.error_for_status()?;
        let payload: serde_json::Value = response.json().await?;
        if let Some(errors) = payload.get("errors") {
            anyhow::bail!("Datalens log query returned errors: {errors}");
        }

        let logs = logs_from_native_query_payload(&payload, query.chain_id)?;
        Ok(DatalensLogQueryResult { logs })
    }
}

pub fn logs_from_native_query_payload(
    payload: &serde_json::Value,
    chain_id: u64,
) -> anyhow::Result<Vec<DatalensLog>> {
    let rows = payload
        .pointer("/data/query/rows")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
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
        })
    }
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
        FinalityMode::Finalized => "finalized",
        FinalityMode::Durable => "durable_only",
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
