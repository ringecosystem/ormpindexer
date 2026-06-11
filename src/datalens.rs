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
pub struct DatalensLog {
    #[serde(default)]
    pub id: Option<String>,
    pub chain_id: u64,
    pub block_number: u64,
    #[serde(default)]
    pub block_timestamp: Option<u64>,
    pub transaction_hash: String,
    #[serde(default)]
    pub transaction_index: Option<i32>,
    pub log_index: u64,
    pub address: String,
    #[serde(default)]
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
        let request = json!({
            "query": r#"
                query OrmpIndexerLogs($input: EvmLogsInput!) {
                  evmLogs(input: $input) {
                    id
                    chainId
                    blockNumber
                    blockTimestamp
                    transactionHash
                    transactionIndex
                    logIndex
                    address
                    transactionFrom
                    topics
                    data
                  }
                }
            "#,
            "variables": {
                "input": {
                    "application": self.config.application,
                    "chainId": query.chain_id,
                    "fromBlock": query.from_block,
                    "toBlock": query.to_block,
                    "addresses": query.contracts,
                    "topics": query.topics,
                    "finality": query.finality_mode.as_str(),
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

        let logs = serde_json::from_value(
            payload
                .pointer("/data/evmLogs")
                .cloned()
                .unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
        )?;
        Ok(DatalensLogQueryResult { logs })
    }
}
