use serde::{Deserialize, Serialize};

use crate::config::FinalityMode;

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
    #[serde(default, alias = "block_hash")]
    pub block_hash: Option<String>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensTransactionQuery {
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: u64,
    pub finality_mode: FinalityMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct DatalensTransaction {
    #[serde(alias = "transaction_hash")]
    pub hash: String,
    #[serde(alias = "blockNumber")]
    pub block_number: u64,
    #[serde(alias = "transaction_from")]
    pub from: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensTransactionQueryResult {
    pub transactions: Vec<DatalensTransaction>,
}

#[allow(async_fn_in_trait)]
pub trait DatalensLogReader {
    async fn latest_block(&self, chain_id: u64, finality_mode: FinalityMode)
    -> anyhow::Result<u64>;

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult>;

    async fn query_transactions(
        &self,
        _query: DatalensTransactionQuery,
    ) -> anyhow::Result<DatalensTransactionQueryResult> {
        Ok(DatalensTransactionQueryResult {
            transactions: Vec::new(),
        })
    }
}
