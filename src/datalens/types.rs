use serde::{Deserialize, Serialize};

use crate::config::FinalityMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteReason {
    Pending,
    ProviderError,
    BudgetExhausted,
    NotFollowedYet,
}

impl IncompleteReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ProviderError => "provider_error",
            Self::BudgetExhausted => "budget_exhausted",
            Self::NotFollowedYet => "not_followed_yet",
        }
    }

    pub(crate) fn retries_immediately(self) -> bool {
        matches!(self, Self::Pending | Self::ProviderError)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensBlockQuery {
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: u64,
    pub finality_mode: FinalityMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatalensBlock {
    #[serde(alias = "chain_id")]
    pub chain_id: u64,
    #[serde(alias = "block_number", alias = "number")]
    pub block_number: u64,
    #[serde(alias = "block_hash", alias = "hash")]
    pub block_hash: String,
    #[serde(default, alias = "parent_hash")]
    pub parent_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensBlockQueryResult {
    pub blocks: Vec<DatalensBlock>,
}

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
    #[serde(default, alias = "parent_hash")]
    pub parent_hash: Option<String>,
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

/// Logs inside the contiguous served prefix, plus why the request stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensLogQueryResult {
    pub logs: Vec<DatalensLog>,
    /// Inclusive start of `served`. `None` when the server served nothing.
    pub served_start: Option<u64>,
    /// Inclusive end of the served prefix. `None` means the server served nothing.
    pub served_end: Option<u64>,
    pub incomplete_reason: Option<IncompleteReason>,
}

impl DatalensLogQueryResult {
    pub fn covers(&self, to_block: u64) -> bool {
        self.incomplete_reason.is_none() && self.served_end.is_some_and(|end| end >= to_block)
    }
}

#[allow(async_fn_in_trait)]
pub trait DatalensLogReader {
    async fn latest_block(&self, chain_id: u64, finality_mode: FinalityMode)
    -> anyhow::Result<u64>;

    async fn finalized_head(&self, chain_id: u64) -> anyhow::Result<u64> {
        let _ = chain_id;
        Ok(0)
    }

    async fn query_blocks(
        &self,
        _query: DatalensBlockQuery,
    ) -> anyhow::Result<DatalensBlockQueryResult> {
        anyhow::bail!("Datalens block query is not implemented")
    }

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult>;
}
