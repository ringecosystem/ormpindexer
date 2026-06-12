use anyhow::ensure;
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::CheckpointStore,
    config::{ChainConfig, RuntimeConfig},
    datalens::evm_chain_name,
    planner::{EVM_LOGS_DATASET, TRON_CHAIN_ID, chain_dataset},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensWarmupConfig {
    pub enabled: bool,
    pub ensure_on_startup: bool,
    pub required: bool,
    pub chunk_size: u64,
    pub end_block: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatalensWarmupEnsureOutcome {
    Disabled,
    SkippedNonEvm {
        chain_id: u64,
        dataset: String,
    },
    Failed {
        chain_id: u64,
        error: String,
    },
    Submitted {
        chain_id: u64,
        task_id: String,
        created: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DatalensWarmupSubmitRequest {
    pub chain: WarmupChainIdentity,
    pub dataset_key: String,
    pub selector: WarmupSelector,
    pub range_kind: WarmupRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    pub mode: String,
    pub chunk_policy: WarmupChunkPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupChainIdentity {
    pub family: String,
    pub configured_name: String,
    pub network_id: WarmupNetworkId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupNetworkId {
    pub kind: String,
    pub value: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupSelector {
    pub kind: String,
    pub value: WarmupEvmLogsSelector,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupEvmLogsSelector {
    pub addresses: Vec<String>,
    pub topics: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupRangeKind {
    pub kind: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupChunkPolicy {
    pub max_range_len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupSubmitResponse {
    pub task_id: String,
    pub created: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct WarmupSubmitApiResponse {
    pub task_id: WarmupTaskIdResponse,
    pub created: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum WarmupTaskIdResponse {
    String(String),
    Object { task_id: String },
}

impl WarmupSubmitApiResponse {
    pub(crate) fn into_submit_response(self) -> WarmupSubmitResponse {
        WarmupSubmitResponse {
            task_id: self.task_id.into_string(),
            created: self.created,
        }
    }
}

impl WarmupTaskIdResponse {
    fn into_string(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Object { task_id } => task_id,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait DatalensWarmupEnsurer {
    async fn ensure_warmup_task(
        &self,
        request: DatalensWarmupSubmitRequest,
    ) -> anyhow::Result<WarmupSubmitResponse>;
}

pub async fn ensure_startup_warmup<C, E>(
    config: &RuntimeConfig,
    checkpoints: &C,
    ensurer: &E,
) -> anyhow::Result<Vec<DatalensWarmupEnsureOutcome>>
where
    C: CheckpointStore,
    E: DatalensWarmupEnsurer,
{
    if !config.warmup.enabled || !config.warmup.ensure_on_startup {
        log::info!(
            "Datalens follow_query warmup startup ensure disabled enabled={} ensure_on_startup={}",
            config.warmup.enabled,
            config.warmup.ensure_on_startup
        );
        return Ok(vec![DatalensWarmupEnsureOutcome::Disabled]);
    }

    let mut outcomes = Vec::new();
    for chain in &config.enabled_chains {
        let dataset = chain_dataset(chain.chain_id)?;
        if chain.chain_id == TRON_CHAIN_ID || dataset != EVM_LOGS_DATASET {
            log::info!(
                "skipping Datalens follow_query warmup for non-EVM chain_id={} dataset={}",
                chain.chain_id,
                dataset
            );
            outcomes.push(DatalensWarmupEnsureOutcome::SkippedNonEvm {
                chain_id: chain.chain_id,
                dataset: dataset.to_owned(),
            });
            continue;
        }

        let checkpoint = checkpoints
            .read_or_create(chain.chain_id, dataset, chain.start_block)
            .await?;
        let request = evm_warmup_request(config, chain, checkpoint.next_block)?;
        match ensurer.ensure_warmup_task(request).await {
            Ok(response) => {
                log::info!(
                    "Datalens follow_query warmup task ensured chain_id={} dataset={} checkpoint_next_block={} task_id={} created={}",
                    chain.chain_id,
                    dataset,
                    checkpoint.next_block,
                    response.task_id,
                    response.created
                );
                outcomes.push(DatalensWarmupEnsureOutcome::Submitted {
                    chain_id: chain.chain_id,
                    task_id: response.task_id,
                    created: response.created,
                });
            }
            Err(error) if config.warmup.required => {
                log::warn!(
                    "Datalens follow_query warmup startup ensure failed; failing startup chain_id={} dataset={} required=true error={}",
                    chain.chain_id,
                    dataset,
                    error
                );
                return Err(error);
            }
            Err(error) => {
                log::warn!(
                    "Datalens follow_query warmup startup ensure failed; continuing indexing chain_id={} dataset={} required=false error={}",
                    chain.chain_id,
                    dataset,
                    error
                );
                outcomes.push(DatalensWarmupEnsureOutcome::Failed {
                    chain_id: chain.chain_id,
                    error: error.to_string(),
                });
            }
        }
    }

    Ok(outcomes)
}

pub fn evm_warmup_request(
    config: &RuntimeConfig,
    chain: &ChainConfig,
    start_block: u64,
) -> anyhow::Result<DatalensWarmupSubmitRequest> {
    ensure!(
        !chain.contracts.is_empty(),
        "Datalens warmup selector requires at least one EVM contract address"
    );
    ensure!(
        !chain.topics.is_empty(),
        "Datalens warmup selector requires at least one EVM event topic"
    );

    Ok(DatalensWarmupSubmitRequest {
        chain: WarmupChainIdentity {
            family: "Evm".to_owned(),
            configured_name: evm_chain_name(chain.chain_id)?.to_owned(),
            network_id: WarmupNetworkId {
                kind: "numeric".to_owned(),
                value: chain.chain_id,
            },
        },
        dataset_key: EVM_LOGS_DATASET.to_owned(),
        selector: WarmupSelector {
            kind: "evm_logs".to_owned(),
            value: WarmupEvmLogsSelector {
                addresses: chain.contracts.clone(),
                topics: vec![chain.topics.clone()],
            },
        },
        range_kind: WarmupRangeKind {
            kind: "block".to_owned(),
        },
        start: start_block,
        end: config.warmup.end_block,
        mode: "follow_query".to_owned(),
        chunk_policy: WarmupChunkPolicy {
            max_range_len: config.warmup.chunk_size,
        },
    })
}
