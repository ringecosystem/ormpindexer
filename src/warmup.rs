use anyhow::ensure;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    checkpoint::CheckpointStore,
    config::{ChainConfig, RuntimeConfig},
    datalens::{evm_chain_name, tron_chain_name},
    planner::{EVM_LOGS_DATASET, TRON_EVENTS_DATASET, chain_dataset},
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
    pub family: serde_json::Value,
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
    pub value: serde_json::Value,
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
        if dataset != EVM_LOGS_DATASET && dataset != TRON_EVENTS_DATASET {
            log::info!(
                "skipping Datalens follow_query warmup for unsupported dataset chain_id={} dataset={}",
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
        let request = warmup_request(config, chain, dataset, checkpoint.next_block)?;
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

pub fn warmup_request(
    config: &RuntimeConfig,
    chain: &ChainConfig,
    dataset: &str,
    start_block: u64,
) -> anyhow::Result<DatalensWarmupSubmitRequest> {
    match dataset {
        EVM_LOGS_DATASET => evm_warmup_request(config, chain, start_block),
        TRON_EVENTS_DATASET => tron_warmup_request(config, chain, start_block),
        _ => anyhow::bail!("unsupported Datalens warmup dataset: {dataset}"),
    }
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
            family: json!("Evm"),
            configured_name: evm_chain_name(chain.chain_id)?.to_owned(),
            network_id: WarmupNetworkId {
                kind: "numeric".to_owned(),
                value: chain.chain_id,
            },
        },
        dataset_key: EVM_LOGS_DATASET.to_owned(),
        selector: WarmupSelector {
            kind: "evm_logs".to_owned(),
            value: json!(WarmupEvmLogsSelector {
                addresses: chain.contracts.clone(),
                topics: vec![chain.topics.clone()],
            }),
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

pub fn tron_warmup_request(
    config: &RuntimeConfig,
    chain: &ChainConfig,
    start_block: u64,
) -> anyhow::Result<DatalensWarmupSubmitRequest> {
    ensure!(
        !chain.contracts.is_empty(),
        "Datalens warmup selector requires at least one Tron contract address"
    );
    ensure!(
        !chain.topics.is_empty(),
        "Datalens warmup selector requires at least one Tron event name"
    );

    let selector = tron_event_selector(&chain.contracts, &chain.topics)?;
    Ok(DatalensWarmupSubmitRequest {
        chain: WarmupChainIdentity {
            family: json!({ "Other": "tron" }),
            configured_name: tron_chain_name(chain.chain_id)?.to_owned(),
            network_id: WarmupNetworkId {
                kind: "numeric".to_owned(),
                value: chain.chain_id,
            },
        },
        dataset_key: TRON_EVENTS_DATASET.to_owned(),
        selector: WarmupSelector {
            kind: "other".to_owned(),
            value: selector,
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
        "fingerprint": format!("tron-events/ormp-v2/{}", digest_prefix(&canonical_key, 12)),
        "canonical_key": canonical_key,
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
