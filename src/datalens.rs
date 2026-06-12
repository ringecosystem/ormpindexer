use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::time::sleep;

use anyhow::Context;

use crate::config::{DatalensConfig, FinalityMode};
use crate::planner::TRON_CHAIN_ID;
use crate::warmup::{
    DatalensWarmupEnsurer, DatalensWarmupSubmitRequest, WarmupSubmitApiResponse,
    WarmupSubmitResponse,
};

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
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("build Datalens HTTP client");
        Self { config, http }
    }

    fn native_graphql_endpoint(&self) -> String {
        format!(
            "{}/native/graphql",
            self.config.endpoint.trim_end_matches('/')
        )
    }

    fn warmup_tasks_endpoint(&self) -> String {
        format!(
            "{}/v1/warmup/tasks",
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

    pub async fn submit_warmup_task(
        &self,
        request: DatalensWarmupSubmitRequest,
    ) -> anyhow::Result<WarmupSubmitResponse> {
        let body = self
            .request_text_with_retries(
                "Datalens warmup submit",
                || {
                    let mut builder = self
                        .http
                        .post(self.warmup_tasks_endpoint())
                        .header("x-datalens-application", &self.config.application)
                        .json(&request);
                    if let Some(token) = &self.config.token {
                        builder = builder.bearer_auth(token.expose_secret());
                    }
                    builder
                },
                |_| false,
            )
            .await?;

        let payload: WarmupSubmitApiResponse = serde_json::from_str(&body)?;
        Ok(payload.into_submit_response())
    }

    async fn request_text_with_retries<B, G>(
        &self,
        operation: &str,
        mut build_request: B,
        should_retry_body: G,
    ) -> anyhow::Result<String>
    where
        B: FnMut() -> reqwest::RequestBuilder,
        G: Fn(&str) -> bool,
    {
        for attempt in 1..=self.config.query_max_attempts {
            let response = match build_request().send().await {
                Ok(response) => response,
                Err(error) => {
                    if attempt < self.config.query_max_attempts {
                        log::warn!(
                            "{} send failed attempt={} max_attempts={} error={}",
                            operation,
                            attempt,
                            self.config.query_max_attempts,
                            error
                        );
                        sleep(datalens_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(error).with_context(|| format!("{operation} send failed"));
                }
            };

            let status = response.status();
            let body = match response.text().await {
                Ok(body) => body,
                Err(error) => {
                    if attempt < self.config.query_max_attempts {
                        log::warn!(
                            "{} body read failed attempt={} max_attempts={} error={}",
                            operation,
                            attempt,
                            self.config.query_max_attempts,
                            error
                        );
                        sleep(datalens_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(error).with_context(|| format!("{operation} body read failed"));
                }
            };
            if status.is_success() {
                if should_retry_body(&body) && attempt < self.config.query_max_attempts {
                    log::warn!(
                        "{} returned retryable response attempt={} max_attempts={}",
                        operation,
                        attempt,
                        self.config.query_max_attempts,
                    );
                    sleep(datalens_retry_delay(attempt)).await;
                    continue;
                }
                return Ok(body);
            }

            if is_retryable_http_status(status) && attempt < self.config.query_max_attempts {
                log::warn!(
                    "{} failed with status {} attempt={} max_attempts={} body={}",
                    operation,
                    status,
                    attempt,
                    self.config.query_max_attempts,
                    body
                );
                sleep(datalens_retry_delay(attempt)).await;
                continue;
            }

            anyhow::bail!("{operation} failed with status {status}: {body}");
        }

        unreachable!("query_max_attempts is validated as greater than zero")
    }
}

impl DatalensWarmupEnsurer for DatalensHttpClient {
    async fn ensure_warmup_task(
        &self,
        request: DatalensWarmupSubmitRequest,
    ) -> anyhow::Result<WarmupSubmitResponse> {
        self.submit_warmup_task(request).await
    }
}

impl DatalensLogReader for DatalensHttpClient {
    async fn latest_block(
        &self,
        chain_id: u64,
        finality_mode: FinalityMode,
    ) -> anyhow::Result<u64> {
        let endpoint = self.chain_head_endpoint(chain_id, finality_mode)?;
        let body = self
            .request_text_with_retries(
                "Datalens chain head query",
                || {
                    let mut builder = self
                        .http
                        .get(&endpoint)
                        .header("x-datalens-application", &self.config.application);
                    if let Some(token) = &self.config.token {
                        builder = builder.bearer_auth(token.expose_secret());
                    }
                    builder
                },
                |_| false,
            )
            .await?;

        let payload: ChainHeadResponse = serde_json::from_str(&body)?;
        Ok(payload.height)
    }

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult> {
        let request = native_graphql_request(&query)?;
        let endpoint = self.native_graphql_endpoint();
        let body = self
            .request_text_with_retries(
                "Datalens log query",
                || {
                    let mut builder = self
                        .http
                        .post(&endpoint)
                        .header("x-datalens-application", &self.config.application)
                        .json(&request);
                    if let Some(token) = &self.config.token {
                        builder = builder.bearer_auth(token.expose_secret());
                    }
                    builder
                },
                body_has_retryable_graphql_errors,
            )
            .await?;
        let payload: serde_json::Value = serde_json::from_str(&body)?;
        if let Some(errors) = payload.get("errors") {
            anyhow::bail!("Datalens log query returned errors: {errors}");
        }

        let logs = logs_from_native_query_payload(&payload, query.chain_id)?;
        Ok(DatalensLogQueryResult { logs })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatalensFailureKind {
    ProviderLimit,
    Transient,
    Other,
}

pub fn classify_datalens_failure_message(message: &str) -> DatalensFailureKind {
    let message = message.to_ascii_lowercase();
    if message.contains("query returns too many logs")
        || message.contains("too many logs")
        || message.contains("narrow your filter")
        || message.contains("provider limit")
        || message.contains("range limit")
        || message.contains("block range too large")
        || message.contains("range too large")
    {
        return DatalensFailureKind::ProviderLimit;
    }

    if message.contains("timeout")
        || message.contains("timed out")
        || message.contains("provider_failure")
        || message.contains("providerfailure")
        || message.contains("rate-limit")
        || message.contains("rate limit")
        || message.contains("bad gateway")
        || message.contains("service unavailable")
        || message.contains("gateway timeout")
    {
        return DatalensFailureKind::Transient;
    }

    DatalensFailureKind::Other
}

fn body_has_retryable_graphql_errors(body: &str) -> bool {
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let Some(errors) = payload.get("errors") else {
        return false;
    };
    errors
        .as_array()
        .map(|errors| errors.iter().any(graphql_error_is_retryable))
        .unwrap_or_else(|| graphql_error_is_retryable(errors))
}

fn graphql_error_is_retryable(error: &serde_json::Value) -> bool {
    if let Some(message) = error.get("message").and_then(serde_json::Value::as_str) {
        if matches!(
            classify_datalens_failure_message(message),
            DatalensFailureKind::Transient
        ) {
            return true;
        }
    }

    let Some(extensions) = error.get("extensions") else {
        return false;
    };

    if let Some(code) = extensions.get("code").and_then(serde_json::Value::as_str) {
        if matches!(
            classify_datalens_failure_message(code),
            DatalensFailureKind::Transient
        ) {
            return true;
        }
    }

    ["status", "statusCode", "httpStatus"]
        .iter()
        .filter_map(|key| extensions.get(*key))
        .any(retryable_status_value)
}

fn retryable_status_value(value: &serde_json::Value) -> bool {
    let status = value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()));
    matches!(status, Some(429 | 500..=599))
}

fn datalens_retry_delay(attempt: u64) -> std::time::Duration {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.saturating_sub(1).min(2));
    std::time::Duration::from_millis(millis.min(1_000))
}

fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
        || status.as_u16() == 524
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
    let rows = native_rows(rows).context("Datalens native query response missing evm log rows")?;
    Ok(serde_json::from_value(rows.clone())?)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_body_has_retryable_graphql_errors_checks_structured_status() {
        let body = serde_json::json!({
            "errors": [{
                "message": "upstream provider failed",
                "extensions": {
                    "code": "PROVIDER_FAILURE",
                    "status": 502
                }
            }]
        })
        .to_string();

        assert!(body_has_retryable_graphql_errors(&body));
    }

    #[test]
    fn test_body_has_retryable_graphql_errors_ignores_validation_numbers() {
        let body = serde_json::json!({
            "errors": [{
                "message": "validation failed: limit must be at most 500",
                "extensions": {
                    "code": "BAD_USER_INPUT"
                }
            }]
        })
        .to_string();

        assert!(!body_has_retryable_graphql_errors(&body));
    }
}
