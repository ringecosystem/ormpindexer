use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::Context;
use serde::Deserialize;
use tokio::time::sleep;

use crate::{
    config::{DatalensConfig, FinalityMode},
    datalens::{
        query::{
            blocks_from_native_query_payload, chain_head_finality, chain_name,
            logs_from_native_query_payload, native_graphql_block_request, native_graphql_request,
            native_graphql_transaction_request, transactions_from_native_query_payload,
        },
        retry::{
            body_has_retryable_graphql_errors, datalens_retry_delay, graphql_retry_after,
            http_retry_after, is_retryable_http_status,
        },
        types::{
            DatalensBlockQuery, DatalensBlockQueryResult, DatalensLogQuery, DatalensLogQueryResult,
            DatalensLogReader, DatalensTransactionQuery, DatalensTransactionQueryResult,
        },
    },
    warmup::{
        DatalensWarmupEnsurer, DatalensWarmupSubmitRequest, WarmupSubmitApiResponse,
        WarmupSubmitResponse,
    },
};

#[derive(Clone)]
pub struct DatalensHttpClient {
    config: DatalensConfig,
    http: reqwest::Client,
    request_pacer: Arc<Mutex<Option<Instant>>>,
}

impl DatalensHttpClient {
    pub fn new(config: DatalensConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("build Datalens HTTP client");
        Self {
            config,
            http,
            request_pacer: Arc::new(Mutex::new(None)),
        }
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
            self.wait_for_request_slot().await;
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
                    let retry_delay =
                        graphql_retry_after(&body).unwrap_or_else(|| datalens_retry_delay(attempt));
                    log::warn!(
                        "{} returned retryable response attempt={} max_attempts={} retry_delay_ms={}",
                        operation,
                        attempt,
                        self.config.query_max_attempts,
                        retry_delay.as_millis()
                    );
                    sleep(retry_delay).await;
                    continue;
                }
                return Ok(body);
            }

            if is_retryable_http_status(status) && attempt < self.config.query_max_attempts {
                let retry_delay =
                    http_retry_after(&body).unwrap_or_else(|| datalens_retry_delay(attempt));
                log::warn!(
                    "{} failed with status {} attempt={} max_attempts={} retry_delay_ms={} body={}",
                    operation,
                    status,
                    attempt,
                    self.config.query_max_attempts,
                    retry_delay.as_millis(),
                    body
                );
                sleep(retry_delay).await;
                continue;
            }

            anyhow::bail!("{operation} failed with status {status}: {body}");
        }

        unreachable!("query_max_attempts is validated as greater than zero")
    }

    async fn wait_for_request_slot(&self) {
        let min_interval = self.config.min_request_interval;
        if min_interval.is_zero() {
            return;
        }
        loop {
            let delay = {
                let mut last_request_at = self
                    .request_pacer
                    .lock()
                    .expect("Datalens request pacer lock");
                match *last_request_at {
                    Some(last_started_at) => {
                        let elapsed = last_started_at.elapsed();
                        if elapsed >= min_interval {
                            *last_request_at = Some(Instant::now());
                            None
                        } else {
                            Some(min_interval - elapsed)
                        }
                    }
                    None => {
                        *last_request_at = Some(Instant::now());
                        None
                    }
                }
            };
            let Some(delay) = delay else {
                return;
            };
            sleep(delay).await;
        }
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

    async fn query_blocks(
        &self,
        query: DatalensBlockQuery,
    ) -> anyhow::Result<DatalensBlockQueryResult> {
        let request = native_graphql_block_request(&query)?;
        let endpoint = self.native_graphql_endpoint();
        let body = self
            .request_text_with_retries(
                "Datalens block query",
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
            anyhow::bail!("Datalens block query returned errors: {errors}");
        }

        let blocks = blocks_from_native_query_payload(&payload, query.chain_id)?;
        Ok(DatalensBlockQueryResult { blocks })
    }

    async fn query_transactions(
        &self,
        query: DatalensTransactionQuery,
    ) -> anyhow::Result<DatalensTransactionQueryResult> {
        let request = native_graphql_transaction_request(&query)?;
        let endpoint = self.native_graphql_endpoint();
        let body = self
            .request_text_with_retries(
                "Datalens transaction query",
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
            anyhow::bail!("Datalens transaction query returned errors: {errors}");
        }

        let transactions = transactions_from_native_query_payload(&payload, query.chain_id)?;
        Ok(DatalensTransactionQueryResult { transactions })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
struct ChainHeadResponse {
    height: u64,
}
