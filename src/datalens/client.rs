use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use reqwest::RequestBuilder;
use serde_json::Value;
use tokio::time::sleep;

use crate::{
    config::{DatalensConfig, FinalityMode},
    datalens::{
        query::{
            QueryPage, WireHeads, block_query_body, blocks_from_page, head_height, log_query_body,
            logs_from_page, parse_query_page,
        },
        retry::{datalens_retry_delay, http_retry_after, is_retryable_http_status},
        types::{
            DatalensBlockQuery, DatalensBlockQueryResult, DatalensLogQuery, DatalensLogQueryResult,
            DatalensLogReader, IncompleteReason,
        },
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

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.config.endpoint.trim_end_matches('/'))
    }

    fn authorized(&self, builder: RequestBuilder) -> RequestBuilder {
        let builder = builder.header("x-datalens-application", &self.config.application);
        match &self.config.token {
            Some(token) => builder.bearer_auth(token.expose_secret()),
            None => builder,
        }
    }

    async fn post_query(&self, request: &Value) -> anyhow::Result<QueryPage> {
        let mut best: Option<QueryPage> = None;
        let max_attempts = self.config.query_max_attempts;
        for attempt in 1..=max_attempts {
            match self.query_pages(request).await {
                Ok(page) if page.incomplete_reason.is_none() => return Ok(page),
                Ok(page) => {
                    let improved = best
                        .as_ref()
                        .is_none_or(|current| served_rank(&page) > served_rank(current));
                    if improved {
                        best = Some(page);
                    }
                    if attempt == max_attempts {
                        break;
                    }
                    let delay = datalens_retry_delay(attempt);
                    log::warn!(
                        "Datalens query incomplete attempt={attempt} max_attempts={max_attempts} retry_delay_ms={}",
                        delay.as_millis()
                    );
                    sleep(delay).await;
                }
                Err(error) if error.retryable && attempt < max_attempts => {
                    let delay = error.delay.unwrap_or_else(|| datalens_retry_delay(attempt));
                    log::warn!(
                        "Datalens query failed attempt={attempt} max_attempts={max_attempts} retry_delay_ms={} error={}",
                        delay.as_millis(),
                        error.detail
                    );
                    sleep(delay).await;
                }
                Err(error) => {
                    if let Some(page) = best {
                        return Ok(page);
                    }
                    anyhow::bail!(error.detail);
                }
            }
        }
        best.context("Datalens query produced no response")
    }

    async fn query_pages(&self, request: &Value) -> Result<QueryPage, CallError> {
        let mut body = request.clone();
        let mut rows = Vec::new();
        let mut seen_cursors = HashSet::new();
        loop {
            let text = self
                .send_once(
                    "Datalens query",
                    self.authorized(self.http.post(self.url("/v1/query")).json(&body)),
                )
                .await?;
            let mut page = parse_query_page(&text).map_err(CallError::fatal)?;
            rows.append(&mut page.rows);
            let Some(cursor) = page.next_cursor.clone() else {
                page.rows = rows;
                return Ok(page);
            };
            if !seen_cursors.insert(cursor.clone()) {
                return Err(CallError::fatal(anyhow::anyhow!(
                    "Datalens query repeated page cursor"
                )));
            }
            body["page"] = serde_json::json!({ "cursor": cursor });
        }
    }

    async fn send_once(
        &self,
        operation: &str,
        builder: RequestBuilder,
    ) -> Result<String, CallError> {
        self.wait_for_request_slot().await;
        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                return Err(CallError::retryable(
                    None,
                    format!("{operation} send failed: {error}"),
                ));
            }
        };
        let status = response.status();
        let body = match response.text().await {
            Ok(body) => body,
            Err(error) => {
                return Err(CallError::retryable(
                    None,
                    format!("{operation} body read failed: {error}"),
                ));
            }
        };
        if status.is_success() {
            return Ok(body);
        }
        let detail = format!("{operation} failed with status {status}: {body}");
        if is_retryable_http_status(status) {
            Err(CallError::retryable(http_retry_after(&body), detail))
        } else {
            Err(CallError::fatal(anyhow::anyhow!(detail)))
        }
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

struct CallError {
    retryable: bool,
    delay: Option<Duration>,
    detail: String,
}

impl CallError {
    fn fatal(error: anyhow::Error) -> Self {
        Self {
            retryable: false,
            delay: None,
            detail: format!("{error:#}"),
        }
    }

    fn retryable(delay: Option<Duration>, detail: String) -> Self {
        Self {
            retryable: true,
            delay,
            detail,
        }
    }
}

fn served_rank(page: &QueryPage) -> u64 {
    page.served_end
        .map(|end| end.saturating_add(1))
        .unwrap_or(0)
}

impl DatalensLogReader for DatalensHttpClient {
    async fn latest_block(
        &self,
        chain_id: u64,
        finality_mode: FinalityMode,
    ) -> anyhow::Result<u64> {
        let chain_name = crate::datalens::evm_chain_name(chain_id)?;
        let url = self.url(&format!("/v1/heads/{chain_name}"));
        let max_attempts = self.config.query_max_attempts;
        for attempt in 1..=max_attempts {
            match self
                .send_once(
                    "Datalens chain head query",
                    self.authorized(self.http.get(&url)),
                )
                .await
            {
                Ok(body) => {
                    let heads: WireHeads =
                        serde_json::from_str(&body).context("decode Datalens chain heads")?;
                    return Ok(head_height(finality_mode, &heads));
                }
                Err(error) if error.retryable && attempt < max_attempts => {
                    let delay = error.delay.unwrap_or_else(|| datalens_retry_delay(attempt));
                    log::warn!(
                        "Datalens chain head query failed attempt={attempt} max_attempts={max_attempts} retry_delay_ms={} error={}",
                        delay.as_millis(),
                        error.detail
                    );
                    sleep(delay).await;
                }
                Err(error) => anyhow::bail!(error.detail),
            }
        }
        unreachable!("query_max_attempts is validated as greater than zero")
    }

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult> {
        let request = log_query_body(&query)?;
        let page = self.post_query(&request).await?;
        let logs = logs_from_page(&page, query.chain_id)?;
        Ok(DatalensLogQueryResult {
            logs,
            served_end: page.served_end,
            incomplete_reason: page.incomplete_reason,
        })
    }

    async fn query_blocks(
        &self,
        query: DatalensBlockQuery,
    ) -> anyhow::Result<DatalensBlockQueryResult> {
        let request = block_query_body(&query)?;
        let page = self.post_query(&request).await?;
        let served_end = page.served_end;
        let covers = page.incomplete_reason.is_none()
            && page
                .served_start
                .is_some_and(|start| start <= query.from_block)
            && served_end.is_some_and(|end| end >= query.to_block);
        if !covers {
            anyhow::bail!(
                "Datalens block query incomplete chain_id={} from_block={} to_block={} served_end={served_end:?} reason={}",
                query.chain_id,
                query.from_block,
                query.to_block,
                page.incomplete_reason
                    .map(IncompleteReason::as_str)
                    .unwrap_or("none")
            );
        }
        let blocks = blocks_from_page(&page, query.chain_id)?;
        Ok(DatalensBlockQueryResult { blocks })
    }
}
