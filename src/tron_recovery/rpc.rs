use std::time::Duration;

use anyhow::{Context, bail, ensure};
use reqwest::{
    Client, Url,
    header::{HeaderMap, HeaderValue},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) const CHAIN_ID: u64 = 728_126_428;
pub(super) const ORMP: &str = "0x5c5c383febe62f377f8c0ea1de97f2a2ba102e98";
pub(super) const MSGPORT: &str = "0x3bc5362ec3a3dbc07292aed4ef18be18de02da3a";

#[derive(Clone)]
pub(super) struct TronRpc {
    http: Client,
    base: Url,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub log_index: String,
    #[serde(default)]
    pub removed: bool,
}

pub(super) fn quantity(value: &Value, field: &str) -> anyhow::Result<u64> {
    let text = value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("RPC missing {field}"))?;
    hex_u64(text)
}

pub(super) fn hex_u64(value: &str) -> anyhow::Result<u64> {
    let digits = value
        .strip_prefix("0x")
        .context("RPC quantity must have 0x prefix")?;
    u64::from_str_radix(digits, 16).context("invalid RPC quantity")
}

pub(super) fn text<'a>(value: &'a Value, field: &str) -> anyhow::Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("RPC missing {field}"))
}

impl TronRpc {
    pub fn new(endpoint: &str, api_key: Option<&str>) -> anyhow::Result<Self> {
        let mut base = Url::parse(endpoint).map_err(|_| anyhow::anyhow!("invalid Tron RPC URL"))?;
        ensure!(
            matches!(base.scheme(), "http" | "https"),
            "Tron RPC requires HTTP(S)"
        );
        let path = base
            .path()
            .trim_end_matches('/')
            .trim_end_matches("/jsonrpc")
            .to_owned();
        base.set_path(&path);
        let mut headers = HeaderMap::new();
        if let Some(key) = api_key.filter(|key| !key.is_empty()) {
            let mut value = HeaderValue::from_str(key).context("invalid Tron API key header")?;
            value.set_sensitive(true);
            headers.insert("TRON-PRO-API-KEY", value);
        }
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(headers)
            .build()?;
        Ok(Self { http, base })
    }

    async fn post(&self, suffix: &str, body: Value) -> anyhow::Result<Value> {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}{suffix}",
            self.base.path().trim_end_matches('/')
        ));
        for attempt in 0..4 {
            let response = self.http.post(url.clone()).json(&body).send().await;
            match response {
                Ok(response) if response.status().is_success() => {
                    return response
                        .json()
                        .await
                        .map_err(|error| error.without_url())
                        .context("invalid Tron RPC response");
                }
                Ok(response) => {
                    let status = response.status();
                    if attempt == 3 || !(status.as_u16() == 429 || status.is_server_error()) {
                        bail!("Tron RPC HTTP status {status}");
                    }
                }
                Err(error) if attempt == 3 => {
                    return Err(error.without_url()).context("Tron RPC request failed");
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
        }
        unreachable!()
    }

    pub async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let body = self
            .post(
                "/jsonrpc",
                json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}),
            )
            .await?;
        if body.get("error").is_some_and(|error| !error.is_null()) {
            // Upstream error messages can include credential-bearing URLs.
            bail!(
                "Tron RPC {method} failed (code {})",
                body.pointer("/error/code").unwrap_or(&Value::Null)
            );
        }
        let result = body
            .get("result")
            .filter(|result| !result.is_null())
            .context("Tron RPC result missing")?;
        Ok(result.clone())
    }

    pub async fn ensure_mainnet(&self) -> anyhow::Result<()> {
        let chain = self.call("eth_chainId", json!([])).await?;
        let chain = chain.as_str().context("Tron chain ID missing")?;
        ensure!(
            hex_u64(chain)? == CHAIN_ID,
            "Tron RPC must use mainnet chain {CHAIN_ID}"
        );
        Ok(())
    }

    pub async fn confirmed_head(&self) -> anyhow::Result<u64> {
        let head = self.post("/walletsolidity/getnowblock", json!({})).await?;
        head.pointer("/block_header/raw_data/number")
            .and_then(Value::as_u64)
            .context("confirmed Tron head missing")
    }

    pub async fn logs(&self, start: u64, end: u64) -> anyhow::Result<Vec<RpcLog>> {
        use crate::planner::{
            MSGPORT_MESSAGE_SENT_TOPIC, ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC,
        };
        let result = self.call("eth_getLogs", json!([{
            "fromBlock":format!("0x{start:x}"), "toBlock":format!("0x{end:x}"),
            "address":[ORMP, MSGPORT],
            "topics":[[ORMP_MESSAGE_ACCEPTED_TOPIC, ORMP_MESSAGE_ASSIGNED_TOPIC, MSGPORT_MESSAGE_SENT_TOPIC]]
        }])).await?;
        let logs: Vec<RpcLog> = serde_json::from_value(result).context("invalid Tron log array")?;
        ensure!(
            logs.len() < 10_000,
            "Tron log result may be truncated; shrink range"
        );
        Ok(logs)
    }
}
