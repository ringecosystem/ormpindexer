use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use ormpindexer::{
    config::{DatalensConfig, FinalityMode, SecretString},
    datalens::{
        DatalensBlockQuery, DatalensHttpClient, DatalensLogQuery, DatalensLogReader,
        IncompleteReason,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone)]
struct Mock(Arc<MockInner>);

struct MockInner {
    calls: Mutex<Vec<Recorded>>,
    script: Mutex<VecDeque<Scripted>>,
}

struct Scripted {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

struct Recorded {
    path: String,
    headers: HeaderMap,
    body: Value,
}

impl Mock {
    fn new(script: Vec<Scripted>) -> Self {
        Self(Arc::new(MockInner {
            calls: Mutex::new(Vec::new()),
            script: Mutex::new(script.into()),
        }))
    }

    fn calls(&self) -> Vec<(String, Value)> {
        self.0
            .calls
            .lock()
            .expect("calls")
            .iter()
            .map(|call| (call.path.clone(), call.body.clone()))
            .collect()
    }

    fn headers(&self, index: usize) -> HeaderMap {
        self.0.calls.lock().expect("calls")[index].headers.clone()
    }
}

async fn handle(State(mock): State<Mock>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("request json")
    };
    let query = if path == "/v1/query" {
        match validate_query(&body) {
            Ok(query) => Some(query),
            Err(message) => {
                mock.0.calls.lock().expect("calls").push(Recorded {
                    path,
                    headers,
                    body,
                });
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": {"kind": "invalid_input", "message": message}
                    })),
                )
                    .into_response();
            }
        }
    } else {
        None
    };
    mock.0.calls.lock().expect("calls").push(Recorded {
        path,
        headers,
        body,
    });
    let scripted = mock
        .0
        .script
        .lock()
        .expect("script")
        .pop_front()
        .expect("scripted response");
    let mut response_body = scripted.body;
    if let Some(query) = query {
        project_rows(&mut response_body, query.fields.as_ref());
    }
    let mut response = (scripted.status, Json(response_body)).into_response();
    response.headers_mut().extend(scripted.headers);
    response
}

fn ok(body: Value) -> Scripted {
    Scripted {
        status: StatusCode::OK,
        headers: HeaderMap::new(),
        body,
    }
}

fn http_status(status: StatusCode, body: Value) -> Scripted {
    Scripted {
        status,
        headers: HeaderMap::new(),
        body,
    }
}

async fn serve(mock: Mock) -> String {
    let app = Router::new().fallback(any(handle)).with_state(mock);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{address}")
}

fn config(endpoint: &str, attempts: u64) -> DatalensConfig {
    DatalensConfig {
        endpoint: endpoint.to_owned(),
        application: "ormp".to_owned(),
        token: Some(SecretString::new("secret-token")),
        timeout: Duration::from_secs(5),
        query_max_attempts: attempts,
        head_buffer_blocks: 1,
        min_request_interval: Duration::ZERO,
    }
}

const ADDRESS: &str = "0x1111111111111111111111111111111111111111";
const SENDER: &str = "0x2222222222222222222222222222222222222222";
const TOPIC_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOPIC_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BLOCK_HASH: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const TX_HASH: &str = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

fn log_fields() -> Vec<&'static str> {
    vec![
        "block_number",
        "block_hash",
        "transaction_hash",
        "transaction_index",
        "log_index",
        "address",
        "topics",
        "data",
        "block_timestamp",
        "transaction_from",
    ]
}

fn log_query() -> DatalensLogQuery {
    DatalensLogQuery {
        chain_id: 46,
        from_block: 10,
        to_block: 14,
        contracts: vec![ADDRESS.to_owned()],
        topics: vec![TOPIC_A.to_owned(), TOPIC_B.to_owned()],
        finality_mode: FinalityMode::Finalized,
    }
}

fn expected_log_body() -> Value {
    json!({
        "chain": "darwinia",
        "dataset": "evm.logs",
        "selector": {
            "addresses": [ADDRESS],
            "topics": [[TOPIC_A, TOPIC_B]]
        },
        "range": {"start": 10, "end": 14},
        "finality": "durable_only",
        "fields": log_fields()
    })
}

fn log_row(block_number: u64, log_index: u32) -> Value {
    json!({
        "block_number": block_number,
        "block_hash": BLOCK_HASH,
        "block_timestamp": 1_700_000_000,
        "transaction_hash": TX_HASH,
        "transaction_index": 1,
        "log_index": log_index,
        "address": ADDRESS,
        "topics": [TOPIC_A],
        "data": "0x",
        "finality": "finalized",
        "transaction_from": SENDER
    })
}

fn query_response(
    served_end: Option<u64>,
    reason: Option<&str>,
    cursor: Option<&str>,
    rows: Vec<Value>,
) -> Value {
    json!({
        "chain": "darwinia",
        "dataset": "evm.logs",
        "requested": {"start": 10, "end": 14},
        "served": served_end.map(|end| json!({"start": 10, "end": end})),
        "next_start": served_end.and_then(|end| (end < 14).then_some(end + 1)),
        "incomplete_reason": reason,
        "segments": [],
        "rows": rows,
        "page": {"next_cursor": cursor}
    })
}

fn heads() -> Value {
    json!({
        "chain": "darwinia",
        "latest": 120,
        "latest_hash": "0x1",
        "safe": 110,
        "finalized": 100
    })
}

fn assert_auth(headers: &HeaderMap) {
    assert_eq!(
        headers
            .get("x-datalens-application")
            .and_then(|value| value.to_str().ok()),
        Some("ormp")
    );
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer secret-token")
    );
}

#[tokio::test]
async fn test_client_posts_log_query_and_reads_finality_from_heads() {
    let mock = Mock::new(vec![
        ok(heads()),
        ok(heads()),
        ok(heads()),
        ok(heads()),
        ok(query_response(Some(14), None, None, vec![log_row(12, 2)])),
    ]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 1));

    assert_eq!(
        client
            .latest_block(46, FinalityMode::Finalized)
            .await
            .expect("finalized head"),
        100
    );
    assert_eq!(
        client
            .latest_block(46, FinalityMode::Durable)
            .await
            .expect("durable head"),
        110
    );
    assert_eq!(
        client
            .latest_block(46, FinalityMode::Safe)
            .await
            .expect("safe head"),
        110
    );
    assert_eq!(
        client
            .latest_block(46, FinalityMode::Latest)
            .await
            .expect("latest head"),
        120
    );
    let logs = client.query_logs(log_query()).await.expect("logs");

    assert_eq!(logs.served_end, Some(14));
    assert_eq!(logs.incomplete_reason, None);
    assert_eq!(logs.logs.len(), 1);
    assert_eq!(logs.logs[0].chain_id, 46);
    assert_eq!(logs.logs[0].block_number, 12);
    assert_eq!(logs.logs[0].block_timestamp, Some(1_700_000_000));
    assert_eq!(logs.logs[0].transaction_from.as_deref(), Some(SENDER));
    assert_eq!(logs.logs[0].block_hash.as_deref(), Some(BLOCK_HASH));
    let log_id = format!("46-12-{TX_HASH}-2");
    assert_eq!(logs.logs[0].id.as_deref(), Some(log_id.as_str()));
    let calls = mock.calls();
    assert_eq!(calls[0].0, "/v1/heads/darwinia");
    assert_eq!(calls[0].1, Value::Null);
    assert_eq!(calls[4].0, "/v1/query");
    assert_eq!(calls[4].1, expected_log_body());
    let request = validate_query(&calls[4].1).expect("query matches the wire schema");
    assert_eq!(
        request.fields.expect("fields"),
        log_fields()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    );
    assert_auth(&mock.headers(4));
}

#[tokio::test]
async fn test_client_follows_page_cursor_and_keeps_rows() {
    let mock = Mock::new(vec![
        ok(query_response(
            Some(14),
            None,
            Some("cursor-1"),
            vec![log_row(12, 1)],
        )),
        ok(query_response(Some(14), None, None, vec![log_row(13, 4)])),
    ]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 1));

    let logs = client.query_logs(log_query()).await.expect("paged logs");

    assert_eq!(
        logs.logs
            .iter()
            .map(|log| log.log_index)
            .collect::<Vec<_>>(),
        vec![1, 4]
    );
    let calls = mock.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].1, expected_log_body());
    assert_eq!(
        calls[1].1,
        json!({
            "chain": "darwinia",
            "dataset": "evm.logs",
            "selector": {
            "addresses": [ADDRESS],
            "topics": [[TOPIC_A, TOPIC_B]]
            },
            "range": {"start": 10, "end": 14},
            "finality": "durable_only",
            "fields": log_fields(),
            "page": {"cursor": "cursor-1"}
        })
    );
}

#[tokio::test]
async fn test_client_retries_pending_then_returns_the_served_prefix() {
    let mock = Mock::new(vec![
        ok(query_response(
            Some(11),
            Some("pending"),
            None,
            vec![log_row(11, 1)],
        )),
        ok(query_response(
            Some(11),
            Some("pending"),
            None,
            vec![log_row(11, 1)],
        )),
    ]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 2));

    let logs = client.query_logs(log_query()).await.expect("partial logs");

    assert_eq!(logs.served_end, Some(11));
    assert_eq!(logs.incomplete_reason, Some(IncompleteReason::Pending));
    assert_eq!(logs.logs.len(), 1);
    assert_eq!(mock.calls().len(), 2);
    assert_eq!(mock.calls()[0].1, expected_log_body());
    assert_eq!(mock.calls()[1].1, expected_log_body());
}

#[tokio::test]
async fn test_client_retries_quota_exceeded_then_reads_the_log_page() {
    let mut limited = http_status(
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error": {"kind": "quota_exceeded", "message": "slow down"}}),
    );
    limited.headers.insert(
        reqwest::header::RETRY_AFTER,
        "0".parse().expect("retry-after"),
    );
    let mock = Mock::new(vec![
        limited,
        ok(query_response(Some(14), None, None, vec![log_row(12, 2)])),
    ]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 2));

    let logs = client
        .query_logs(log_query())
        .await
        .expect("logs after retry");

    assert_eq!(logs.logs[0].transaction_from.as_deref(), Some(SENDER));
    assert_eq!(mock.calls().len(), 2);
}

#[tokio::test]
async fn test_client_posts_block_anchor_query_without_a_selector() {
    let mock = Mock::new(vec![ok(json!({
        "chain": "arbitrum",
        "dataset": "evm.blocks",
        "requested": {"start": 20, "end": 21},
        "served": {"start": 20, "end": 21},
        "next_start": null,
        "incomplete_reason": null,
        "segments": [],
        "rows": [{
            "number": 20,
            "hash": BLOCK_HASH,
            "parent_hash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "timestamp": 1_700_000_000,
            "finality": "safe"
        }],
        "page": {"next_cursor": null}
    }))]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 1));

    let blocks = client
        .query_blocks(DatalensBlockQuery {
            chain_id: 42161,
            from_block: 20,
            to_block: 21,
            finality_mode: FinalityMode::Safe,
        })
        .await
        .expect("blocks");

    assert_eq!(blocks.blocks.len(), 1);
    assert_eq!(blocks.blocks[0].block_number, 20);
    assert_eq!(blocks.blocks[0].block_hash, BLOCK_HASH);
    assert_eq!(
        blocks.blocks[0].parent_hash.as_deref(),
        Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
    );
    assert_eq!(
        mock.calls()[0].1,
        json!({
            "chain": "arbitrum",
            "dataset": "evm.blocks",
            "range": {"start": 20, "end": 21},
            "finality": "safe_to_latest"
        })
    );
}

#[tokio::test]
async fn test_client_refuses_an_incomplete_block_query() {
    let mock = Mock::new(vec![ok(json!({
        "chain": "arbitrum",
        "dataset": "evm.blocks",
        "requested": {"start": 20, "end": 21},
        "served": {"start": 20, "end": 20},
        "next_start": 21,
        "incomplete_reason": "not_followed_yet",
        "segments": [],
        "rows": [],
        "page": {"next_cursor": null}
    }))]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 3));

    let error = client
        .query_blocks(DatalensBlockQuery {
            chain_id: 42161,
            from_block: 20,
            to_block: 21,
            finality_mode: FinalityMode::Latest,
        })
        .await
        .expect_err("incomplete blocks fail");

    assert!(error.to_string().contains("not_followed_yet"));
    assert_eq!(mock.calls()[0].1["finality"], json!("safe_to_latest"));
    assert_eq!(mock.calls().len(), 1);
}

#[tokio::test]
async fn test_client_returns_not_followed_yet_and_budget_exhausted_without_retry() {
    for reason in ["not_followed_yet", "budget_exhausted"] {
        let mock = Mock::new(vec![ok(query_response(
            Some(11),
            Some(reason),
            None,
            vec![log_row(11, 1)],
        ))]);
        let endpoint = serve(mock.clone()).await;
        let client = DatalensHttpClient::new(config(&endpoint, 3));

        let logs = client
            .query_logs(log_query())
            .await
            .expect("incomplete page is returned");

        assert_eq!(logs.served_end, Some(11));
        assert_eq!(
            logs.incomplete_reason.map(|reason| reason.as_str()),
            Some(reason)
        );
        assert_eq!(mock.calls().len(), 1);
    }
}

#[tokio::test]
async fn test_client_does_not_retry_unauthorized() {
    let mock = Mock::new(vec![http_status(
        StatusCode::UNAUTHORIZED,
        json!({"error": {"kind": "unauthorized", "message": "bad token"}}),
    )]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 3));

    let error = client
        .latest_block(1, FinalityMode::Latest)
        .await
        .expect_err("unauthorized");

    assert!(error.to_string().contains("401"));
    assert_eq!(mock.calls().len(), 1);
    assert_eq!(mock.calls()[0].0, "/v1/heads/ethereum");
}

fn project_rows(body: &mut Value, fields: Option<&Vec<String>>) {
    let Some(fields) = fields else {
        return;
    };
    let Some(rows) = body.get_mut("rows").and_then(|rows| rows.as_array_mut()) else {
        return;
    };
    for row in rows {
        if let Some(object) = row.as_object_mut() {
            object.retain(|key, _| fields.iter().any(|field| field == key));
        }
    }
}

fn validate_query(value: &Value) -> Result<QueryRequest, String> {
    let request: QueryRequest =
        serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
    if !is_chain_name(&request.chain) {
        return Err(format!("invalid chain name {}", request.chain));
    }
    if request.dataset != "evm.logs" && request.dataset != "evm.blocks" {
        return Err(format!("unknown dataset {}", request.dataset));
    }
    if request.dataset == "evm.blocks" && request.selector.is_some() {
        return Err("evm.blocks takes no selector".into());
    }
    if let Some(finality) = &request.finality
        && !matches!(
            finality.as_str(),
            "durable_only" | "safe_to_latest" | "latest_only"
        )
    {
        return Err(format!("unknown finality {finality}"));
    }
    if request.range.start > request.range.end {
        return Err(format!(
            "block range start {} is after end {}",
            request.range.start, request.range.end
        ));
    }
    if let Some(selector) = &request.selector {
        if selector.topics.len() > 4 {
            return Err(format!(
                "selector has {} topic positions, at most 4 allowed",
                selector.topics.len()
            ));
        }
        for address in &selector.addresses {
            if !is_hex(&address.0, 20) {
                return Err(format!("invalid address {}", address.0));
            }
        }
        for position in &selector.topics {
            if position.0.is_empty() {
                return Err("topic filter must list at least one topic".into());
            }
        }
    }
    let _ = request
        .page
        .as_ref()
        .map(|page| (page.limit, page.cursor.as_deref()));
    let _ = request.wait_ms;
    Ok(request)
}

fn is_chain_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
}

fn is_hex(value: &str, bytes: usize) -> bool {
    let Some(rest) = value.strip_prefix("0x") else {
        return false;
    };
    rest.len() == bytes * 2 && rest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequest {
    chain: String,
    dataset: String,
    #[serde(default)]
    selector: Option<LogSelector>,
    range: WireRange,
    #[serde(default)]
    finality: Option<String>,
    #[serde(default)]
    fields: Option<Vec<String>>,
    #[serde(default)]
    page: Option<PageRequest>,
    #[serde(default)]
    wait_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageRequest {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRange {
    start: u64,
    end: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSelector {
    #[serde(default)]
    addresses: Vec<Hex20>,
    #[serde(default)]
    topics: Vec<TopicPosition>,
}

#[derive(Debug, Deserialize)]
#[serde(try_from = "String")]
struct Hex20(String);

impl TryFrom<String> for Hex20 {
    type Error = String;

    fn try_from(value: String) -> Result<Self, String> {
        if is_hex(&value, 20) {
            Ok(Self(value))
        } else {
            Err(format!("invalid address {value}"))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(try_from = "Vec<String>")]
struct TopicPosition(Vec<String>);

impl TryFrom<Vec<String>> for TopicPosition {
    type Error = String;

    fn try_from(values: Vec<String>) -> Result<Self, String> {
        if values.is_empty() {
            return Err("topic filter must list at least one topic".into());
        }
        for value in &values {
            if !is_hex(value, 32) {
                return Err(format!("invalid topic {value}"));
            }
        }
        Ok(Self(values))
    }
}
