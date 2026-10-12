use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    routing::any,
};
use ormpindexer::{
    config::{DatalensConfig, FinalityMode, SecretString},
    datalens::{
        DatalensBlockQuery, DatalensHttpClient, DatalensLogQuery, DatalensLogReader,
        IncompleteReason,
    },
};
use serde_json::{Value, json};

#[derive(Clone)]
struct Mock(Arc<MockInner>);

struct MockInner {
    calls: Mutex<Vec<Recorded>>,
    script: Mutex<VecDeque<(StatusCode, Value)>>,
}

struct Recorded {
    path: String,
    headers: HeaderMap,
    body: Value,
}

impl Mock {
    fn new(script: Vec<(StatusCode, Value)>) -> Self {
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

async fn handle(State(mock): State<Mock>, request: Request) -> (StatusCode, Json<Value>) {
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
    mock.0.calls.lock().expect("calls").push(Recorded {
        path,
        headers,
        body,
    });
    let (status, body) = mock
        .0
        .script
        .lock()
        .expect("script")
        .pop_front()
        .expect("scripted response");
    (status, Json(body))
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

fn log_query() -> DatalensLogQuery {
    DatalensLogQuery {
        chain_id: 46,
        from_block: 10,
        to_block: 14,
        contracts: vec!["0x333".to_owned()],
        topics: vec!["0xaaa".to_owned(), "0xbbb".to_owned()],
        finality_mode: FinalityMode::Finalized,
    }
}

fn expected_log_body() -> Value {
    json!({
        "chain": "darwinia",
        "dataset": "evm.logs",
        "selector": {
            "addresses": ["0x333"],
            "topics": [["0xaaa", "0xbbb"]]
        },
        "range": {"start": 10, "end": 14},
        "finality": "durable_only",
        "fields": ["block_timestamp", "transaction_from"]
    })
}

fn log_row(block_number: u64, log_index: u32) -> Value {
    json!({
        "block_number": block_number,
        "block_hash": "0xblock",
        "block_timestamp": 1_700_000_000,
        "transaction_hash": "0xtx",
        "transaction_index": 1,
        "log_index": log_index,
        "address": "0x333",
        "topics": ["0xaaa"],
        "data": "0x",
        "finality": "finalized",
        "transaction_from": "0xsender"
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
        (StatusCode::OK, heads()),
        (StatusCode::OK, heads()),
        (StatusCode::OK, heads()),
        (
            StatusCode::OK,
            query_response(Some(14), None, None, vec![log_row(12, 2)]),
        ),
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
        100
    );
    assert_eq!(
        client
            .latest_block(46, FinalityMode::Safe)
            .await
            .expect("safe head"),
        110
    );
    let logs = client.query_logs(log_query()).await.expect("logs");

    assert_eq!(logs.served_end, Some(14));
    assert_eq!(logs.incomplete_reason, None);
    assert_eq!(logs.logs.len(), 1);
    assert_eq!(logs.logs[0].chain_id, 46);
    assert_eq!(logs.logs[0].block_number, 12);
    assert_eq!(logs.logs[0].block_timestamp, Some(1_700_000_000));
    assert_eq!(logs.logs[0].transaction_from.as_deref(), Some("0xsender"));
    assert_eq!(logs.logs[0].id.as_deref(), Some("46-12-0xtx-2"));
    let calls = mock.calls();
    assert_eq!(calls[0].0, "/v1/heads/darwinia");
    assert_eq!(calls[0].1, Value::Null);
    assert_eq!(calls[3].0, "/v1/query");
    assert_eq!(calls[3].1, expected_log_body());
    assert_auth(&mock.headers(3));
}

#[tokio::test]
async fn test_client_follows_page_cursor_and_keeps_rows() {
    let mock = Mock::new(vec![
        (
            StatusCode::OK,
            query_response(Some(14), None, Some("cursor-1"), vec![log_row(12, 1)]),
        ),
        (
            StatusCode::OK,
            query_response(Some(14), None, None, vec![log_row(13, 4)]),
        ),
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
                "addresses": ["0x333"],
                "topics": [["0xaaa", "0xbbb"]]
            },
            "range": {"start": 10, "end": 14},
            "finality": "durable_only",
            "fields": ["block_timestamp", "transaction_from"],
            "page": {"cursor": "cursor-1"}
        })
    );
}

#[tokio::test]
async fn test_client_retries_incomplete_then_returns_the_served_prefix() {
    let mock = Mock::new(vec![
        (
            StatusCode::OK,
            query_response(
                Some(11),
                Some("not_followed_yet"),
                None,
                vec![log_row(11, 1)],
            ),
        ),
        (
            StatusCode::OK,
            query_response(
                Some(11),
                Some("not_followed_yet"),
                None,
                vec![log_row(11, 1)],
            ),
        ),
    ]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 2));

    let logs = client.query_logs(log_query()).await.expect("partial logs");

    assert_eq!(logs.served_end, Some(11));
    assert_eq!(
        logs.incomplete_reason,
        Some(IncompleteReason::NotFollowedYet)
    );
    assert_eq!(logs.logs.len(), 1);
    assert_eq!(mock.calls().len(), 2);
    assert_eq!(mock.calls()[0].1, expected_log_body());
    assert_eq!(mock.calls()[1].1, expected_log_body());
}

#[tokio::test]
async fn test_client_retries_quota_exceeded_then_reads_the_log_page() {
    let mock = Mock::new(vec![
        (
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error": {"kind": "quota_exceeded", "message": "slow down"}, "retry_after_seconds": 0}),
        ),
        (
            StatusCode::OK,
            query_response(Some(14), None, None, vec![log_row(12, 2)]),
        ),
    ]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 2));

    let logs = client
        .query_logs(log_query())
        .await
        .expect("logs after retry");

    assert_eq!(logs.logs[0].transaction_from.as_deref(), Some("0xsender"));
    assert_eq!(mock.calls().len(), 2);
}

#[tokio::test]
async fn test_client_posts_block_anchor_query_without_a_selector() {
    let mock = Mock::new(vec![(
        StatusCode::OK,
        json!({
            "chain": "arbitrum",
            "dataset": "evm.blocks",
            "requested": {"start": 20, "end": 21},
            "served": {"start": 20, "end": 21},
            "next_start": null,
            "incomplete_reason": null,
            "segments": [],
            "rows": [{
                "number": 20,
                "hash": "0xhash",
                "parent_hash": "0xparent",
                "timestamp": 1_700_000_000,
                "finality": "safe"
            }],
            "page": {"next_cursor": null}
        }),
    )]);
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
    assert_eq!(blocks.blocks[0].block_hash, "0xhash");
    assert_eq!(blocks.blocks[0].parent_hash.as_deref(), Some("0xparent"));
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
    let mock = Mock::new(vec![(
        StatusCode::OK,
        json!({
            "chain": "arbitrum",
            "dataset": "evm.blocks",
            "requested": {"start": 20, "end": 21},
            "served": {"start": 20, "end": 20},
            "next_start": 21,
            "incomplete_reason": "not_followed_yet",
            "segments": [],
            "rows": [],
            "page": {"next_cursor": null}
        }),
    )]);
    let endpoint = serve(mock.clone()).await;
    let client = DatalensHttpClient::new(config(&endpoint, 1));

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
    assert_eq!(mock.calls()[0].1["finality"], json!("latest_only"));
}

#[tokio::test]
async fn test_client_does_not_retry_unauthorized() {
    let mock = Mock::new(vec![(
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
