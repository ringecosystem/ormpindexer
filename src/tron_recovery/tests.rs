use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    command::{Args, execute},
    rpc::TronRpc,
    storage::RecoveryStore,
};

const BLOCK: u64 = 87_383_928;
const FIXTURE: &str = include_str!("../../tests/fixtures/tron_recovery/transaction.json");

#[derive(Clone, Default)]
struct RpcState {
    fail_logs: Arc<AtomicBool>,
    malformed: Arc<AtomicBool>,
    small_ranges: Arc<AtomicBool>,
    wrong_metadata: Arc<AtomicBool>,
    wrong_chain: Arc<AtomicBool>,
    max_end: Arc<AtomicU64>,
    requests: Arc<AtomicU64>,
}

async fn rpc_handler(State(state): State<RpcState>, Json(body): Json<Value>) -> Json<Value> {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let result = match body["method"].as_str().unwrap() {
        "eth_chainId" => {
            if state.wrong_chain.load(Ordering::Relaxed) {
                json!("0x1")
            } else {
                json!(format!("0x{:x}", super::rpc::CHAIN_ID))
            }
        }
        "eth_getLogs" => {
            state.requests.fetch_add(1, Ordering::Relaxed);
            if state.fail_logs.load(Ordering::Relaxed) {
                return Json(json!({"error":{"code":-32000,"message":"range unavailable"}}));
            }
            let start =
                super::rpc::hex_u64(body["params"][0]["fromBlock"].as_str().unwrap()).unwrap();
            let end = super::rpc::hex_u64(body["params"][0]["toBlock"].as_str().unwrap()).unwrap();
            if state.small_ranges.load(Ordering::Relaxed) && end - start > 2 {
                return Json(json!({"error":{"code":-32000,"message":"range too large"}}));
            }
            state.max_end.fetch_max(end, Ordering::Relaxed);
            if !(start..=end).contains(&BLOCK) {
                json!([])
            } else {
                let mut logs = fixture["logs"].clone();
                if state.malformed.load(Ordering::Relaxed) {
                    logs[1]["data"] = json!("0xinvalid");
                }
                logs
            }
        }
        "eth_getBlockByNumber" => fixture["block"].clone(),
        "eth_getTransactionByHash" => {
            let mut transaction = fixture["transaction"].clone();
            if state.wrong_metadata.load(Ordering::Relaxed) {
                transaction["blockNumber"] = json!("0x0");
            }
            transaction
        }
        method => panic!("unexpected method {method}"),
    };
    Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
}

async fn mock_rpc(state: RpcState) -> (TronRpc, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/jsonrpc", post(rpc_handler))
        .route(
            "/walletsolidity/getnowblock",
            post(|| async { Json(json!({"block_header":{"raw_data":{"number":BLOCK+2}}})) }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (TronRpc::new(&endpoint, None).unwrap(), server)
}

fn args(dry_run: bool) -> Args {
    Args {
        from_block: BLOCK - 100,
        to_block: None,
        batch_size: 500,
        dry_run,
    }
}

#[tokio::test]
async fn dry_run_decodes_real_transaction_and_stops_at_confirmed_head() {
    let state = RpcState::default();
    let (rpc, server) = mock_rpc(state.clone()).await;
    execute(args(true), rpc, None).await.unwrap();
    assert_eq!(state.max_end.load(Ordering::Relaxed), BLOCK + 2);
    assert_eq!(state.requests.load(Ordering::Relaxed), 1);
    server.abort();
}

#[tokio::test]
async fn rejects_unconfirmed_end_and_malformed_events() {
    let state = RpcState::default();
    let (rpc, server) = mock_rpc(state.clone()).await;
    let mut options = args(true);
    options.to_block = Some(BLOCK + 3);
    assert!(
        execute(options, rpc.clone(), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("confirmed")
    );
    assert_eq!(state.requests.load(Ordering::Relaxed), 0);
    state.malformed.store(true, Ordering::Relaxed);
    assert!(execute(args(true), rpc, None).await.is_err());
    server.abort();
}

#[tokio::test]
async fn shrinks_provider_ranges_and_rejects_inconsistent_transaction_metadata() {
    let state = RpcState::default();
    state.small_ranges.store(true, Ordering::Relaxed);
    let (rpc, server) = mock_rpc(state.clone()).await;
    execute(args(true), rpc.clone(), None).await.unwrap();
    assert!(state.requests.load(Ordering::Relaxed) > 1);
    state.wrong_metadata.store(true, Ordering::Relaxed);
    assert!(execute(args(true), rpc, None).await.is_err());
    server.abort();
}

#[tokio::test]
async fn database_replay_resume_lock_conflict_and_rollback() {
    let Ok(url) = std::env::var("ORMPINDEXER_RECOVERY_TEST_DATABASE_URL") else {
        eprintln!(
            "skipping isolated recovery DB test; ORMPINDEXER_RECOVERY_TEST_DATABASE_URL is unset"
        );
        return;
    };
    let pool = PgPool::connect(&url).await.unwrap();
    crate::database::apply_migrations(&pool).await.unwrap();
    // The opt-in URL must point to a disposable database dedicated to this test.
    sqlx::query("TRUNCATE ormp_message_accepted, ormp_message_assigned, msgport_message_sent, ormp_indexer_checkpoint, ormp_hash_imported").execute(&pool).await.unwrap();
    let state = RpcState::default();
    let (rpc, server) = mock_rpc(state.clone()).await;

    // A wrong-network RPC must never initialize or advance the mainnet checkpoint.
    state.wrong_chain.store(true, Ordering::Relaxed);
    assert!(
        execute(args(false), rpc.clone(), Some(pool.clone()))
            .await
            .is_err()
    );
    let checkpoints: i64 = sqlx::query_scalar("SELECT count(*) FROM ormp_indexer_checkpoint")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(checkpoints, 0);
    state.wrong_chain.store(false, Ordering::Relaxed);

    // Malformed and unavailable data must leave the first block pending.
    state.malformed.store(true, Ordering::Relaxed);
    assert!(
        execute(args(false), rpc.clone(), Some(pool.clone()))
            .await
            .is_err()
    );
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    assert_eq!(count(&pool).await, 0);
    state.malformed.store(false, Ordering::Relaxed);
    state.fail_logs.store(true, Ordering::Relaxed);
    assert!(
        execute(args(false), rpc.clone(), Some(pool.clone()))
            .await
            .is_err()
    );
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    state.fail_logs.store(false, Ordering::Relaxed);

    // Simulate a mid-write database failure. All earlier rows and progress roll back.
    sqlx::query("ALTER TABLE msgport_message_sent ADD CONSTRAINT recovery_fail CHECK (chain_id <> 728126428)").execute(&pool).await.unwrap();
    assert!(
        execute(args(false), rpc.clone(), Some(pool.clone()))
            .await
            .is_err()
    );
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    assert_eq!(count(&pool).await, 0);
    sqlx::query("ALTER TABLE msgport_message_sent DROP CONSTRAINT recovery_fail")
        .execute(&pool)
        .await
        .unwrap();

    execute(args(false), rpc.clone(), Some(pool.clone()))
        .await
        .unwrap();
    assert_eq!(checkpoint(&pool).await, BLOCK + 3);
    assert_eq!(count(&pool).await, 1);
    let accepted: (String, String, String, bool, bool, String) = sqlx::query_as(
        "SELECT \"index\"::TEXT, from_chain_id::TEXT, to_chain_id::TEXT, oracle_assigned, relayer_assigned, block_timestamp::TEXT FROM ormp_message_accepted",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        accepted,
        (
            "10".into(),
            "728126428".into(),
            "46".into(),
            true,
            true,
            "1792824963000".into()
        )
    );
    let sent: (String, String) =
        sqlx::query_as("SELECT msg_id, transaction_from FROM msgport_message_sent")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        sent.0,
        "0xa428da57230ca61d90e25487dd98320036e21b15759cf0cbe726960e1ccd7353"
    );
    assert_eq!(sent.1, "0x223b34a39c5f1cddd76e02ab433f6624b565abbc");

    // Preserve an assignment ID produced by the retired indexer during replay.
    sqlx::query("UPDATE ormp_message_assigned SET id = 'legacy-tron-assignment'")
        .execute(&pool)
        .await
        .unwrap();

    // Already-caught-up runs perform no getLogs. An explicit replay remains idempotent.
    let requests = state.requests.load(Ordering::Relaxed);
    execute(args(false), rpc.clone(), Some(pool.clone()))
        .await
        .unwrap();
    assert_eq!(state.requests.load(Ordering::Relaxed), requests);
    sqlx::query("UPDATE ormp_indexer_checkpoint SET next_block = 87383828")
        .execute(&pool)
        .await
        .unwrap();
    execute(args(false), rpc.clone(), Some(pool.clone()))
        .await
        .unwrap();
    assert_eq!(count(&pool).await, 1);
    let assignments: i64 = sqlx::query_scalar("SELECT count(*) FROM ormp_message_assigned")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(assignments, 1);
    let id: String = sqlx::query_scalar("SELECT id FROM ormp_message_assigned")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id, "legacy-tron-assignment");

    // Dry-run ignores an available database and never advances saved progress.
    sqlx::query("UPDATE ormp_indexer_checkpoint SET next_block = 87383828")
        .execute(&pool)
        .await
        .unwrap();
    execute(args(true), rpc.clone(), Some(pool.clone()))
        .await
        .unwrap();
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    let held = acquire(&pool).await;
    execute(args(false), rpc.clone(), Some(pool.clone()))
        .await
        .unwrap();
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    drop(held);

    // Refuse to move progress if a previously imported source root is absent.
    sqlx::query("INSERT INTO ormp_hash_imported VALUES ('missing-root', 1, '0xtx', 1, 46, 728126428, 46, 'oracle', 'channel', 9, 'missing-root')")
        .execute(&pool).await.unwrap();
    assert!(
        execute(args(false), rpc.clone(), Some(pool.clone()))
            .await
            .is_err()
    );
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    sqlx::query("DELETE FROM ormp_hash_imported WHERE id = 'missing-root'")
        .execute(&pool)
        .await
        .unwrap();

    // Never overwrite a conflicting existing source message.
    sqlx::query("UPDATE ormp_message_accepted SET encoded = '0xconflict'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(execute(args(false), rpc, Some(pool.clone())).await.is_err());
    assert_eq!(checkpoint(&pool).await, BLOCK - 100);
    server.abort();
}

async fn acquire(pool: &PgPool) -> RecoveryStore {
    // A just-dropped lock connection may still be closing at the PostgreSQL server.
    for _ in 0..100 {
        if let Some(store) = RecoveryStore::lock(pool.clone()).await.unwrap() {
            return store;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("previous lock not released");
}

async fn checkpoint(pool: &PgPool) -> u64 {
    let value: String = sqlx::query_scalar("SELECT next_block::TEXT FROM ormp_indexer_checkpoint WHERE dataset = 'tron.ormp-recovery.v1'").fetch_one(pool).await.unwrap();
    value.parse().unwrap()
}

async fn count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM ormp_message_accepted")
        .fetch_one(pool)
        .await
        .unwrap()
}
