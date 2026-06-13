use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use ormpindexer::{
    checkpoint::InMemoryCheckpointStore,
    config::{FinalityMode, RuntimeConfig},
    warmup::{
        DatalensWarmupEnsureOutcome, DatalensWarmupEnsurer, DatalensWarmupSubmitRequest,
        WarmupSubmitResponse, ensure_startup_warmup, evm_warmup_request, tron_warmup_request,
    },
};

#[test]
fn test_evm_warmup_request_uses_ormp_selector_and_checkpoint_start() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "42161".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "250".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "durable".to_owned()),
        (
            "ORMPINDEXER_CHAIN_42161_CONTRACTS".to_owned(),
            "0x111,0x222".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_42161_TOPICS".to_owned(),
            "0xaaa,0xbbb".to_owned(),
        ),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let chain = config.chain(42161).expect("Arbitrum chain");

    let request = evm_warmup_request(&config, chain, 466_386_813).expect("build request");

    assert_eq!(
        serde_json::to_value(request).expect("serialize request"),
        serde_json::json!({
            "chain": {
                "family": "Evm",
                "configured_name": "arbitrum",
                "network_id": {"kind": "numeric", "value": 42161}
            },
            "dataset_key": "evm.logs",
            "selector": {
                "kind": "evm_logs",
                "value": {
                    "addresses": ["0x111", "0x222"],
                    "topics": [["0xaaa", "0xbbb"]]
                }
            },
            "range_kind": {"kind": "block"},
            "start": 466386813,
            "end": null,
            "mode": "follow_query",
            "chunk_policy": {"max_range_len": 250}
        })
    );
}

#[test]
fn test_tron_warmup_request_uses_ormp_selector_and_checkpoint_start() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        (
            "ORMPINDEXER_ENABLED_CHAINS".to_owned(),
            "728126428".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_728126428_START_BLOCK".to_owned(),
            "100".to_owned(),
        ),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "5000".to_owned()),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let chain = config.chain(728_126_428).expect("Tron chain");

    let request = tron_warmup_request(&config, chain, 68_291_337).expect("build request");
    let value = serde_json::to_value(request).expect("serialize request");

    assert_eq!(
        value,
        serde_json::json!({
            "chain": {
                "family": {"Other": "tron"},
                "configured_name": "tron-mainnet",
                "network_id": {"kind": "numeric", "value": 728126428}
            },
            "dataset_key": "tron.events",
            "selector": {
                "kind": "other",
                "value": {
                    "kind": "tron_events",
                    "fingerprint": "tron-events/ormp-v2/7b3dbd4a0128ad9c818ddb13",
                    "canonical_key": "contracts/413bc5362ec3a3dbc07292aed4ef18be18de02da3a+4157aa601a0377f5ab313c5a955ee874f5d495fc92+415c5c383febe62f377f8c0ea1de97f2a2ba102e98/events/HashImported+MessageAccepted+MessageAssigned+MessageDispatched+MessageRecv+MessageSent+SignatureSubmittion"
                }
            },
            "range_kind": {"kind": "block"},
            "start": 68291337,
            "end": null,
            "mode": "follow_query",
            "chunk_policy": {"max_range_len": 5000}
        })
    );
}

#[tokio::test]
async fn test_startup_warmup_failure_continues_when_not_required() {
    let config = warmup_config(false);
    let checkpoints = InMemoryCheckpointStore::default();
    let ensurer = FailingWarmupEnsurer;

    let outcomes = ensure_startup_warmup(&config, &checkpoints, &ensurer)
        .await
        .expect("non-required warmup failure continues");

    assert_eq!(
        outcomes,
        vec![DatalensWarmupEnsureOutcome::Failed {
            chain_id: 46,
            error: "submit failed".to_owned()
        }]
    );
}

#[tokio::test]
async fn test_startup_warmup_failure_fails_when_required() {
    let config = warmup_config(true);
    let checkpoints = InMemoryCheckpointStore::default();
    let ensurer = FailingWarmupEnsurer;

    let error = ensure_startup_warmup(&config, &checkpoints, &ensurer)
        .await
        .expect_err("required warmup failure fails startup");

    assert!(error.to_string().contains("submit failed"));
}

#[tokio::test]
async fn test_startup_warmup_submits_tron() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "ORMPINDEXER_ENABLED_CHAINS".to_owned(),
            "728126428".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_728126428_START_BLOCK".to_owned(),
            "100".to_owned(),
        ),
    ]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");
    let checkpoints = InMemoryCheckpointStore::default();
    let ensurer = RecordingWarmupEnsurer::new(WarmupSubmitResponse {
        task_id: "unused".to_owned(),
        created: true,
    });

    let outcomes = ensure_startup_warmup(&config, &checkpoints, &ensurer)
        .await
        .expect("Tron warmup submits");

    assert_eq!(
        outcomes,
        vec![DatalensWarmupEnsureOutcome::Submitted {
            chain_id: 728_126_428,
            task_id: "unused".to_owned(),
            created: true
        }]
    );
    assert_eq!(ensurer.requests.borrow().len(), 1);
    assert_eq!(ensurer.requests.borrow()[0].dataset_key, "tron.events");
    assert_eq!(ensurer.requests.borrow()[0].selector.kind, "other");
}

fn warmup_config(required: bool) -> RuntimeConfig {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_REQUIRED".to_owned(),
            required.to_string(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "250".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "1000".to_owned()),
        (
            "ORMPINDEXER_CHAIN_46_CONTRACTS".to_owned(),
            "0x111".to_owned(),
        ),
        ("ORMPINDEXER_CHAIN_46_TOPICS".to_owned(), "0xaaa".to_owned()),
    ]);

    let mut config = RuntimeConfig::from_env_map(&env).expect("config parses");
    config.finality_mode = FinalityMode::Finalized;
    config
}

struct FailingWarmupEnsurer;

impl DatalensWarmupEnsurer for FailingWarmupEnsurer {
    async fn ensure_warmup_task(
        &self,
        _request: DatalensWarmupSubmitRequest,
    ) -> anyhow::Result<WarmupSubmitResponse> {
        anyhow::bail!("submit failed");
    }
}

#[derive(Clone)]
struct RecordingWarmupEnsurer {
    response: WarmupSubmitResponse,
    requests: Rc<RefCell<Vec<DatalensWarmupSubmitRequest>>>,
}

impl RecordingWarmupEnsurer {
    fn new(response: WarmupSubmitResponse) -> Self {
        Self {
            response,
            requests: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl DatalensWarmupEnsurer for RecordingWarmupEnsurer {
    async fn ensure_warmup_task(
        &self,
        request: DatalensWarmupSubmitRequest,
    ) -> anyhow::Result<WarmupSubmitResponse> {
        self.requests.borrow_mut().push(request);
        Ok(self.response.clone())
    }
}
