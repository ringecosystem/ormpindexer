use std::collections::BTreeMap;

use ormpindexer::{
    config::{FinalityMode, RuntimeConfig},
    planner::{
        PRODUCTION_EVM_CHAIN_IDS, SIGNATURE_PUB_ADDRESS, SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC,
        default_evm_chain_config, plan_evm_log_queries,
    },
};

#[test]
fn test_production_evm_chains_produce_default_query_plans() {
    for chain_id in PRODUCTION_EVM_CHAIN_IDS {
        let chain = default_evm_chain_config(*chain_id).expect("configured production chain");
        let plans = plan_evm_log_queries(
            "datalens-native",
            &chain,
            chain.start_block,
            chain.start_block + 9,
            100,
            FinalityMode::Finalized,
        )
        .expect("query plans");

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].dataset, "datalens-native");
        assert_eq!(plans[0].query.chain_id, *chain_id);
        assert_eq!(plans[0].query.from_block, chain.start_block);
        assert_eq!(plans[0].query.to_block, chain.start_block + 9);
        assert_eq!(plans[0].query.contracts, chain.contracts);
        assert_eq!(plans[0].query.topics, chain.topics);
        assert_eq!(plans[0].query.finality_mode, FinalityMode::Finalized);
    }
}

#[test]
fn test_darwinia_includes_signature_pub_but_other_defaults_do_not() {
    let darwinia = default_evm_chain_config(46).expect("darwinia");
    let ethereum = default_evm_chain_config(1).expect("ethereum");
    let polygon = default_evm_chain_config(137).expect("polygon");
    let arbitrum = default_evm_chain_config(42161).expect("arbitrum");

    assert!(
        darwinia
            .contracts
            .contains(&SIGNATURE_PUB_ADDRESS.to_owned())
    );
    assert!(
        darwinia
            .topics
            .contains(&SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC.to_owned())
    );

    for chain in [ethereum, polygon, arbitrum] {
        assert!(!chain.contracts.contains(&SIGNATURE_PUB_ADDRESS.to_owned()));
        assert!(
            !chain
                .topics
                .contains(&SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC.to_owned())
        );
    }
}

#[test]
fn test_plan_evm_log_queries_splits_ranges_by_limit() {
    let chain = default_evm_chain_config(46).expect("darwinia");
    let plans = plan_evm_log_queries(
        "datalens-native",
        &chain,
        100,
        220,
        50,
        FinalityMode::Durable,
    )
    .expect("split plans");
    let ranges = plans
        .iter()
        .map(|plan| (plan.query.from_block, plan.query.to_block))
        .collect::<Vec<_>>();

    assert_eq!(ranges, vec![(100, 149), (150, 199), (200, 220)]);
    assert!(plans.iter().all(|plan| plan.query.chain_id == 46));
    assert!(
        plans
            .iter()
            .all(|plan| plan.query.finality_mode == FinalityMode::Durable)
    );
}

#[test]
fn test_unknown_chain_returns_clear_error() {
    let error = default_evm_chain_config(999_999).expect_err("unknown chain");

    assert!(
        error
            .to_string()
            .contains("unconfigured ORMP EVM chain 999999")
    );
}

#[test]
fn test_runtime_config_uses_confirmed_defaults_without_lisk() {
    let env = BTreeMap::from([("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "1,46".to_owned())]);
    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert!(config.chain(1).is_some());
    assert!(config.chain(46).is_some());
    assert!(config.chain(1135).is_none());
    assert_eq!(config.chain(1).expect("ethereum").start_block, 20_009_590);
    assert_eq!(config.chain(46).expect("darwinia").start_block, 2_830_100);
}

#[test]
fn test_confirmed_evm_chain_defaults_are_available_without_lisk() {
    let starts = [
        (1, 20_009_590),
        (46, 2_830_100),
        (137, 57_244_567),
        (42161, 217_891_600),
        (8453, 30_508_102),
        (44, 2_900_604),
        (1284, 6_294_138),
        (81457, 4_293_849),
        (2818, 59_565),
    ];

    for (chain_id, start_block) in starts {
        let chain = default_evm_chain_config(chain_id).expect("confirmed default chain");

        assert_eq!(chain.start_block, start_block);
    }

    assert!(default_evm_chain_config(1135).is_err());
}
