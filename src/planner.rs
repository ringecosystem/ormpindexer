use anyhow::{bail, ensure};

use crate::{
    config::{ChainConfig, FinalityMode},
    datalens::DatalensLogQuery,
};

pub const MSGPORT_ADDRESS: &str = "0x2cd1867Fb8016f93710B6386f7f9F1D540A60812";
pub const ORMP_ADDRESS: &str = "0x13b2211a7cA45Db2808F6dB05557ce5347e3634e";
pub const SIGNATURE_PUB_ADDRESS: &str = "0x57Aa601A0377f5AB313C5A955ee874f5D495fC92";

pub const MSGPORT_MESSAGE_RECV_TOPIC: &str =
    "0xea087580bb17f433441f3b6c0c0b80cae92ee74a8d7f50050388646d9ffd1431";
pub const MSGPORT_MESSAGE_SENT_TOPIC: &str =
    "0x40195d26d027672e04e23e34282d68c3d43ea138415b24c54fcdb9c2573e5975";
pub const ORMP_HASH_IMPORTED_TOPIC: &str =
    "0xa931ec14fe958397dcb26e285e56292c13d77907712b51bbaa24cfc9349b789d";
pub const ORMP_MESSAGE_ACCEPTED_TOPIC: &str =
    "0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18";
pub const ORMP_MESSAGE_ASSIGNED_TOPIC: &str =
    "0x3832f95736b288316c84b775a004a9d17177362548ce253cba9acb4801875f4d";
pub const ORMP_MESSAGE_DISPATCHED_TOPIC: &str =
    "0x62b1dc20fd6f1518626da5b6f9897e8cd4ebadbad071bb66dc96a37c970087a8";
pub const SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC: &str =
    "0x8b3975e4768e70d323e926e2cef0676fc9a3250437d9b8f90b52c770f0d7545f";

pub const PRODUCTION_EVM_CHAIN_IDS: &[u64] = &[1, 46, 137, 42161];

const DEFAULT_EVM_CHAINS: &[DefaultEvmChain] = &[
    DefaultEvmChain {
        chain_id: 1,
        start_block: 20_009_590,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 46,
        start_block: 2_830_100,
        include_signature_pub: true,
    },
    DefaultEvmChain {
        chain_id: 137,
        start_block: 57_244_567,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 42161,
        start_block: 217_891_600,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 8453,
        start_block: 30_508_102,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 44,
        start_block: 2_900_604,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 1284,
        start_block: 6_294_138,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 81457,
        start_block: 4_293_849,
        include_signature_pub: false,
    },
    DefaultEvmChain {
        chain_id: 2818,
        start_block: 59_565,
        include_signature_pub: false,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DefaultEvmChain {
    chain_id: u64,
    start_block: u64,
    include_signature_pub: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedDatalensLogQuery {
    pub dataset: String,
    pub query: DatalensLogQuery,
}

pub fn default_evm_chain_config(chain_id: u64) -> anyhow::Result<ChainConfig> {
    let Some(default) = DEFAULT_EVM_CHAINS
        .iter()
        .find(|chain| chain.chain_id == chain_id)
    else {
        bail!("unconfigured ORMP EVM chain {chain_id}");
    };

    Ok(ChainConfig {
        chain_id,
        start_block: default.start_block,
        contracts: default_contracts(default.include_signature_pub),
        topics: default_topics(default.include_signature_pub),
    })
}

pub fn plan_evm_log_queries(
    dataset: &str,
    chain: &ChainConfig,
    from_block: u64,
    to_block: u64,
    max_range_len: u64,
    finality_mode: FinalityMode,
) -> anyhow::Result<Vec<PlannedDatalensLogQuery>> {
    ensure!(
        max_range_len > 0,
        "max range length must be greater than zero"
    );
    ensure!(
        from_block <= to_block,
        "from block must be less than or equal to to block"
    );
    ensure!(
        !chain.contracts.is_empty(),
        "EVM log query planner requires at least one contract address"
    );
    ensure!(
        !chain.topics.is_empty(),
        "EVM log query planner requires at least one event topic"
    );

    let mut plans = Vec::new();
    let mut next_from = from_block;

    while next_from <= to_block {
        let range_end = next_from.saturating_add(max_range_len - 1).min(to_block);
        plans.push(PlannedDatalensLogQuery {
            dataset: dataset.to_owned(),
            query: DatalensLogQuery {
                chain_id: chain.chain_id,
                from_block: next_from,
                to_block: range_end,
                contracts: chain.contracts.clone(),
                topics: chain.topics.clone(),
                finality_mode,
            },
        });

        if range_end == u64::MAX {
            break;
        }
        next_from = range_end + 1;
    }

    Ok(plans)
}

fn default_contracts(include_signature_pub: bool) -> Vec<String> {
    let mut contracts = vec![MSGPORT_ADDRESS.to_owned(), ORMP_ADDRESS.to_owned()];
    if include_signature_pub {
        contracts.push(SIGNATURE_PUB_ADDRESS.to_owned());
    }
    contracts
}

fn default_topics(include_signature_pub: bool) -> Vec<String> {
    let mut topics = vec![
        MSGPORT_MESSAGE_RECV_TOPIC.to_owned(),
        MSGPORT_MESSAGE_SENT_TOPIC.to_owned(),
        ORMP_HASH_IMPORTED_TOPIC.to_owned(),
        ORMP_MESSAGE_ACCEPTED_TOPIC.to_owned(),
        ORMP_MESSAGE_ASSIGNED_TOPIC.to_owned(),
        ORMP_MESSAGE_DISPATCHED_TOPIC.to_owned(),
    ];
    if include_signature_pub {
        topics.push(SIGNATURE_PUB_SIGNATURE_SUBMITTION_TOPIC.to_owned());
    }
    topics
}
