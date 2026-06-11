use std::{collections::BTreeMap, env, fmt, time::Duration};

use anyhow::{Context, bail};

use crate::planner::{TRON_CHAIN_ID, default_chain_config};

pub const DEFAULT_DATASET: &str = "datalens-native";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub datalens: DatalensConfig,
    pub database_url: Option<SecretString>,
    pub enabled_chains: Vec<ChainConfig>,
    pub batch_size: u64,
    pub start_block: u64,
    pub finality_mode: FinalityMode,
    pub dataset: String,
    pub poll_interval: Duration,
}

impl RuntimeConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let env = env::vars().collect::<BTreeMap<_, _>>();
        Self::from_env_map(&env)
    }

    pub fn database_url_from_env() -> anyhow::Result<SecretString> {
        let env = env::vars().collect::<BTreeMap<_, _>>();
        Self::database_url_from_env_map(&env)
    }

    pub fn database_url_from_env_map(
        env: &BTreeMap<String, String>,
    ) -> anyhow::Result<SecretString> {
        optional_env(env, "ORMPINDEXER_DATABASE_URL")
            .map(SecretString::new)
            .context("ORMPINDEXER_DATABASE_URL must be configured")
    }

    pub fn from_env_map(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let start_block = optional_u64(env, "ORMPINDEXER_START_BLOCK")?;
        let chain_ids = required_list(env, "ORMPINDEXER_ENABLED_CHAINS")?
            .into_iter()
            .map(|value| {
                value
                    .parse::<u64>()
                    .with_context(|| format!("parse ORMPINDEXER_ENABLED_CHAINS item: {value}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let enabled_chains = chain_ids
            .into_iter()
            .map(|chain_id| ChainConfig::from_env_map(env, chain_id, start_block))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            datalens: DatalensConfig {
                endpoint: optional_env(env, "ORMPINDEXER_DATALENS_ENDPOINT")
                    .unwrap_or_else(|| "http://localhost:8080".to_owned()),
                application: optional_env(env, "ORMPINDEXER_DATALENS_APPLICATION")
                    .unwrap_or_else(|| "ormpindexer".to_owned()),
                token: optional_env(env, "ORMPINDEXER_DATALENS_TOKEN").map(SecretString::new),
            },
            database_url: optional_env(env, "ORMPINDEXER_DATABASE_URL").map(SecretString::new),
            enabled_chains,
            batch_size: optional_u64(env, "ORMPINDEXER_BATCH_SIZE")?.unwrap_or(1_000),
            start_block: start_block.unwrap_or(0),
            finality_mode: optional_env(env, "ORMPINDEXER_FINALITY_MODE")
                .as_deref()
                .map(str::parse)
                .transpose()?
                .unwrap_or(FinalityMode::Finalized),
            dataset: optional_env(env, "ORMPINDEXER_DATASET")
                .unwrap_or_else(|| DEFAULT_DATASET.to_owned()),
            poll_interval: Duration::from_secs(
                optional_u64(env, "ORMPINDEXER_POLL_INTERVAL_SECS")?.unwrap_or(30),
            ),
        })
    }

    pub fn chain(&self, chain_id: u64) -> Option<&ChainConfig> {
        self.enabled_chains
            .iter()
            .find(|chain| chain.chain_id == chain_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensConfig {
    pub endpoint: String,
    pub application: String,
    pub token: Option<SecretString>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainConfig {
    pub chain_id: u64,
    pub start_block: u64,
    pub contracts: Vec<String>,
    pub topics: Vec<String>,
}

impl ChainConfig {
    fn from_env_map(
        env: &BTreeMap<String, String>,
        chain_id: u64,
        default_start_block: Option<u64>,
    ) -> anyhow::Result<Self> {
        let prefix = format!("ORMPINDEXER_CHAIN_{chain_id}");
        let default = default_chain_config(chain_id)?;
        let contracts = optional_list(env, &format!("{prefix}_CONTRACTS"));
        let topics = optional_list(env, &format!("{prefix}_TOPICS"));
        let chain_start_block = optional_u64(env, &format!("{prefix}_START_BLOCK"))?;
        let start_block = if chain_id == TRON_CHAIN_ID {
            chain_start_block
                .with_context(|| format!("{prefix}_START_BLOCK must be configured for Tron"))?
        } else {
            chain_start_block
                .or(default_start_block)
                .unwrap_or(default.start_block)
        };

        Ok(Self {
            chain_id,
            start_block,
            contracts: if contracts.is_empty() {
                default.contracts
            } else {
                contracts
            },
            topics: if topics.is_empty() {
                default.topics
            } else {
                topics
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityMode {
    Finalized,
    Durable,
}

impl FinalityMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Durable => "durable",
        }
    }
}

impl std::str::FromStr for FinalityMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "finalized" | "safe" => Ok(Self::Finalized),
            "durable" => Ok(Self::Durable),
            _ => bail!("ORMPINDEXER_FINALITY_MODE must be finalized or durable"),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn optional_env(env: &BTreeMap<String, String>, key: &str) -> Option<String> {
    env.get(key)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_u64(env: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Option<u64>> {
    optional_env(env, key)
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("parse {key} as u64"))
        })
        .transpose()
}

fn optional_list(env: &BTreeMap<String, String>, key: &str) -> Vec<String> {
    optional_env(env, key)
        .map(|value| split_list(&value))
        .unwrap_or_default()
}

fn required_list(env: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Vec<String>> {
    let values = optional_list(env, key);
    if values.is_empty() {
        bail!("{key} must be configured");
    }
    Ok(values)
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
