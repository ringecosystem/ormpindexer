use crate::config::RuntimeConfig;
use ormpindexer::schema::POSTGRES_SCHEMA_MIGRATION;

pub async fn run() -> anyhow::Result<()> {
    let config = RuntimeConfig::from_env();

    log::info!(
        "starting ORMP indexer datalens_endpoint={} datalens_application={} token_configured={} database_configured={}",
        config.datalens_endpoint,
        config.datalens_application,
        config.datalens_token.is_some(),
        config.database_url.is_some(),
    );

    Ok(())
}

pub async fn migrate() -> anyhow::Result<()> {
    let config = RuntimeConfig::from_env();

    log::info!(
        "ORMP indexer schema compatibility migration is defined bytes={} database_configured={}",
        POSTGRES_SCHEMA_MIGRATION.len(),
        config.database_url.is_some(),
    );

    Ok(())
}
