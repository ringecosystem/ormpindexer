use crate::config::RuntimeConfig;

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
        "no ORMP indexer migrations are defined yet database_configured={}",
        config.database_url.is_some(),
    );

    Ok(())
}
