use anyhow::Context;

use crate::{
    config::RuntimeConfig,
    database::{PostgresCheckpointStore, PostgresEventWriter, apply_migrations, connect},
    datalens::DatalensHttpClient,
    decoder::EvmEventDecoder,
    runner::IndexerRunner,
};

pub async fn run(run_once: bool) -> anyhow::Result<()> {
    let config = RuntimeConfig::from_env().context("load ORMP indexer runtime config")?;
    let database_url = config
        .database_url
        .as_ref()
        .context("ORMPINDEXER_DATABASE_URL must be configured for run")?;
    let pool = connect(database_url, 5).await?;
    apply_migrations(&pool).await?;

    log::info!(
        "starting ORMP Datalens indexer endpoint={} application={} token_configured={} database_configured=true chains={} batch_size={} dataset={} finality={} dry_run=false",
        config.datalens.endpoint,
        config.datalens.application,
        config.datalens.token.is_some(),
        config.enabled_chains.len(),
        config.batch_size,
        config.dataset,
        config.finality_mode.as_str(),
    );

    let runner = IndexerRunner::new(
        config.clone(),
        DatalensHttpClient::new(config.datalens.clone()),
        PostgresCheckpointStore::new(pool.clone()),
        EvmEventDecoder,
        PostgresEventWriter::new(pool),
    );

    if run_once {
        runner.run_once().await?;
    } else {
        runner.run_loop().await?;
    }

    Ok(())
}

pub async fn migrate() -> anyhow::Result<()> {
    let database_url =
        RuntimeConfig::database_url_from_env().context("load ORMP indexer database config")?;
    let pool = connect(&database_url, 1).await?;
    apply_migrations(&pool).await?;

    log::info!("ORMP indexer migrations applied");

    Ok(())
}
