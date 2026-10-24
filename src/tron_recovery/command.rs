use anyhow::{Context, ensure};
use clap::Args as ClapArgs;
use sqlx::PgPool;

use super::{rpc::TronRpc, scan::recover, storage::RecoveryStore};
use crate::{config::RuntimeConfig, database};

#[derive(Debug, Clone, ClapArgs)]
pub struct Args {
    /// First block on the first run. Subsequent runs resume the recovery checkpoint.
    #[arg(long, default_value_t = 87_383_828)]
    pub from_block: u64,
    /// Inclusive end, bounded by the confirmed head sampled at startup.
    #[arg(long)]
    pub to_block: Option<u64>,
    /// Maximum inclusive getLogs span. Failed queries shrink to a single block.
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u64).range(1..=2000))]
    pub batch_size: u64,
    /// Preview the explicit range without writing events or progress.
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let endpoint = std::env::var("ORMPINDEXER_TRON_RPC_URL").context(
        "ORMPINDEXER_TRON_RPC_URL must be configured (Tron HTTP API base or /jsonrpc URL)",
    )?;
    let api_key = std::env::var("ORMPINDEXER_TRON_API_KEY").ok();
    let rpc = TronRpc::new(&endpoint, api_key.as_deref())?;
    let pool = if args.dry_run {
        None
    } else {
        let url = RuntimeConfig::database_url_from_env()?;
        Some(database::connect(&url, 3).await?)
    };
    execute(args, rpc, pool).await
}

pub(super) async fn execute(args: Args, rpc: TronRpc, pool: Option<PgPool>) -> anyhow::Result<()> {
    ensure!(args.batch_size > 0, "batch size must be positive");
    rpc.ensure_mainnet().await?;
    let confirmed_head = rpc.confirmed_head().await?;
    let end = args.to_block.unwrap_or(confirmed_head);
    ensure!(end < u64::MAX, "Tron end height overflow");
    ensure!(
        end <= confirmed_head,
        "requested end is above confirmed Tron head {confirmed_head}"
    );
    if let Some(end) = args.to_block {
        ensure!(args.from_block <= end, "from-block exceeds to-block");
    }
    let store = match pool.filter(|_| !args.dry_run) {
        Some(pool) => match RecoveryStore::lock(pool).await? {
            Some(store) => Some(store),
            None => {
                log::info!("Tron recovery already running; exiting without writes");
                return Ok(());
            }
        },
        None => {
            ensure!(args.dry_run, "database required for recovery writes");
            None
        }
    };
    let start = match &store {
        Some(store) => {
            store.check_preceding_message().await?;
            store.next_block(args.from_block).await?
        }
        None => args.from_block,
    };
    log::info!(
        "Tron recovery start={start} end={end} confirmed_head={confirmed_head} dry_run={}",
        args.dry_run
    );
    if start > end {
        log::info!("Tron recovery already caught up");
        return Ok(());
    }
    let (ranges, events) = recover(&rpc, store.as_ref(), start, end, args.batch_size).await?;
    log::info!(
        "Tron recovery complete ranges={ranges} events={events} next_block={} dry_run={}",
        end + 1,
        args.dry_run
    );
    Ok(())
}
