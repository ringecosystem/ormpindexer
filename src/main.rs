use anyhow::Context;
use clap::{Parser, Subcommand};

use ormpindexer::{
    config::RuntimeConfig,
    database,
    graphql::{build_router, build_schema},
    runtime::{migrate, run},
};

#[derive(Debug, Parser)]
#[command(name = "ormpindexer")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        once: bool,
    },
    Serve {
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen_addr: String,
    },
    Migrate,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging()?;
    let cli = Cli::parse();

    match cli.command {
        Command::Run { once } => run(once).await,
        Command::Serve { listen_addr } => serve(&listen_addr).await,
        Command::Migrate => migrate().await,
    }
}

async fn serve(listen_addr: &str) -> anyhow::Result<()> {
    let database_url = RuntimeConfig::database_url_from_env()?;
    let pool = database::connect(&database_url, 5).await?;
    database::apply_migrations(&pool).await?;
    let app = build_router(build_schema(pool));
    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind GraphQL server to {listen_addr}"))?;
    axum::serve(listener, app)
        .await
        .context("serve GraphQL server")
}

fn init_logging() -> anyhow::Result<()> {
    tracing_log::LogTracer::init().context("initialize log tracer")?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize tracing subscriber: {error}"))
}
