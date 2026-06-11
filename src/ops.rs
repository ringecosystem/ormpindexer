use serde::Serialize;
use sqlx::{FromRow, PgPool};

const LEGACY_TABLES: &[&str] = &[
    "ormp_hash_imported",
    "ormp_message_accepted",
    "ormp_message_assigned",
    "ormp_message_dispatched",
    "msgport_message_recv",
    "msgport_message_sent",
    "signature_pub_signature_submittion",
];

#[derive(Clone, Debug, Eq, PartialEq, FromRow, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointRow {
    pub chain_id: String,
    pub dataset: String,
    pub next_block: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, FromRow, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainProgressRow {
    pub chain_id: String,
    pub datasets: i64,
    pub min_next_block: String,
    pub max_next_block: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, FromRow, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetProgressRow {
    pub dataset: String,
    pub chains: i64,
    pub min_next_block: String,
    pub max_next_block: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct LegacyTableRowCount {
    pub table_name: String,
    pub row_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    pub checkpoints: Vec<CheckpointRow>,
    pub progress: ProgressSummary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressSummary {
    pub chains: Vec<ChainProgressRow>,
    pub datasets: Vec<DatasetProgressRow>,
}

pub async fn check_readiness(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}

pub async fn load_status(pool: &PgPool) -> anyhow::Result<StatusResponse> {
    let checkpoints = sqlx::query_as::<_, CheckpointRow>(
        "SELECT
            chain_id::TEXT AS chain_id,
            dataset,
            next_block::TEXT AS next_block,
            updated_at::TEXT AS updated_at
         FROM ormp_indexer_checkpoint
         ORDER BY chain_id::NUMERIC, dataset",
    )
    .fetch_all(pool)
    .await?;

    let chains = sqlx::query_as::<_, ChainProgressRow>(
        "SELECT
            chain_id::TEXT AS chain_id,
            COUNT(*)::BIGINT AS datasets,
            MIN(next_block)::TEXT AS min_next_block,
            MAX(next_block)::TEXT AS max_next_block,
            MAX(updated_at)::TEXT AS updated_at
         FROM ormp_indexer_checkpoint
         GROUP BY chain_id
         ORDER BY chain_id::NUMERIC",
    )
    .fetch_all(pool)
    .await?;

    let datasets = sqlx::query_as::<_, DatasetProgressRow>(
        "SELECT
            dataset,
            COUNT(*)::BIGINT AS chains,
            MIN(next_block)::TEXT AS min_next_block,
            MAX(next_block)::TEXT AS max_next_block,
            MAX(updated_at)::TEXT AS updated_at
         FROM ormp_indexer_checkpoint
         GROUP BY dataset
         ORDER BY dataset",
    )
    .fetch_all(pool)
    .await?;

    Ok(StatusResponse {
        checkpoints,
        progress: ProgressSummary { chains, datasets },
    })
}

pub async fn render_metrics(pool: &PgPool) -> anyhow::Result<String> {
    let checkpoints = sqlx::query_as::<_, CheckpointRow>(
        "SELECT
            chain_id::TEXT AS chain_id,
            dataset,
            next_block::TEXT AS next_block,
            updated_at::TEXT AS updated_at
         FROM ormp_indexer_checkpoint
         ORDER BY chain_id::NUMERIC, dataset",
    )
    .fetch_all(pool)
    .await?;

    let row_counts = sqlx::query_as::<_, LegacyTableRowCount>(
        "SELECT table_name, row_count
         FROM (
           SELECT 'ormp_hash_imported'::TEXT AS table_name, COUNT(*)::BIGINT AS row_count FROM ormp_hash_imported
           UNION ALL
           SELECT 'ormp_message_accepted', COUNT(*)::BIGINT FROM ormp_message_accepted
           UNION ALL
           SELECT 'ormp_message_assigned', COUNT(*)::BIGINT FROM ormp_message_assigned
           UNION ALL
           SELECT 'ormp_message_dispatched', COUNT(*)::BIGINT FROM ormp_message_dispatched
           UNION ALL
           SELECT 'msgport_message_recv', COUNT(*)::BIGINT FROM msgport_message_recv
           UNION ALL
           SELECT 'msgport_message_sent', COUNT(*)::BIGINT FROM msgport_message_sent
           UNION ALL
           SELECT 'signature_pub_signature_submittion', COUNT(*)::BIGINT FROM signature_pub_signature_submittion
         ) counts",
    )
    .fetch_all(pool)
    .await?;

    Ok(format_metrics(&checkpoints, &row_counts))
}

fn format_metrics(checkpoints: &[CheckpointRow], row_counts: &[LegacyTableRowCount]) -> String {
    let mut body = String::new();

    body.push_str(
        "# HELP ormp_indexer_checkpoint_next_block Next block recorded per chain and dataset.\n",
    );
    body.push_str("# TYPE ormp_indexer_checkpoint_next_block gauge\n");
    for checkpoint in checkpoints {
        body.push_str("ormp_indexer_checkpoint_next_block{chain_id=\"");
        body.push_str(&escape_label_value(&checkpoint.chain_id));
        body.push_str("\",dataset=\"");
        body.push_str(&escape_label_value(&checkpoint.dataset));
        body.push_str("\"} ");
        body.push_str(&checkpoint.next_block);
        body.push('\n');
    }

    body.push_str("# HELP ormp_indexer_checkpoint_rows Total checkpoint rows.\n");
    body.push_str("# TYPE ormp_indexer_checkpoint_rows gauge\n");
    body.push_str("ormp_indexer_checkpoint_rows ");
    body.push_str(&checkpoints.len().to_string());
    body.push('\n');

    body.push_str("# HELP ormp_indexer_legacy_table_rows Legacy GraphQL table row counts.\n");
    body.push_str("# TYPE ormp_indexer_legacy_table_rows gauge\n");
    for table in LEGACY_TABLES {
        let count = row_counts
            .iter()
            .find(|row| row.table_name == *table)
            .map(|row| row.row_count)
            .unwrap_or_default();
        body.push_str("ormp_indexer_legacy_table_rows{table=\"");
        body.push_str(table);
        body.push_str("\"} ");
        body.push_str(&count.to_string());
        body.push('\n');
    }

    body
}

fn escape_label_value(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', "\\\"")
}
