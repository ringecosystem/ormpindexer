use sqlx::PgPool;

use ormpindexer::{
    database::{EventWriter, PostgresEventWriter, apply_migrations},
    decoder::{EventDecoder, EvmEventDecoder, decode_evm_log},
};

mod legacy_parity_support;

use legacy_parity_support::{
    compatibility_rows, evm_expected_rows, evm_fixture_logs, fetch_compatibility_rows,
    legacy_expected_rows, test_database_url, tron_expected_rows, tron_fixture_logs,
    truncate_legacy_tables,
};

#[test]
fn test_evm_legacy_rows_match_subsquid_compatibility_expectations() {
    let decoded = evm_fixture_logs()
        .iter()
        .map(|log| decode_evm_log(log).expect("decode EVM parity fixture"))
        .collect::<Vec<_>>();

    assert_eq!(compatibility_rows(&decoded), evm_expected_rows());
}

#[tokio::test]
async fn test_tron_structured_json_rows_match_subsquid_compatibility_expectations() {
    let decoder = EvmEventDecoder;
    let mut decoded = Vec::new();
    for log in tron_fixture_logs() {
        decoded.extend(
            decoder
                .decode(&log)
                .await
                .expect("decode Tron parity fixture"),
        );
    }

    assert_eq!(compatibility_rows(&decoded), tron_expected_rows());
}

#[tokio::test]
async fn test_postgres_writer_rows_match_subsquid_compatibility_expectations() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping parity DB test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };

    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_legacy_tables(&pool).await;

    let mut events = evm_fixture_logs()
        .iter()
        .map(|log| decode_evm_log(log).expect("decode EVM parity fixture"))
        .collect::<Vec<_>>();
    let decoder = EvmEventDecoder;
    for log in tron_fixture_logs() {
        events.extend(
            decoder
                .decode(&log)
                .await
                .expect("decode Tron parity fixture"),
        );
    }
    let expected = legacy_expected_rows();

    let writer = PostgresEventWriter::new(pool.clone());
    let written = writer
        .write_events(&events)
        .await
        .expect("write parity fixture events");

    assert_eq!(written, events.len());
    assert_eq!(fetch_compatibility_rows(&pool).await, expected);
}
