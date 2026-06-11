use ormpindexer::datalens::{DatalensLog, logs_from_native_query_payload};

#[test]
fn test_datalens_log_decodes_native_graphql_camel_case_response() {
    let log: DatalensLog = serde_json::from_value(serde_json::json!({
        "id": "46-0xtx-3",
        "chainId": 46,
        "blockNumber": 123,
        "blockTimestamp": 456,
        "transactionHash": "0xtx",
        "transactionIndex": 2,
        "logIndex": 3,
        "address": "0xcontract",
        "transactionFrom": "0xsender",
        "topics": ["0xtopic"],
        "data": "0xdata"
    }))
    .expect("decode native GraphQL log shape");

    assert_eq!(log.id.as_deref(), Some("46-0xtx-3"));
    assert_eq!(log.chain_id, 46);
    assert_eq!(log.block_number, 123);
    assert_eq!(log.block_timestamp, Some(456));
    assert_eq!(log.transaction_hash, "0xtx");
    assert_eq!(log.transaction_index, Some(2));
    assert_eq!(log.log_index, 3);
    assert_eq!(log.address, "0xcontract");
    assert_eq!(log.transaction_from.as_deref(), Some("0xsender"));
    assert_eq!(log.topics, vec!["0xtopic"]);
    assert_eq!(log.data, "0xdata");
}

#[test]
fn test_datalens_log_decodes_legacy_snake_case_response() {
    let log: DatalensLog = serde_json::from_value(serde_json::json!({
        "id": "46-0xtx-7",
        "chain_id": 46,
        "block_number": 123,
        "block_timestamp": 456,
        "transaction_hash": "0xtx",
        "transaction_index": 2,
        "eventIndex": 7,
        "address": "0xcontract",
        "transaction_from": "0xsender",
        "topics": ["0xtopic"],
        "data": "0xdata"
    }))
    .expect("decode legacy-compatible log shape");

    assert_eq!(log.id.as_deref(), Some("46-0xtx-7"));
    assert_eq!(log.chain_id, 46);
    assert_eq!(log.block_number, 123);
    assert_eq!(log.block_timestamp, Some(456));
    assert_eq!(log.transaction_hash, "0xtx");
    assert_eq!(log.transaction_index, Some(2));
    assert_eq!(log.log_index, 7);
    assert_eq!(log.address, "0xcontract");
    assert_eq!(log.transaction_from.as_deref(), Some("0xsender"));
    assert_eq!(log.topics, vec!["0xtopic"]);
    assert_eq!(log.data, "0xdata");
}

#[test]
fn test_native_query_rows_decode_with_context_metadata() {
    let logs = logs_from_native_query_payload(
        &serde_json::json!({
            "data": {
                "query": {
                    "rows": {
                        "dataset_key": { "family": { "kind": "evm" }, "name": "logs" },
                        "rows": {
                            "dataset": "logs",
                            "rows": [{
                                "block_number": 123,
                                "block_hash": "0xblock",
                                "parent_hash": "0xparent",
                                "block_timestamp": 456,
                                "transaction_hash": "0xtx",
                                "transaction_index": 2,
                                "log_index": 3,
                                "address": "0xcontract",
                                "topics": ["0xtopic"],
                                "data": "0xdata",
                                "removed": false
                            }]
                        }
                    }
                }
            }
        }),
        46,
    )
    .expect("decode native query rows");

    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].id.as_deref(), Some("46-123-0xtx-3"));
    assert_eq!(logs[0].chain_id, 46);
    assert_eq!(logs[0].block_number, 123);
    assert_eq!(logs[0].block_timestamp, Some(456));
    assert_eq!(logs[0].transaction_hash, "0xtx");
    assert_eq!(logs[0].transaction_index, Some(2));
    assert_eq!(logs[0].log_index, 3);
    assert_eq!(logs[0].address, "0xcontract");
    assert_eq!(logs[0].transaction_from, None);
    assert_eq!(logs[0].topics, vec!["0xtopic"]);
    assert_eq!(logs[0].data, "0xdata");
}
