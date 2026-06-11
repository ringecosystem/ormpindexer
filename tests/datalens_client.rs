use ormpindexer::datalens::DatalensLog;

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
