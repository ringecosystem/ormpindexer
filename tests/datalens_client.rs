use ormpindexer::{
    config::FinalityMode,
    datalens::{
        DatalensFailureKind, DatalensLog, chain_head_finality, classify_datalens_failure_message,
        evm_chain_name, logs_from_native_query_payload, native_graphql_request,
        native_graphql_transaction_request, transactions_from_native_query_payload,
        tron_chain_name,
    },
    planner::TRON_CHAIN_ID,
};

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
    assert_eq!(logs[0].block_hash.as_deref(), Some("0xblock"));
    assert_eq!(logs[0].parent_hash.as_deref(), Some("0xparent"));
    assert_eq!(logs[0].block_timestamp, Some(456));
    assert_eq!(logs[0].transaction_hash, "0xtx");
    assert_eq!(logs[0].transaction_index, Some(2));
    assert_eq!(logs[0].log_index, 3);
    assert_eq!(logs[0].address, "0xcontract");
    assert_eq!(logs[0].transaction_from, None);
    assert_eq!(logs[0].topics, vec!["0xtopic"]);
    assert_eq!(logs[0].data, "0xdata");
}

#[test]
fn test_evm_transaction_query_uses_transactions_dataset_and_decodes_senders() {
    let request =
        native_graphql_transaction_request(&ormpindexer::datalens::DatalensTransactionQuery {
            chain_id: 46,
            from_block: 100,
            to_block: 110,
            finality_mode: ormpindexer::config::FinalityMode::Durable,
        })
        .expect("build transaction request");
    let input = &request["variables"]["input"];

    assert_eq!(
        input["datasetKey"],
        serde_json::json!({"family": "evm", "name": "transactions"})
    );
    assert_eq!(input["selector"], serde_json::json!({"kind": "all"}));

    let transactions = transactions_from_native_query_payload(
        &serde_json::json!({
            "data": {
                "query": {
                    "rows": {
                        "dataset": "transactions",
                        "rows": [{
                            "hash": "0xtx",
                            "block_number": 123,
                            "from": "0xsender"
                        }]
                    }
                }
            }
        }),
        46,
    )
    .expect("decode transaction rows");

    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0].hash, "0xtx");
    assert_eq!(transactions[0].block_number, 123);
    assert_eq!(transactions[0].from, "0xsender");
}

#[test]
fn test_tron_native_query_rows_decode_with_context_metadata() {
    let logs = logs_from_native_query_payload(
        &serde_json::json!({
            "data": {
                "query": {
                    "rows": {
                        "dataset_key": { "family": { "kind": "tron" }, "name": "events" },
                        "rows": {
                            "dataset": "events",
                            "rows": [{
                                "contract_address": "41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD",
                                "event_name": "MessageSent",
                                "event_signature": "MessageSent(bytes32,address,uint256,address,bytes,bytes)",
                                "indexed_fields": [],
                                "non_indexed_fields": {
                                    "msgId": "0x1111111111111111111111111111111111111111111111111111111111111111",
                                    "fromDapp": "0x0000000000000000000000000000000000000030",
                                    "toChainId": "42161",
                                    "toDapp": "0x0000000000000000000000000000000000000031",
                                    "message": "0xaa",
                                    "params": "0xbbcc"
                                },
                                "transaction_id": "trontx",
                                "block_number": 123,
                                "block_hash": "0xblock",
                                "parent_hash": "0xparent",
                                "block_timestamp": 456,
                                "transaction_index": 2,
                                "event_index": 3,
                                "confirmed": true,
                                "source": { "provider": "trongrid_contract_events" }
                            }]
                        }
                    }
                }
            }
        }),
        TRON_CHAIN_ID,
    )
    .expect("decode native Tron query rows");

    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].id.as_deref(), Some("728126428-123-trontx-3"));
    assert_eq!(logs[0].chain_id, TRON_CHAIN_ID);
    assert_eq!(logs[0].block_number, 123);
    assert_eq!(logs[0].block_hash.as_deref(), Some("0xblock"));
    assert_eq!(logs[0].parent_hash.as_deref(), Some("0xparent"));
    assert_eq!(logs[0].block_timestamp, Some(456));
    assert_eq!(logs[0].transaction_hash, "trontx");
    assert_eq!(logs[0].transaction_index, Some(2));
    assert_eq!(logs[0].log_index, 3);
    assert_eq!(
        logs[0].address,
        "41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD"
    );
    assert_eq!(logs[0].transaction_from, None);
    assert_eq!(logs[0].event_name.as_deref(), Some("MessageSent"));
    assert_eq!(
        logs[0].event_signature.as_deref(),
        Some("MessageSent(bytes32,address,uint256,address,bytes,bytes)")
    );
    assert_eq!(
        logs[0].non_indexed_fields.as_ref().expect("decoded args")["toChainId"],
        "42161"
    );
}

#[test]
fn test_tron_native_query_rows_decode_nested_empty_adapter_response() {
    let logs = logs_from_native_query_payload(
        &serde_json::json!({
            "data": {
                "query": {
                    "rows": {
                        "dataset_key": { "family": { "Other": "tron" }, "name": "events" },
                        "rows": {
                            "dataset": "adapter_json",
                            "rows": {
                                "dataset_key": { "family": { "Other": "tron" }, "name": "events" },
                                "rows": []
                            }
                        }
                    }
                }
            }
        }),
        TRON_CHAIN_ID,
    )
    .expect("decode nested empty Tron adapter response");

    assert!(logs.is_empty());
}

#[test]
fn test_tron_native_graphql_request_uses_other_selector_shape() {
    let request = native_graphql_request(&ormpindexer::datalens::DatalensLogQuery {
        chain_id: TRON_CHAIN_ID,
        from_block: 100,
        to_block: 110,
        contracts: vec!["TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_owned()],
        topics: vec!["MessageDispatched".to_owned(), "MessageAccepted".to_owned()],
        finality_mode: ormpindexer::config::FinalityMode::Durable,
    })
    .expect("build Tron request");
    let input = &request["variables"]["input"];

    assert_eq!(
        input["chain"]["family"],
        serde_json::json!({"kind": "other", "other": "tron"})
    );
    assert_eq!(input["chain"]["configuredName"], "tron-mainnet");
    assert_eq!(
        input["chain"]["networkId"],
        serde_json::json!({"numeric": TRON_CHAIN_ID})
    );
    assert_eq!(
        input["datasetKey"],
        serde_json::json!({"family": "tron", "name": "events"})
    );
    assert_eq!(input["selector"]["kind"], "other");
    assert_eq!(input["selector"]["other"]["kind"], "tron_events");
    assert_eq!(
        input["selector"]["other"]["canonicalKey"],
        "contracts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/events/MessageAccepted+MessageDispatched"
    );
    assert_eq!(
        input["selector"]["other"]["fingerprint"],
        "tron-events/ormp-v3/2edc7c1723a90acd0d3d37c0"
    );
    assert_eq!(
        input["range"],
        serde_json::json!({"kind": "block", "start": 100, "end": 110})
    );
    assert_eq!(input["finality"], "durable_only");
}

#[test]
fn test_evm_native_graphql_request_uses_datalens_finality_enum() {
    for (finality_mode, expected) in [
        (FinalityMode::Finalized, "durable_only"),
        (FinalityMode::Durable, "durable_only"),
        (FinalityMode::Safe, "safe_to_latest"),
        (FinalityMode::Latest, "latest_only"),
    ] {
        let request = native_graphql_request(&ormpindexer::datalens::DatalensLogQuery {
            chain_id: 42161,
            from_block: 466_386_813,
            to_block: 466_386_813,
            contracts: vec!["0x2cd1867fb8016f93710b6386f7f9f1d540a60812".to_owned()],
            topics: Vec::new(),
            finality_mode,
        })
        .expect("build EVM request");

        assert_eq!(request["variables"]["input"]["finality"], expected);
    }
}

#[test]
fn test_chain_head_finality_maps_runtime_modes_to_datalens_head_finality() {
    assert_eq!(chain_head_finality(FinalityMode::Finalized), "finalized");
    assert_eq!(chain_head_finality(FinalityMode::Durable), "finalized");
    assert_eq!(chain_head_finality(FinalityMode::Safe), "safe");
    assert_eq!(chain_head_finality(FinalityMode::Latest), "latest");
}

#[test]
fn test_tron_native_graphql_request_rejects_invalid_contract_address() {
    let error = native_graphql_request(&ormpindexer::datalens::DatalensLogQuery {
        chain_id: TRON_CHAIN_ID,
        from_block: 100,
        to_block: 110,
        contracts: vec!["not/a/tron/address".to_owned()],
        topics: Vec::new(),
        finality_mode: ormpindexer::config::FinalityMode::Durable,
    })
    .expect_err("invalid Tron address should fail");

    assert!(
        error
            .to_string()
            .contains("Tron contract address must be hex")
    );
}

#[test]
fn test_evm_chain_name_rejects_unknown_chain_ids() {
    assert_eq!(evm_chain_name(46).expect("darwinia chain name"), "darwinia");
    assert!(evm_chain_name(99_999).is_err());
}

#[test]
fn test_tron_chain_name_rejects_unknown_chain_ids() {
    assert_eq!(
        tron_chain_name(TRON_CHAIN_ID).expect("tron mainnet"),
        "tron-mainnet"
    );
    assert!(tron_chain_name(46).is_err());
}

#[test]
fn test_datalens_failure_classifies_provider_limits() {
    assert_eq!(
        classify_datalens_failure_message("query returns too many logs, narrow your filter"),
        DatalensFailureKind::ProviderLimit
    );
    assert_eq!(
        classify_datalens_failure_message("provider range limit exceeded"),
        DatalensFailureKind::ProviderLimit
    );
}

#[test]
fn test_datalens_failure_classifies_transient_errors() {
    assert_eq!(
        classify_datalens_failure_message("ProviderFailure: upstream returned 502"),
        DatalensFailureKind::Transient
    );
    assert_eq!(
        classify_datalens_failure_message("request timed out after 300 seconds"),
        DatalensFailureKind::Transient
    );
    assert_eq!(
        classify_datalens_failure_message("rate-limit 429"),
        DatalensFailureKind::Transient
    );
}

#[test]
fn test_datalens_failure_classifies_other_errors() {
    assert_eq!(
        classify_datalens_failure_message("permission denied"),
        DatalensFailureKind::Other
    );
}
