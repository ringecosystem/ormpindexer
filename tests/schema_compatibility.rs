use ormpindexer::schema::{
    ADDRESS_ORACLE, ADDRESS_RELAYER, AssignmentConfig, ChainLogMetadata, EventSource,
    LegacyOrmPEvent, LegacySchema, MsgportMessageRecvRow, MsgportMessageSentRow,
    OrmpMessageAcceptedRow, OrmpMessageAssignedRow, SignaturePubSignatureSubmittionRow,
    apply_assignment_to_accepted,
};

#[test]
fn test_ormp_accepted_uses_msg_hash_id_and_nullable_assignment_fields() {
    let event = LegacyOrmPEvent::MessageAccepted {
        metadata: evm_metadata("log-1"),
        msg_hash: "0xabc".to_owned(),
        channel: "0xchannel".to_owned(),
        index: 7,
        from_chain_id: 1,
        from: "0xfrom".to_owned(),
        to_chain_id: 46,
        to: "0xto".to_owned(),
        gas_limit: 500_000,
        encoded: "0xencoded".to_owned(),
    };

    let row = OrmpMessageAcceptedRow::from_event(event);

    assert_eq!(row.id, "0xabc");
    assert_eq!(row.msg_hash, "0xabc");
    assert_eq!(row.log_index, 3);
    assert_eq!(row.transaction_hash, "0xtx");
    assert_eq!(row.oracle, None);
    assert_eq!(row.oracle_assigned, None);
    assert_eq!(row.oracle_assigned_fee, None);
    assert_eq!(row.relayer, None);
    assert_eq!(row.relayer_assigned, None);
    assert_eq!(row.relayer_assigned_fee, None);
}

#[test]
fn test_assigned_backfills_accepted_when_configured_addresses_match() {
    let accepted_event = LegacyOrmPEvent::MessageAccepted {
        metadata: evm_metadata("accepted-log"),
        msg_hash: "0xmsg".to_owned(),
        channel: "0xchannel".to_owned(),
        index: 8,
        from_chain_id: 1,
        from: "0xfrom".to_owned(),
        to_chain_id: 46,
        to: "0xto".to_owned(),
        gas_limit: 500_000,
        encoded: "0xencoded".to_owned(),
    };
    let mut accepted = OrmpMessageAcceptedRow::from_event(accepted_event);
    let assigned = OrmpMessageAssignedRow::from_event(LegacyOrmPEvent::MessageAssigned {
        metadata: evm_metadata("assigned-log"),
        msg_hash: "0xmsg".to_owned(),
        oracle: ADDRESS_ORACLE[0].to_ascii_uppercase(),
        relayer: ADDRESS_RELAYER[0].to_ascii_uppercase(),
        oracle_fee: 11,
        relayer_fee: 22,
        params: "0xparams".to_owned(),
    });

    let updated = apply_assignment_to_accepted(
        &mut accepted,
        &assigned,
        &AssignmentConfig::legacy_defaults(),
    );

    assert!(updated.oracle);
    assert!(updated.relayer);
    assert_eq!(accepted.oracle.as_deref(), Some(assigned.oracle.as_str()));
    assert_eq!(accepted.oracle_assigned, Some(true));
    assert_eq!(accepted.oracle_assigned_fee, Some(11));
    assert_eq!(accepted.relayer.as_deref(), Some(assigned.relayer.as_str()));
    assert_eq!(accepted.relayer_assigned, Some(true));
    assert_eq!(accepted.relayer_assigned_fee, Some(22));
}

#[test]
fn test_msgport_sent_and_recv_preserve_event_metadata() {
    let sent = MsgportMessageSentRow::from_event(LegacyOrmPEvent::MsgportMessageSent {
        metadata: evm_metadata("sent-log"),
        msg_id: "0xmsgid".to_owned(),
        from_dapp: "0xfromdapp".to_owned(),
        to_chain_id: 728_126_428,
        to_dapp: "0xtodapp".to_owned(),
        message: "0xmessage".to_owned(),
        params: "0xparams".to_owned(),
    });
    let recv = MsgportMessageRecvRow::from_event(LegacyOrmPEvent::MsgportMessageRecv {
        metadata: tron_metadata("recv-log"),
        msg_id: "0xmsgid".to_owned(),
        result: true,
        return_data: "0xreturn".to_owned(),
    });

    assert_eq!(sent.id, "sent-log");
    assert_eq!(sent.from_chain_id, sent.chain_id);
    assert_eq!(sent.transaction_index, 2);
    assert_eq!(sent.log_index, 3);
    assert_eq!(sent.transaction_from.as_deref(), Some("0xsender"));

    assert_eq!(recv.id, "recv-log");
    assert_eq!(recv.transaction_index, 9);
    assert_eq!(recv.log_index, 5);
    assert_eq!(recv.port_address, "0xtronport");
}

#[test]
fn test_signature_submittion_uses_event_id_and_event_payload_fields() {
    let row =
        SignaturePubSignatureSubmittionRow::from_event(LegacyOrmPEvent::SignatureSubmittion {
            metadata: evm_metadata("sig-log"),
            chain_id: 46,
            channel: "0xchannel".to_owned(),
            signer: "0xsigner".to_owned(),
            msg_index: 99,
            signature: "0xsig".to_owned(),
            data: "0xdata".to_owned(),
        });

    assert_eq!(row.id, "sig-log");
    assert_eq!(row.block_number, 123);
    assert_eq!(row.transaction_hash, "0xtx");
    assert_eq!(row.chain_id, 46);
    assert_eq!(row.msg_index, 99);
    assert_eq!(row.signature, "0xsig");
}

#[test]
fn test_schema_contract_lists_legacy_tables_and_metadata_gaps() {
    let tables = LegacySchema::tables();
    let names = tables
        .iter()
        .map(|table| table.table_name)
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "ormp_hash_imported",
            "ormp_message_accepted",
            "ormp_message_assigned",
            "ormp_message_dispatched",
            "msgport_message_recv",
            "msgport_message_sent",
            "signature_pub_signature_submittion",
        ]
    );
    assert!(
        tables
            .iter()
            .find(|table| table.table_name == "ormp_message_accepted")
            .expect("accepted table")
            .columns
            .iter()
            .any(|column| column.name == "oracle_assigned_fee" && column.nullable)
    );
    assert!(
        ormpindexer::schema::POSTGRES_SCHEMA_MIGRATION
            .contains("CREATE TABLE IF NOT EXISTS ormp_message_accepted")
    );
    assert_eq!(
        EventSource::Tron
            .transaction_from_source()
            .expect("tron source is documented"),
        "transaction.internalTransactions[logIndex].callerAddress"
    );
}

fn evm_metadata(id: &str) -> ChainLogMetadata {
    ChainLogMetadata {
        id: id.to_owned(),
        source: EventSource::Evm,
        chain_id: 46,
        block_number: 123,
        block_timestamp: 456,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 2,
        log_index: 3,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    }
}

fn tron_metadata(id: &str) -> ChainLogMetadata {
    ChainLogMetadata {
        id: id.to_owned(),
        source: EventSource::Tron,
        chain_id: 728_126_428,
        block_number: 987,
        block_timestamp: 654,
        transaction_hash: "0xtrontx".to_owned(),
        transaction_index: 9,
        log_index: 5,
        contract_address: "0xtronport".to_owned(),
        transaction_from: None,
    }
}
