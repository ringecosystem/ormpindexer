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

const LEGACY_B49E_ORACLE: &str = "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e";

#[test]
fn test_legacy_b49e_oracle_backfills_ethereum_to_darwinia_after_cutover() {
    let mut accepted = OrmpMessageAcceptedRow::from_event(LegacyOrmPEvent::MessageAccepted {
        metadata: evm_metadata("accepted-log"),
        msg_hash: "0x01e4449fa917170d8f95cbefd3c854a04e503b5ba13485c3e2278087f88e3373".to_owned(),
        channel: "0xchannel".to_owned(),
        index: 8,
        from_chain_id: 1,
        from: "0xfrom".to_owned(),
        to_chain_id: 46,
        to: "0xto".to_owned(),
        gas_limit: 500_000,
        encoded: "0xencoded".to_owned(),
    });
    accepted.chain_id = 1;
    accepted.block_number = 23_738_310;
    let assigned = OrmpMessageAssignedRow::from_event(LegacyOrmPEvent::MessageAssigned {
        metadata: evm_metadata("assigned-log"),
        msg_hash: accepted.id.clone(),
        oracle: LEGACY_B49E_ORACLE.to_owned(),
        relayer: "0x0000000000000000000000000000000000000001".to_owned(),
        oracle_fee: 2_000_000_000_000,
        relayer_fee: 22,
        params: "0xparams".to_owned(),
    });

    let updated = apply_assignment_to_accepted(
        &mut accepted,
        &assigned,
        &AssignmentConfig::legacy_defaults(),
    );

    assert!(updated.oracle);
    assert!(!updated.relayer);
    assert_eq!(accepted.oracle.as_deref(), Some(LEGACY_B49E_ORACLE));
    assert_eq!(accepted.oracle_assigned, Some(true));
    assert_eq!(accepted.oracle_assigned_fee, Some(2_000_000_000_000));
}

#[test]
fn test_legacy_b49e_oracle_does_not_backfill_outside_ethereum_to_darwinia_cutover() {
    for (chain_id, from_chain_id, to_chain_id, block_number) in [
        (1, 1, 42_161, 22_336_887),
        (1, 1, 46, 22_363_073),
        (46, 46, 1, 23_738_310),
    ] {
        let mut accepted = OrmpMessageAcceptedRow::from_event(LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata("accepted-log"),
            msg_hash: "0xmsg".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 8,
            from_chain_id,
            from: "0xfrom".to_owned(),
            to_chain_id,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        });
        accepted.chain_id = chain_id;
        accepted.block_number = block_number;
        let assigned = OrmpMessageAssignedRow::from_event(LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("assigned-log"),
            msg_hash: "0xmsg".to_owned(),
            oracle: LEGACY_B49E_ORACLE.to_owned(),
            relayer: "0x0000000000000000000000000000000000000001".to_owned(),
            oracle_fee: 11,
            relayer_fee: 22,
            params: "0xparams".to_owned(),
        });

        let updated = apply_assignment_to_accepted(
            &mut accepted,
            &assigned,
            &AssignmentConfig::legacy_defaults(),
        );

        assert!(
            !updated.oracle,
            "unexpected b49e oracle backfill for chain {chain_id}, from {from_chain_id}, to {to_chain_id}, block {block_number}"
        );
        assert_eq!(accepted.oracle, None);
        assert_eq!(accepted.oracle_assigned, None);
        assert_eq!(accepted.oracle_assigned_fee, None);
    }
}

#[test]
fn test_unverified_oracles_do_not_backfill_unconditionally() {
    for oracle in [
        "0xbe01b76ab454ae2497ae43168b1f70c92ac1c726",
        "0xd250c974cbe8eea25ab75c0fc9a18d612ae4b043",
        "0x985bddbc7e66964f131e3161ba8864f481cbcb2d",
    ] {
        let mut accepted = OrmpMessageAcceptedRow::from_event(LegacyOrmPEvent::MessageAccepted {
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
        });
        let assigned = OrmpMessageAssignedRow::from_event(LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("assigned-log"),
            msg_hash: "0xmsg".to_owned(),
            oracle: oracle.to_owned(),
            relayer: "0x0000000000000000000000000000000000000001".to_owned(),
            oracle_fee: 11,
            relayer_fee: 22,
            params: "0xparams".to_owned(),
        });

        let updated = apply_assignment_to_accepted(
            &mut accepted,
            &assigned,
            &AssignmentConfig::legacy_defaults(),
        );

        assert!(!updated.oracle, "unexpected oracle backfill for {oracle}");
        assert_eq!(accepted.oracle, None);
        assert_eq!(accepted.oracle_assigned, None);
        assert_eq!(accepted.oracle_assigned_fee, None);
    }
}

#[test]
fn test_msgport_sent_and_recv_preserve_event_metadata() {
    let sent_metadata = ChainLogMetadata {
        id: "42161-466386813-0x0f1e4961852aada25e6fda15c4883c7512e2f14c584e0d0917b03d9758682a57-14"
            .to_owned(),
        source: EventSource::Evm,
        chain_id: 42161,
        block_number: 466_386_813,
        block_hash: Some(
            "0x5bcb00ac00000000000000000000000000000000000000000000000000000000".to_owned(),
        ),
        block_timestamp: 456,
        transaction_hash: "0x0f1e4961852aada25e6fda15c4883c7512e2f14c584e0d0917b03d9758682a57"
            .to_owned(),
        transaction_index: 2,
        log_index: 14,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    };
    let recv_metadata = ChainLogMetadata {
        id: "728126428-9989983-trontx6b95f-4".to_owned(),
        source: EventSource::Tron,
        chain_id: 728_126_428,
        block_number: 9_989_983,
        block_hash: Some(
            "0x6b95f000000000000000000000000000000000000000000000000000000000000".to_owned(),
        ),
        block_timestamp: 654,
        transaction_hash: "0x368465dbe681b2cbdd7d16e8de578f12b6e30cd604aef11a28f0fccbccae169a"
            .to_owned(),
        transaction_index: 9,
        log_index: 4,
        contract_address: "0xtronport".to_owned(),
        transaction_from: None,
    };
    let sent = MsgportMessageSentRow::from_event(LegacyOrmPEvent::MsgportMessageSent {
        metadata: sent_metadata,
        msg_id: "0xmsgid".to_owned(),
        from_dapp: "0xfromdapp".to_owned(),
        to_chain_id: 728_126_428,
        to_dapp: "0xtodapp".to_owned(),
        message: "0xmessage".to_owned(),
        params: "0xparams".to_owned(),
    });
    let recv = MsgportMessageRecvRow::from_event(LegacyOrmPEvent::MsgportMessageRecv {
        metadata: recv_metadata,
        msg_id: "0xmsgid".to_owned(),
        result: true,
        return_data: "0xreturn".to_owned(),
    });

    assert_eq!(sent.id, "0466386813-5bcb0-000014");
    assert_eq!(sent.from_chain_id, sent.chain_id);
    assert_eq!(sent.transaction_index, 2);
    assert_eq!(sent.log_index, 14);
    assert_eq!(sent.transaction_from.as_deref(), Some("0xsender"));

    assert_eq!(recv.id, "0009989983-6b95f-000004");
    assert_eq!(recv.transaction_index, 9);
    assert_eq!(recv.log_index, 4);
    assert_eq!(recv.port_address, "0xtronport");
}

#[test]
fn test_signature_submittion_uses_legacy_event_id_and_event_payload_fields() {
    let row =
        SignaturePubSignatureSubmittionRow::from_event(LegacyOrmPEvent::SignatureSubmittion {
            metadata: ChainLogMetadata {
                id: "46-12054798-0x2a8dcfc8999ca0173b3e27b850e5cfe81d02f4ef6db4501221b384f88a46de65-1"
                    .to_owned(),
                source: EventSource::Evm,
                chain_id: 46,
                block_number: 12_054_798,
                block_hash: Some(
                    "0x6de65e4800000000000000000000000000000000000000000000000000000000"
                        .to_owned(),
                ),
                block_timestamp: 456,
                transaction_hash:
                    "0x2a8dcfc8999ca0173b3e27b850e5cfe81d02f4ef6db4501221b384f88a4bd947"
                        .to_owned(),
                transaction_index: 2,
                log_index: 1,
                contract_address: "0xport".to_owned(),
                transaction_from: Some("0xsender".to_owned()),
            },
            chain_id: 46,
            channel: "0xchannel".to_owned(),
            signer: "0xsigner".to_owned(),
            msg_index: 99,
            signature: "0xsig".to_owned(),
            data: "0xdata".to_owned(),
        });

    assert_eq!(row.id, "0012054798-6de65-000001");
    assert_eq!(row.block_number, 12_054_798);
    assert_eq!(
        row.transaction_hash,
        "0x2a8dcfc8999ca0173b3e27b850e5cfe81d02f4ef6db4501221b384f88a4bd947"
    );
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
        block_hash: None,
        block_timestamp: 456,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 2,
        log_index: 3,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    }
}
