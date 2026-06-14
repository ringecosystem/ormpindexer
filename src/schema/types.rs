#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyColumnType {
    Text,
    Numeric,
    Integer,
    Boolean,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyColumn {
    pub name: &'static str,
    pub graphql_name: &'static str,
    pub column_type: LegacyColumnType,
    pub nullable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyTable {
    pub table_name: &'static str,
    pub graphql_entity: &'static str,
    pub id_rule: LegacyIdRule,
    pub columns: &'static [LegacyColumn],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyIdRule {
    EventId,
    HashField(&'static str),
    MessageHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventSource {
    Evm,
    Tron,
}

impl EventSource {
    pub fn transaction_from_source(self) -> Option<&'static str> {
        match self {
            Self::Evm => Some("transaction.from"),
            Self::Tron => Some("transaction.internalTransactions[logIndex].callerAddress"),
        }
    }

    pub fn event_index_source(self) -> &'static str {
        match self {
            Self::Evm => "log.logIndex",
            Self::Tron => "log.logIndex as eventIndex-compatible slot",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainLogMetadata {
    pub id: String,
    pub source: EventSource,
    pub chain_id: u128,
    pub block_number: u128,
    pub block_hash: Option<String>,
    pub block_timestamp: u128,
    pub transaction_hash: String,
    pub transaction_index: i32,
    pub log_index: i32,
    pub contract_address: String,
    pub transaction_from: Option<String>,
}

pub(super) fn legacy_event_log_id(metadata: &ChainLogMetadata) -> String {
    let Some(block_hash) = metadata.block_hash.as_deref() else {
        return metadata.id.clone();
    };
    let block_hash = block_hash
        .strip_prefix("0x")
        .or_else(|| block_hash.strip_prefix("0X"))
        .unwrap_or(block_hash);
    let block_hash_prefix = block_hash
        .get(..block_hash.len().min(5))
        .unwrap_or(block_hash)
        .to_ascii_lowercase();
    format!(
        "{:010}-{}-{:06}",
        metadata.block_number, block_hash_prefix, metadata.log_index
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LegacyOrmPEvent {
    HashImported {
        metadata: ChainLogMetadata,
        src_chain_id: u128,
        target_chain_id: u128,
        oracle: String,
        channel: String,
        msg_index: u128,
        hash: String,
    },
    MessageAccepted {
        metadata: ChainLogMetadata,
        msg_hash: String,
        channel: String,
        index: u128,
        from_chain_id: u128,
        from: String,
        to_chain_id: u128,
        to: String,
        gas_limit: u128,
        encoded: String,
    },
    MessageAssigned {
        metadata: ChainLogMetadata,
        msg_hash: String,
        oracle: String,
        relayer: String,
        oracle_fee: u128,
        relayer_fee: u128,
        params: String,
    },
    MessageDispatched {
        metadata: ChainLogMetadata,
        target_chain_id: u128,
        msg_hash: String,
        dispatch_result: bool,
    },
    MsgportMessageRecv {
        metadata: ChainLogMetadata,
        msg_id: String,
        result: bool,
        return_data: String,
    },
    MsgportMessageSent {
        metadata: ChainLogMetadata,
        msg_id: String,
        from_dapp: String,
        to_chain_id: u128,
        to_dapp: String,
        message: String,
        params: String,
    },
    SignatureSubmittion {
        metadata: ChainLogMetadata,
        chain_id: u128,
        channel: String,
        signer: String,
        msg_index: u128,
        signature: String,
        data: String,
    },
}
