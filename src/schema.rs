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

fn legacy_event_log_id(metadata: &ChainLogMetadata) -> String {
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

pub const ADDRESS_RELAYER: &[&str] = &[
    "0x114890eb7386f94eae410186f20968bfaf66142a",
    "0xb607762f43f1a72593715497d4a7ddd754c62a6a",
];

pub const ADDRESS_ORACLE: &[&str] = &[
    "0x8d8a2bd991c1d900c59a82a2eeb0df44e0671aab",
    "0x2cdc7178013de451ed99607ac15def6bab8c37e6",
    "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentConfig {
    pub oracle_addresses: Vec<String>,
    pub relayer_addresses: Vec<String>,
}

impl AssignmentConfig {
    pub fn legacy_defaults() -> Self {
        Self {
            oracle_addresses: ADDRESS_ORACLE
                .iter()
                .map(|address| address.to_string())
                .collect(),
            relayer_addresses: ADDRESS_RELAYER
                .iter()
                .map(|address| address.to_string())
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AssignmentUpdate {
    pub oracle: bool,
    pub relayer: bool,
}

pub fn apply_assignment_to_accepted(
    accepted: &mut OrmpMessageAcceptedRow,
    assigned: &OrmpMessageAssignedRow,
    config: &AssignmentConfig,
) -> AssignmentUpdate {
    if accepted.id != assigned.msg_hash {
        return AssignmentUpdate::default();
    }

    let mut update = AssignmentUpdate::default();

    if contains_address(&config.relayer_addresses, &assigned.relayer) {
        accepted.relayer = Some(assigned.relayer.clone());
        accepted.relayer_assigned = Some(true);
        accepted.relayer_assigned_fee = Some(assigned.relayer_fee);
        update.relayer = true;
    }

    if contains_address(&config.oracle_addresses, &assigned.oracle) {
        accepted.oracle = Some(assigned.oracle.clone());
        accepted.oracle_assigned = Some(true);
        accepted.oracle_assigned_fee = Some(assigned.oracle_fee);
        update.oracle = true;
    }

    update
}

fn contains_address(addresses: &[String], candidate: &str) -> bool {
    let candidate = candidate.to_ascii_lowercase();
    addresses
        .iter()
        .any(|address| address.eq_ignore_ascii_case(&candidate))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpHashImportedRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub chain_id: u128,
    pub src_chain_id: u128,
    pub target_chain_id: u128,
    pub oracle: String,
    pub channel: String,
    pub msg_index: u128,
    pub hash: String,
}

impl OrmpHashImportedRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::HashImported {
                metadata,
                src_chain_id,
                target_chain_id,
                oracle,
                channel,
                msg_index,
                hash,
            } => Self {
                id: hash.clone(),
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                chain_id: metadata.chain_id,
                src_chain_id,
                target_chain_id,
                oracle,
                channel,
                msg_index,
                hash,
            },
            _ => panic!("expected HashImported event"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpMessageAcceptedRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub chain_id: u128,
    pub log_index: i32,
    pub msg_hash: String,
    pub channel: String,
    pub index: u128,
    pub from_chain_id: u128,
    pub from: String,
    pub to_chain_id: u128,
    pub to: String,
    pub gas_limit: u128,
    pub encoded: String,
    pub oracle: Option<String>,
    pub oracle_assigned: Option<bool>,
    pub oracle_assigned_fee: Option<u128>,
    pub relayer: Option<String>,
    pub relayer_assigned: Option<bool>,
    pub relayer_assigned_fee: Option<u128>,
}

impl OrmpMessageAcceptedRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::MessageAccepted {
                metadata,
                msg_hash,
                channel,
                index,
                from_chain_id,
                from,
                to_chain_id,
                to,
                gas_limit,
                encoded,
            } => Self {
                id: msg_hash.clone(),
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                chain_id: metadata.chain_id,
                log_index: metadata.log_index,
                msg_hash,
                channel,
                index,
                from_chain_id,
                from,
                to_chain_id,
                to,
                gas_limit,
                encoded,
                oracle: None,
                oracle_assigned: None,
                oracle_assigned_fee: None,
                relayer: None,
                relayer_assigned: None,
                relayer_assigned_fee: None,
            },
            _ => panic!("expected MessageAccepted event"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpMessageAssignedRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub chain_id: u128,
    pub msg_hash: String,
    pub oracle: String,
    pub relayer: String,
    pub oracle_fee: u128,
    pub relayer_fee: u128,
    pub params: String,
}

impl OrmpMessageAssignedRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::MessageAssigned {
                metadata,
                msg_hash,
                oracle,
                relayer,
                oracle_fee,
                relayer_fee,
                params,
            } => Self {
                id: metadata.id,
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                chain_id: metadata.chain_id,
                msg_hash,
                oracle,
                relayer,
                oracle_fee,
                relayer_fee,
                params,
            },
            _ => panic!("expected MessageAssigned event"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpMessageDispatchedRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub chain_id: u128,
    pub target_chain_id: u128,
    pub msg_hash: String,
    pub dispatch_result: bool,
}

impl OrmpMessageDispatchedRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::MessageDispatched {
                metadata,
                target_chain_id,
                msg_hash,
                dispatch_result,
            } => Self {
                id: msg_hash.clone(),
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                chain_id: metadata.chain_id,
                target_chain_id,
                msg_hash,
                dispatch_result,
            },
            _ => panic!("expected MessageDispatched event"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MsgportMessageRecvRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub transaction_index: i32,
    pub log_index: i32,
    pub chain_id: u128,
    pub port_address: String,
    pub msg_id: String,
    pub result: bool,
    pub return_data: String,
}

impl MsgportMessageRecvRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::MsgportMessageRecv {
                metadata,
                msg_id,
                result,
                return_data,
            } => Self {
                id: legacy_event_log_id(&metadata),
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                transaction_index: metadata.transaction_index,
                log_index: metadata.log_index,
                chain_id: metadata.chain_id,
                port_address: metadata.contract_address,
                msg_id,
                result,
                return_data,
            },
            _ => panic!("expected MsgportMessageRecv event"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MsgportMessageSentRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub transaction_index: i32,
    pub log_index: i32,
    pub chain_id: u128,
    pub port_address: String,
    pub transaction_from: Option<String>,
    pub from_chain_id: u128,
    pub msg_id: String,
    pub from_dapp: String,
    pub to_chain_id: u128,
    pub to_dapp: String,
    pub message: String,
    pub params: String,
}

impl MsgportMessageSentRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::MsgportMessageSent {
                metadata,
                msg_id,
                from_dapp,
                to_chain_id,
                to_dapp,
                message,
                params,
            } => Self {
                id: legacy_event_log_id(&metadata),
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                transaction_index: metadata.transaction_index,
                log_index: metadata.log_index,
                chain_id: metadata.chain_id,
                port_address: metadata.contract_address,
                transaction_from: metadata.transaction_from,
                from_chain_id: metadata.chain_id,
                msg_id,
                from_dapp,
                to_chain_id,
                to_dapp,
                message,
                params,
            },
            _ => panic!("expected MsgportMessageSent event"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignaturePubSignatureSubmittionRow {
    pub id: String,
    pub block_number: u128,
    pub transaction_hash: String,
    pub block_timestamp: u128,
    pub chain_id: u128,
    pub channel: String,
    pub signer: String,
    pub msg_index: u128,
    pub signature: String,
    pub data: String,
}

impl SignaturePubSignatureSubmittionRow {
    pub fn from_event(event: LegacyOrmPEvent) -> Self {
        match event {
            LegacyOrmPEvent::SignatureSubmittion {
                metadata,
                chain_id,
                channel,
                signer,
                msg_index,
                signature,
                data,
            } => Self {
                id: legacy_event_log_id(&metadata),
                block_number: metadata.block_number,
                transaction_hash: metadata.transaction_hash,
                block_timestamp: metadata.block_timestamp,
                chain_id,
                channel,
                signer,
                msg_index,
                signature,
                data,
            },
            _ => panic!("expected SignatureSubmittion event"),
        }
    }
}

pub struct LegacySchema;

impl LegacySchema {
    pub fn tables() -> &'static [LegacyTable] {
        LEGACY_TABLES
    }
}

pub const POSTGRES_SCHEMA_MIGRATION: &str = include_str!("../migrations/0001_schema_compat.sql");

const COMMON_COLUMNS: &[LegacyColumn] = &[
    column("id", "id", LegacyColumnType::Text, false),
    column(
        "block_number",
        "blockNumber",
        LegacyColumnType::Numeric,
        false,
    ),
    column(
        "transaction_hash",
        "transactionHash",
        LegacyColumnType::Text,
        false,
    ),
    column(
        "block_timestamp",
        "blockTimestamp",
        LegacyColumnType::Numeric,
        false,
    ),
    column("chain_id", "chainId", LegacyColumnType::Numeric, false),
];

const ORMP_HASH_IMPORTED_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    COMMON_COLUMNS[4],
    column(
        "src_chain_id",
        "srcChainId",
        LegacyColumnType::Numeric,
        false,
    ),
    column(
        "target_chain_id",
        "targetChainId",
        LegacyColumnType::Numeric,
        false,
    ),
    column("oracle", "oracle", LegacyColumnType::Text, false),
    column("channel", "channel", LegacyColumnType::Text, false),
    column("msg_index", "msgIndex", LegacyColumnType::Numeric, false),
    column("hash", "hash", LegacyColumnType::Text, false),
];

const ORMP_MESSAGE_ACCEPTED_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    COMMON_COLUMNS[4],
    column("log_index", "logIndex", LegacyColumnType::Integer, false),
    column("msg_hash", "msgHash", LegacyColumnType::Text, false),
    column("channel", "channel", LegacyColumnType::Text, false),
    column("index", "index", LegacyColumnType::Numeric, false),
    column(
        "from_chain_id",
        "fromChainId",
        LegacyColumnType::Numeric,
        false,
    ),
    column("from", "from", LegacyColumnType::Text, false),
    column("to_chain_id", "toChainId", LegacyColumnType::Numeric, false),
    column("to", "to", LegacyColumnType::Text, false),
    column("gas_limit", "gasLimit", LegacyColumnType::Numeric, false),
    column("encoded", "encoded", LegacyColumnType::Text, false),
    column("oracle", "oracle", LegacyColumnType::Text, true),
    column(
        "oracle_assigned",
        "oracleAssigned",
        LegacyColumnType::Boolean,
        true,
    ),
    column(
        "oracle_assigned_fee",
        "oracleAssignedFee",
        LegacyColumnType::Numeric,
        true,
    ),
    column("relayer", "relayer", LegacyColumnType::Text, true),
    column(
        "relayer_assigned",
        "relayerAssigned",
        LegacyColumnType::Boolean,
        true,
    ),
    column(
        "relayer_assigned_fee",
        "relayerAssignedFee",
        LegacyColumnType::Numeric,
        true,
    ),
];

const ORMP_MESSAGE_ASSIGNED_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    COMMON_COLUMNS[4],
    column("msg_hash", "msgHash", LegacyColumnType::Text, false),
    column("oracle", "oracle", LegacyColumnType::Text, false),
    column("relayer", "relayer", LegacyColumnType::Text, false),
    column("oracle_fee", "oracleFee", LegacyColumnType::Numeric, false),
    column(
        "relayer_fee",
        "relayerFee",
        LegacyColumnType::Numeric,
        false,
    ),
    column("params", "params", LegacyColumnType::Text, false),
];

const ORMP_MESSAGE_DISPATCHED_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    COMMON_COLUMNS[4],
    column(
        "target_chain_id",
        "targetChainId",
        LegacyColumnType::Numeric,
        false,
    ),
    column("msg_hash", "msgHash", LegacyColumnType::Text, false),
    column(
        "dispatch_result",
        "dispatchResult",
        LegacyColumnType::Boolean,
        false,
    ),
];

const MSGPORT_MESSAGE_RECV_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    column(
        "transaction_index",
        "transactionIndex",
        LegacyColumnType::Integer,
        false,
    ),
    column("log_index", "logIndex", LegacyColumnType::Integer, false),
    COMMON_COLUMNS[4],
    column("port_address", "portAddress", LegacyColumnType::Text, false),
    column("msg_id", "msgId", LegacyColumnType::Text, false),
    column("result", "result", LegacyColumnType::Boolean, false),
    column("return_data", "returnData", LegacyColumnType::Text, false),
];

const MSGPORT_MESSAGE_SENT_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    column(
        "transaction_index",
        "transactionIndex",
        LegacyColumnType::Integer,
        false,
    ),
    column("log_index", "logIndex", LegacyColumnType::Integer, false),
    COMMON_COLUMNS[4],
    column("port_address", "portAddress", LegacyColumnType::Text, false),
    column(
        "transaction_from",
        "transactionFrom",
        LegacyColumnType::Text,
        true,
    ),
    column(
        "from_chain_id",
        "fromChainId",
        LegacyColumnType::Numeric,
        false,
    ),
    column("msg_id", "msgId", LegacyColumnType::Text, false),
    column("from_dapp", "fromDapp", LegacyColumnType::Text, false),
    column("to_chain_id", "toChainId", LegacyColumnType::Numeric, false),
    column("to_dapp", "toDapp", LegacyColumnType::Text, false),
    column("message", "message", LegacyColumnType::Text, false),
    column("params", "params", LegacyColumnType::Text, false),
];

const SIGNATURE_PUB_SIGNATURE_SUBMITTION_COLUMNS: &[LegacyColumn] = &[
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
    COMMON_COLUMNS[2],
    COMMON_COLUMNS[3],
    COMMON_COLUMNS[4],
    column("channel", "channel", LegacyColumnType::Text, false),
    column("signer", "signer", LegacyColumnType::Text, false),
    column("msg_index", "msgIndex", LegacyColumnType::Numeric, false),
    column("signature", "signature", LegacyColumnType::Text, false),
    column("data", "data", LegacyColumnType::Text, false),
];

const LEGACY_TABLES: &[LegacyTable] = &[
    LegacyTable {
        table_name: "ormp_hash_imported",
        graphql_entity: "ORMPHashImported",
        id_rule: LegacyIdRule::HashField("hash"),
        columns: ORMP_HASH_IMPORTED_COLUMNS,
    },
    LegacyTable {
        table_name: "ormp_message_accepted",
        graphql_entity: "ORMPMessageAccepted",
        id_rule: LegacyIdRule::MessageHash,
        columns: ORMP_MESSAGE_ACCEPTED_COLUMNS,
    },
    LegacyTable {
        table_name: "ormp_message_assigned",
        graphql_entity: "ORMPMessageAssigned",
        id_rule: LegacyIdRule::EventId,
        columns: ORMP_MESSAGE_ASSIGNED_COLUMNS,
    },
    LegacyTable {
        table_name: "ormp_message_dispatched",
        graphql_entity: "ORMPMessageDispatched",
        id_rule: LegacyIdRule::MessageHash,
        columns: ORMP_MESSAGE_DISPATCHED_COLUMNS,
    },
    LegacyTable {
        table_name: "msgport_message_recv",
        graphql_entity: "MsgportMessageRecv",
        id_rule: LegacyIdRule::EventId,
        columns: MSGPORT_MESSAGE_RECV_COLUMNS,
    },
    LegacyTable {
        table_name: "msgport_message_sent",
        graphql_entity: "MsgportMessageSent",
        id_rule: LegacyIdRule::EventId,
        columns: MSGPORT_MESSAGE_SENT_COLUMNS,
    },
    LegacyTable {
        table_name: "signature_pub_signature_submittion",
        graphql_entity: "SignaturePubSignatureSubmittion",
        id_rule: LegacyIdRule::EventId,
        columns: SIGNATURE_PUB_SIGNATURE_SUBMITTION_COLUMNS,
    },
];

const fn column(
    name: &'static str,
    graphql_name: &'static str,
    column_type: LegacyColumnType,
    nullable: bool,
) -> LegacyColumn {
    LegacyColumn {
        name,
        graphql_name,
        column_type,
        nullable,
    }
}
