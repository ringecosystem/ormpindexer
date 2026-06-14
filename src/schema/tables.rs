use super::types::{LegacyColumn, LegacyColumnType, LegacyIdRule, LegacyTable};

pub struct LegacySchema;

impl LegacySchema {
    pub fn tables() -> &'static [LegacyTable] {
        LEGACY_TABLES
    }
}

pub const POSTGRES_SCHEMA_MIGRATION: &str = include_str!("../../migrations/0001_schema_compat.sql");

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
