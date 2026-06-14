use super::types::{LegacyOrmPEvent, legacy_event_log_id};

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
