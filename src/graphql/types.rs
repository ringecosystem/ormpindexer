#![allow(clippy::upper_case_acronyms)]

use async_graphql::{
    Enum, InputObject, InputValueError, InputValueResult, Scalar, ScalarType, SimpleObject, Value,
};
use sqlx::{
    Decode, FromRow, Postgres, Type,
    encode::IsNull,
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BigInt(String);

impl BigInt {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for BigInt {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for BigInt {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[Scalar(name = "BigInt")]
impl ScalarType for BigInt {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::Number(value) => Ok(Self(value.to_string())),
            Value::String(value) => {
                value
                    .parse::<i128>()
                    .map_err(|_| InputValueError::custom("BigInt must be an integer"))?;
                Ok(Self(value))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

impl Type<Postgres> for BigInt {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <String as Type<Postgres>>::compatible(ty)
    }
}

impl<'r> Decode<'r, Postgres> for BigInt {
    fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<String as Decode<Postgres>>::decode(value)?))
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for BigInt {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync + 'static>> {
        <String as sqlx::Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[derive(Clone, Debug, Default, InputObject)]
pub struct LegacyWhereInput {
    #[graphql(name = "AND")]
    pub and: Option<Vec<LegacyWhereInput>>,
    #[graphql(name = "OR")]
    pub or: Option<Vec<LegacyWhereInput>>,
    #[graphql(name = "id_eq")]
    pub id_eq: Option<String>,
    #[graphql(name = "id_not_eq")]
    pub id_not_eq: Option<String>,
    #[graphql(name = "id_in")]
    pub id_in: Option<Vec<String>>,
    #[graphql(name = "id_not_in")]
    pub id_not_in: Option<Vec<String>>,
    #[graphql(name = "blockNumber_eq")]
    pub block_number_eq: Option<BigInt>,
    #[graphql(name = "blockNumber_gt")]
    pub block_number_gt: Option<BigInt>,
    #[graphql(name = "blockNumber_gte")]
    pub block_number_gte: Option<BigInt>,
    #[graphql(name = "blockNumber_lt")]
    pub block_number_lt: Option<BigInt>,
    #[graphql(name = "blockNumber_lte")]
    pub block_number_lte: Option<BigInt>,
    #[graphql(name = "blockNumber_in")]
    pub block_number_in: Option<Vec<BigInt>>,
    #[graphql(name = "transactionHash_eq")]
    pub transaction_hash_eq: Option<String>,
    #[graphql(name = "transactionHash_in")]
    pub transaction_hash_in: Option<Vec<String>>,
    #[graphql(name = "blockTimestamp_eq")]
    pub block_timestamp_eq: Option<BigInt>,
    #[graphql(name = "blockTimestamp_gt")]
    pub block_timestamp_gt: Option<BigInt>,
    #[graphql(name = "blockTimestamp_gte")]
    pub block_timestamp_gte: Option<BigInt>,
    #[graphql(name = "blockTimestamp_lt")]
    pub block_timestamp_lt: Option<BigInt>,
    #[graphql(name = "blockTimestamp_lte")]
    pub block_timestamp_lte: Option<BigInt>,
    #[graphql(name = "chainId_eq")]
    pub chain_id_eq: Option<BigInt>,
    #[graphql(name = "chainId_in")]
    pub chain_id_in: Option<Vec<BigInt>>,
    #[graphql(name = "logIndex_eq")]
    pub log_index_eq: Option<i32>,
    #[graphql(name = "logIndex_gt")]
    pub log_index_gt: Option<i32>,
    #[graphql(name = "logIndex_gte")]
    pub log_index_gte: Option<i32>,
    #[graphql(name = "logIndex_lt")]
    pub log_index_lt: Option<i32>,
    #[graphql(name = "logIndex_lte")]
    pub log_index_lte: Option<i32>,
    #[graphql(name = "transactionIndex_eq")]
    pub transaction_index_eq: Option<i32>,
    #[graphql(name = "msgHash_eq")]
    pub msg_hash_eq: Option<String>,
    #[graphql(name = "msgHash_in")]
    pub msg_hash_in: Option<Vec<String>>,
    #[graphql(name = "msgId_eq")]
    pub msg_id_eq: Option<String>,
    #[graphql(name = "msgId_in")]
    pub msg_id_in: Option<Vec<String>>,
    #[graphql(name = "hash_eq")]
    pub hash_eq: Option<String>,
    #[graphql(name = "channel_eq")]
    pub channel_eq: Option<String>,
    #[graphql(name = "oracle_eq")]
    pub oracle_eq: Option<String>,
    #[graphql(name = "relayer_eq")]
    pub relayer_eq: Option<String>,
    #[graphql(name = "signer_eq")]
    pub signer_eq: Option<String>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "signer_in")]
    pub signer_in: Option<Vec<String>>,
    #[graphql(name = "portAddress_eq")]
    pub port_address_eq: Option<String>,
    #[graphql(name = "from_eq")]
    pub from_eq: Option<String>,
    #[graphql(name = "to_eq")]
    pub to_eq: Option<String>,
    #[graphql(name = "fromDapp_eq")]
    pub from_dapp_eq: Option<String>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "fromDapp_in")]
    pub from_dapp_in: Option<Vec<String>>,
    #[graphql(name = "toDapp_eq")]
    pub to_dapp_eq: Option<String>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "toDapp_in")]
    pub to_dapp_in: Option<Vec<String>>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "transactionFrom_eq")]
    pub transaction_from_eq: Option<String>,
    #[graphql(name = "fromChainId_eq")]
    pub from_chain_id_eq: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "fromChainId_in")]
    pub from_chain_id_in: Option<Vec<BigInt>>,
    #[graphql(name = "toChainId_eq")]
    pub to_chain_id_eq: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "toChainId_in")]
    pub to_chain_id_in: Option<Vec<BigInt>>,
    #[graphql(name = "srcChainId_eq")]
    pub src_chain_id_eq: Option<BigInt>,
    #[graphql(name = "targetChainId_eq")]
    pub target_chain_id_eq: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "targetChainId_in")]
    pub target_chain_id_in: Option<Vec<BigInt>>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "dispatchResult_eq")]
    pub dispatch_result_eq: Option<bool>,
    #[graphql(name = "msgIndex_eq")]
    pub msg_index_eq: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "msgIndex_gt")]
    pub msg_index_gt: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "msgIndex_gte")]
    pub msg_index_gte: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "msgIndex_lt")]
    pub msg_index_lt: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "msgIndex_lte")]
    pub msg_index_lte: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_eq")]
    pub index_eq: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_gt")]
    pub index_gt: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_gte")]
    pub index_gte: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_lt")]
    pub index_lt: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_lte")]
    pub index_lte: Option<BigInt>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "oracleAssigned_eq")]
    pub oracle_assigned_eq: Option<bool>,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "relayerAssigned_eq")]
    pub relayer_assigned_eq: Option<bool>,
}

macro_rules! legacy_where_alias {
    ($name:ident, $graphql_name:literal) => {
        #[derive(Clone, Debug, Default, InputObject)]
        #[graphql(name = $graphql_name)]
        pub struct $name {
            #[graphql(flatten)]
            pub legacy: LegacyWhereInput,
        }

        impl From<$name> for LegacyWhereInput {
            fn from(value: $name) -> Self {
                value.legacy
            }
        }
    };
}

legacy_where_alias!(MsgportMessageSentWhereInput, "MsgportMessageSentWhereInput");
legacy_where_alias!(
    ORMPMessageAcceptedWhereInput,
    "ORMPMessageAcceptedWhereInput"
);
legacy_where_alias!(
    ORMPMessageDispatchedWhereInput,
    "ORMPMessageDispatchedWhereInput"
);

#[derive(Clone, Copy, Debug, Eq, Enum, PartialEq)]
pub enum LegacyOrderByInput {
    #[graphql(name = "id_ASC")]
    IdAsc,
    #[graphql(name = "id_DESC")]
    IdDesc,
    #[graphql(name = "blockNumber_ASC")]
    BlockNumberAsc,
    #[graphql(name = "blockNumber_DESC")]
    BlockNumberDesc,
    #[graphql(name = "blockTimestamp_ASC")]
    BlockTimestampAsc,
    #[graphql(name = "blockTimestamp_DESC")]
    BlockTimestampDesc,
    #[graphql(name = "chainId_ASC")]
    ChainIdAsc,
    #[graphql(name = "chainId_DESC")]
    ChainIdDesc,
    #[graphql(name = "logIndex_ASC")]
    LogIndexAsc,
    #[graphql(name = "logIndex_DESC")]
    LogIndexDesc,
    #[graphql(name = "transactionIndex_ASC")]
    TransactionIndexAsc,
    #[graphql(name = "transactionIndex_DESC")]
    TransactionIndexDesc,
    #[graphql(name = "msgHash_ASC")]
    MsgHashAsc,
    #[graphql(name = "msgHash_DESC")]
    MsgHashDesc,
    #[graphql(name = "msgId_ASC")]
    MsgIdAsc,
    #[graphql(name = "msgId_DESC")]
    MsgIdDesc,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_ASC")]
    IndexAsc,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "index_DESC")]
    IndexDesc,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "msgIndex_ASC")]
    MsgIndexAsc,
    #[cfg(feature = "legacy-query-compat")]
    #[graphql(name = "msgIndex_DESC")]
    MsgIndexDesc,
}

macro_rules! legacy_order_alias {
    ($name:ident, $graphql_name:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Enum, PartialEq)]
        #[graphql(name = $graphql_name)]
        pub enum $name {
            #[graphql(name = "id_ASC")]
            IdAsc,
            #[graphql(name = "id_DESC")]
            IdDesc,
            #[graphql(name = "blockNumber_ASC")]
            BlockNumberAsc,
            #[graphql(name = "blockNumber_DESC")]
            BlockNumberDesc,
            #[graphql(name = "blockTimestamp_ASC")]
            BlockTimestampAsc,
            #[graphql(name = "blockTimestamp_DESC")]
            BlockTimestampDesc,
            #[graphql(name = "chainId_ASC")]
            ChainIdAsc,
            #[graphql(name = "chainId_DESC")]
            ChainIdDesc,
            #[graphql(name = "logIndex_ASC")]
            LogIndexAsc,
            #[graphql(name = "logIndex_DESC")]
            LogIndexDesc,
            #[graphql(name = "transactionIndex_ASC")]
            TransactionIndexAsc,
            #[graphql(name = "transactionIndex_DESC")]
            TransactionIndexDesc,
            #[graphql(name = "msgHash_ASC")]
            MsgHashAsc,
            #[graphql(name = "msgHash_DESC")]
            MsgHashDesc,
            #[graphql(name = "msgId_ASC")]
            MsgIdAsc,
            #[graphql(name = "msgId_DESC")]
            MsgIdDesc,
            #[cfg(feature = "legacy-query-compat")]
            #[graphql(name = "index_ASC")]
            IndexAsc,
            #[cfg(feature = "legacy-query-compat")]
            #[graphql(name = "index_DESC")]
            IndexDesc,
            #[cfg(feature = "legacy-query-compat")]
            #[graphql(name = "msgIndex_ASC")]
            MsgIndexAsc,
            #[cfg(feature = "legacy-query-compat")]
            #[graphql(name = "msgIndex_DESC")]
            MsgIndexDesc,
        }

        impl From<$name> for LegacyOrderByInput {
            fn from(value: $name) -> Self {
                match value {
                    $name::IdAsc => Self::IdAsc,
                    $name::IdDesc => Self::IdDesc,
                    $name::BlockNumberAsc => Self::BlockNumberAsc,
                    $name::BlockNumberDesc => Self::BlockNumberDesc,
                    $name::BlockTimestampAsc => Self::BlockTimestampAsc,
                    $name::BlockTimestampDesc => Self::BlockTimestampDesc,
                    $name::ChainIdAsc => Self::ChainIdAsc,
                    $name::ChainIdDesc => Self::ChainIdDesc,
                    $name::LogIndexAsc => Self::LogIndexAsc,
                    $name::LogIndexDesc => Self::LogIndexDesc,
                    $name::TransactionIndexAsc => Self::TransactionIndexAsc,
                    $name::TransactionIndexDesc => Self::TransactionIndexDesc,
                    $name::MsgHashAsc => Self::MsgHashAsc,
                    $name::MsgHashDesc => Self::MsgHashDesc,
                    $name::MsgIdAsc => Self::MsgIdAsc,
                    $name::MsgIdDesc => Self::MsgIdDesc,
                    #[cfg(feature = "legacy-query-compat")]
                    $name::IndexAsc => Self::IndexAsc,
                    #[cfg(feature = "legacy-query-compat")]
                    $name::IndexDesc => Self::IndexDesc,
                    #[cfg(feature = "legacy-query-compat")]
                    $name::MsgIndexAsc => Self::MsgIndexAsc,
                    #[cfg(feature = "legacy-query-compat")]
                    $name::MsgIndexDesc => Self::MsgIndexDesc,
                }
            }
        }
    };
}

legacy_order_alias!(
    MsgportMessageSentOrderByInput,
    "MsgportMessageSentOrderByInput"
);
legacy_order_alias!(
    ORMPMessageAcceptedOrderByInput,
    "ORMPMessageAcceptedOrderByInput"
);
legacy_order_alias!(
    ORMPMessageDispatchedOrderByInput,
    "ORMPMessageDispatchedOrderByInput"
);

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(name = "ORMPHashImported", rename_fields = "camelCase")]
pub struct ORMPHashImported {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) chain_id: BigInt,
    pub(super) src_chain_id: BigInt,
    pub(super) target_chain_id: BigInt,
    pub(super) oracle: String,
    pub(super) channel: String,
    pub(super) msg_index: BigInt,
    pub(super) hash: String,
}

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(name = "ORMPMessageAccepted", rename_fields = "camelCase")]
pub struct ORMPMessageAccepted {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) chain_id: BigInt,
    pub(super) log_index: i32,
    pub(super) msg_hash: String,
    pub(super) channel: String,
    pub(super) index: BigInt,
    pub(super) from_chain_id: BigInt,
    pub(super) from: String,
    pub(super) to_chain_id: BigInt,
    pub(super) to: String,
    pub(super) gas_limit: BigInt,
    pub(super) encoded: String,
    pub(super) oracle: Option<String>,
    pub(super) oracle_assigned: Option<bool>,
    pub(super) oracle_assigned_fee: Option<BigInt>,
    pub(super) relayer: Option<String>,
    pub(super) relayer_assigned: Option<bool>,
    pub(super) relayer_assigned_fee: Option<BigInt>,
}

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(name = "ORMPMessageAssigned", rename_fields = "camelCase")]
pub struct ORMPMessageAssigned {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) chain_id: BigInt,
    pub(super) msg_hash: String,
    pub(super) oracle: String,
    pub(super) relayer: String,
    pub(super) oracle_fee: BigInt,
    pub(super) relayer_fee: BigInt,
    pub(super) params: String,
}

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(name = "ORMPMessageDispatched", rename_fields = "camelCase")]
pub struct ORMPMessageDispatched {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) chain_id: BigInt,
    pub(super) target_chain_id: BigInt,
    pub(super) msg_hash: String,
    pub(super) dispatch_result: bool,
}

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct MsgportMessageRecv {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) transaction_index: i32,
    pub(super) log_index: i32,
    pub(super) chain_id: BigInt,
    pub(super) port_address: String,
    pub(super) msg_id: String,
    pub(super) result: bool,
    pub(super) return_data: String,
}

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct MsgportMessageSent {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) transaction_index: i32,
    pub(super) log_index: i32,
    pub(super) chain_id: BigInt,
    pub(super) port_address: String,
    pub(super) transaction_from: Option<String>,
    pub(super) from_chain_id: BigInt,
    pub(super) msg_id: String,
    pub(super) from_dapp: String,
    pub(super) to_chain_id: BigInt,
    pub(super) to_dapp: String,
    pub(super) message: String,
    pub(super) params: String,
}

#[derive(Clone, Debug, FromRow, SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct SignaturePubSignatureSubmittion {
    pub(super) id: String,
    pub(super) block_number: BigInt,
    pub(super) transaction_hash: String,
    pub(super) block_timestamp: BigInt,
    pub(super) chain_id: BigInt,
    pub(super) channel: String,
    pub(super) signer: String,
    pub(super) msg_index: BigInt,
    pub(super) signature: String,
    pub(super) data: String,
}

macro_rules! page_type {
    ($name:ident, $graphql_name:literal, $item:ty) => {
        #[derive(Clone, Debug, SimpleObject)]
        #[graphql(name = $graphql_name, rename_fields = "camelCase")]
        pub struct $name {
            pub(super) total_count: i64,
            pub(super) offset: i32,
            pub(super) limit: i32,
            pub(super) items: Vec<$item>,
        }

        impl $name {
            pub(super) fn new(
                total_count: i64,
                offset: i32,
                limit: i32,
                items: Vec<$item>,
            ) -> Self {
                Self {
                    total_count,
                    offset,
                    limit,
                    items,
                }
            }
        }
    };
}

page_type!(
    ORMPHashImportedPage,
    "ORMPHashImportedPage",
    ORMPHashImported
);
page_type!(
    ORMPMessageAcceptedPage,
    "ORMPMessageAcceptedPage",
    ORMPMessageAccepted
);
page_type!(
    ORMPMessageAssignedPage,
    "ORMPMessageAssignedPage",
    ORMPMessageAssigned
);
page_type!(
    ORMPMessageDispatchedPage,
    "ORMPMessageDispatchedPage",
    ORMPMessageDispatched
);
page_type!(
    MsgportMessageRecvPage,
    "MsgportMessageRecvPage",
    MsgportMessageRecv
);
page_type!(
    MsgportMessageSentPage,
    "MsgportMessageSentPage",
    MsgportMessageSent
);
page_type!(
    SignaturePubSignatureSubmittionPage,
    "SignaturePubSignatureSubmittionPage",
    SignaturePubSignatureSubmittion
);

#[cfg(feature = "legacy-query-compat")]
#[derive(Clone, Debug, SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct LegacyTotalCountConnection {
    pub(super) total_count: i64,
}

#[cfg(feature = "legacy-query-compat")]
impl LegacyTotalCountConnection {
    pub(super) fn new(total_count: i64) -> Self {
        Self { total_count }
    }
}
