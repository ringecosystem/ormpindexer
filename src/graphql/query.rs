use async_graphql::Result as GraphqlResult;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder};

use super::pagination::{page_args, push_page};
use super::types::*;

const ORMP_HASH_IMPORTED_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, chain_id::TEXT AS chain_id,
      src_chain_id::TEXT AS src_chain_id, target_chain_id::TEXT AS target_chain_id,
      oracle, channel, msg_index::TEXT AS msg_index, hash
    FROM ormp_hash_imported
"#;
const ORMP_MESSAGE_ACCEPTED_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, chain_id::TEXT AS chain_id,
      log_index, msg_hash, channel, "index"::TEXT AS "index",
      from_chain_id::TEXT AS from_chain_id, "from", to_chain_id::TEXT AS to_chain_id,
      "to", gas_limit::TEXT AS gas_limit, encoded, oracle, oracle_assigned,
      oracle_assigned_fee::TEXT AS oracle_assigned_fee, relayer, relayer_assigned,
      relayer_assigned_fee::TEXT AS relayer_assigned_fee
    FROM ormp_message_accepted
"#;
const ORMP_MESSAGE_ASSIGNED_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, chain_id::TEXT AS chain_id,
      msg_hash, oracle, relayer, oracle_fee::TEXT AS oracle_fee,
      relayer_fee::TEXT AS relayer_fee, params
    FROM ormp_message_assigned
"#;
const ORMP_MESSAGE_DISPATCHED_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, chain_id::TEXT AS chain_id,
      target_chain_id::TEXT AS target_chain_id, msg_hash, dispatch_result
    FROM ormp_message_dispatched
"#;
const MSGPORT_MESSAGE_RECV_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, transaction_index, log_index,
      chain_id::TEXT AS chain_id, port_address, msg_id, result, return_data
    FROM msgport_message_recv
"#;
const MSGPORT_MESSAGE_SENT_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, transaction_index, log_index,
      chain_id::TEXT AS chain_id, port_address, transaction_from,
      from_chain_id::TEXT AS from_chain_id, msg_id, from_dapp,
      to_chain_id::TEXT AS to_chain_id, to_dapp, message, params
    FROM msgport_message_sent
"#;
const SIGNATURE_SUBMITTION_SELECT: &str = r#"
    SELECT id, block_number::TEXT AS block_number, transaction_hash,
      block_timestamp::TEXT AS block_timestamp, chain_id::TEXT AS chain_id,
      channel, signer, msg_index::TEXT AS msg_index, signature, data
    FROM signature_pub_signature_submittion
"#;

macro_rules! query_fns {
    ($by_id:ident, $list:ident, $page:ident, $ty:ty, $page_ty:ty, $table:literal, $select:expr) => {
        pub(super) async fn $by_id(pool: &PgPool, id: String) -> GraphqlResult<Option<$ty>> {
            fetch_by_id(pool, $select, id).await
        }

        pub(super) async fn $list(
            pool: &PgPool,
            where_: Option<&LegacyWhereInput>,
            order_by: Option<&[LegacyOrderByInput]>,
            offset: Option<i32>,
            limit: Option<i32>,
        ) -> GraphqlResult<Vec<$ty>> {
            let (offset, limit) = page_args(offset, limit);
            if limit == 0 {
                return Ok(Vec::new());
            }
            fetch_page(pool, $select, $table, where_, order_by, offset, limit).await
        }

        pub(super) async fn $page(
            pool: &PgPool,
            where_: Option<&LegacyWhereInput>,
            order_by: Option<&[LegacyOrderByInput]>,
            offset: Option<i32>,
            limit: Option<i32>,
        ) -> GraphqlResult<$page_ty> {
            let (offset, limit) = page_args(offset, limit);
            let total_count = count_rows(pool, $table, where_).await?;
            let items = if limit == 0 {
                Vec::new()
            } else {
                fetch_page(pool, $select, $table, where_, order_by, offset, limit).await?
            };
            Ok(<$page_ty>::new(total_count, offset, limit, items))
        }
    };
}

query_fns!(
    query_ormp_hash_imported_by_id,
    query_ormp_hash_importeds,
    query_ormp_hash_importeds_page,
    ORMPHashImported,
    ORMPHashImportedPage,
    "ormp_hash_imported",
    ORMP_HASH_IMPORTED_SELECT
);
query_fns!(
    query_ormp_message_accepted_by_id,
    query_ormp_message_accepteds,
    query_ormp_message_accepteds_page,
    ORMPMessageAccepted,
    ORMPMessageAcceptedPage,
    "ormp_message_accepted",
    ORMP_MESSAGE_ACCEPTED_SELECT
);
query_fns!(
    query_ormp_message_assigned_by_id,
    query_ormp_message_assigneds,
    query_ormp_message_assigneds_page,
    ORMPMessageAssigned,
    ORMPMessageAssignedPage,
    "ormp_message_assigned",
    ORMP_MESSAGE_ASSIGNED_SELECT
);
query_fns!(
    query_ormp_message_dispatched_by_id,
    query_ormp_message_dispatcheds,
    query_ormp_message_dispatcheds_page,
    ORMPMessageDispatched,
    ORMPMessageDispatchedPage,
    "ormp_message_dispatched",
    ORMP_MESSAGE_DISPATCHED_SELECT
);
query_fns!(
    query_msgport_message_recv_by_id,
    query_msgport_message_recvs,
    query_msgport_message_recvs_page,
    MsgportMessageRecv,
    MsgportMessageRecvPage,
    "msgport_message_recv",
    MSGPORT_MESSAGE_RECV_SELECT
);
query_fns!(
    query_msgport_message_sent_by_id,
    query_msgport_message_sents,
    query_msgport_message_sents_page,
    MsgportMessageSent,
    MsgportMessageSentPage,
    "msgport_message_sent",
    MSGPORT_MESSAGE_SENT_SELECT
);
query_fns!(
    query_signature_pub_signature_submittion_by_id,
    query_signature_pub_signature_submittions,
    query_signature_pub_signature_submittions_page,
    SignaturePubSignatureSubmittion,
    SignaturePubSignatureSubmittionPage,
    "signature_pub_signature_submittion",
    SIGNATURE_SUBMITTION_SELECT
);

async fn fetch_by_id<T>(pool: &PgPool, select: &'static str, id: String) -> GraphqlResult<Option<T>>
where
    T: for<'row> FromRow<'row, sqlx::postgres::PgRow> + Send + Unpin,
{
    let mut query = QueryBuilder::<Postgres>::new(select);
    query.push(" WHERE id = ").push_bind(id);

    Ok(query.build_query_as().fetch_optional(pool).await?)
}

async fn fetch_page<T>(
    pool: &PgPool,
    select: &'static str,
    table: &'static str,
    where_: Option<&LegacyWhereInput>,
    order_by: Option<&[LegacyOrderByInput]>,
    offset: i32,
    limit: i32,
) -> GraphqlResult<Vec<T>>
where
    T: for<'row> FromRow<'row, sqlx::postgres::PgRow> + Send + Unpin,
{
    let mut query = QueryBuilder::<Postgres>::new(select);
    push_where(&mut query, where_);
    push_order_by(&mut query, table, order_by);
    push_page(&mut query, offset, limit);

    Ok(query.build_query_as().fetch_all(pool).await?)
}

async fn count_rows(
    pool: &PgPool,
    table: &'static str,
    where_: Option<&LegacyWhereInput>,
) -> GraphqlResult<i64> {
    let mut query =
        QueryBuilder::<Postgres>::new(format!("SELECT COUNT(*)::int8 AS total_count FROM {table}"));
    push_where(&mut query, where_);
    let (total_count,): (i64,) = query.build_query_as().fetch_one(pool).await?;
    Ok(total_count)
}

fn push_where<'a>(query: &mut QueryBuilder<'a, Postgres>, where_: Option<&'a LegacyWhereInput>) {
    let Some(where_) = where_ else {
        return;
    };
    query.push(" WHERE ");
    let mut has_condition = false;
    push_where_group(query, where_, &mut has_condition);
    if !has_condition {
        query.push(" TRUE");
    }
}

fn push_where_group<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    where_: &'a LegacyWhereInput,
    has_condition: &mut bool,
) {
    macro_rules! text_cmp {
        ($field:expr, $column:literal, $op:literal) => {
            if let Some(value) = $field.as_ref() {
                push_and(query, has_condition);
                query.push($column).push($op).push_bind(value);
            }
        };
    }
    macro_rules! text_in {
        ($field:expr, $column:literal, $negated:literal) => {
            if let Some(values) = $field.as_ref() {
                push_and(query, has_condition);
                query.push($column);
                if $negated {
                    query.push(" NOT");
                }
                query.push(" IN (");
                let mut separated = query.separated(", ");
                for value in values {
                    separated.push_bind(value);
                }
                separated.push_unseparated(")");
            }
        };
    }
    macro_rules! bigint_cmp {
        ($field:expr, $column:literal, $op:literal) => {
            if let Some(value) = $field.as_ref() {
                push_and(query, has_condition);
                query
                    .push($column)
                    .push($op)
                    .push_bind(value.as_str())
                    .push("::NUMERIC");
            }
        };
    }
    macro_rules! bigint_in {
        ($field:expr, $column:literal) => {
            if let Some(values) = $field.as_ref() {
                push_and(query, has_condition);
                query.push($column).push(" IN (");
                let mut separated = query.separated(", ");
                for value in values {
                    separated
                        .push_bind(value.as_str())
                        .push_unseparated("::NUMERIC");
                }
                separated.push_unseparated(")");
            }
        };
    }
    macro_rules! int_cmp {
        ($field:expr, $column:literal, $op:literal) => {
            if let Some(value) = $field {
                push_and(query, has_condition);
                query.push($column).push($op).push_bind(value);
            }
        };
    }
    #[cfg(feature = "legacy-query-compat")]
    macro_rules! bool_cmp {
        ($field:expr, $column:literal, $op:literal) => {
            if let Some(value) = $field {
                push_and(query, has_condition);
                query.push($column).push($op).push_bind(value);
            }
        };
    }

    text_cmp!(where_.id_eq, "id", " = ");
    text_cmp!(where_.id_not_eq, "id", " <> ");
    text_in!(where_.id_in, "id", false);
    text_in!(where_.id_not_in, "id", true);
    bigint_cmp!(where_.block_number_eq, "block_number", " = ");
    bigint_cmp!(where_.block_number_gt, "block_number", " > ");
    bigint_cmp!(where_.block_number_gte, "block_number", " >= ");
    bigint_cmp!(where_.block_number_lt, "block_number", " < ");
    bigint_cmp!(where_.block_number_lte, "block_number", " <= ");
    bigint_in!(where_.block_number_in, "block_number");
    text_cmp!(where_.transaction_hash_eq, "transaction_hash", " = ");
    text_in!(where_.transaction_hash_in, "transaction_hash", false);
    bigint_cmp!(where_.block_timestamp_eq, "block_timestamp", " = ");
    bigint_cmp!(where_.block_timestamp_gt, "block_timestamp", " > ");
    bigint_cmp!(where_.block_timestamp_gte, "block_timestamp", " >= ");
    bigint_cmp!(where_.block_timestamp_lt, "block_timestamp", " < ");
    bigint_cmp!(where_.block_timestamp_lte, "block_timestamp", " <= ");
    bigint_cmp!(where_.chain_id_eq, "chain_id", " = ");
    bigint_in!(where_.chain_id_in, "chain_id");
    int_cmp!(where_.log_index_eq, "log_index", " = ");
    int_cmp!(where_.log_index_gt, "log_index", " > ");
    int_cmp!(where_.log_index_gte, "log_index", " >= ");
    int_cmp!(where_.log_index_lt, "log_index", " < ");
    int_cmp!(where_.log_index_lte, "log_index", " <= ");
    int_cmp!(where_.transaction_index_eq, "transaction_index", " = ");
    text_cmp!(where_.msg_hash_eq, "msg_hash", " = ");
    text_in!(where_.msg_hash_in, "msg_hash", false);
    text_cmp!(where_.msg_id_eq, "msg_id", " = ");
    text_in!(where_.msg_id_in, "msg_id", false);
    text_cmp!(where_.hash_eq, "hash", " = ");
    text_cmp!(where_.channel_eq, "channel", " = ");
    text_cmp!(where_.oracle_eq, "oracle", " = ");
    text_cmp!(where_.relayer_eq, "relayer", " = ");
    text_cmp!(where_.signer_eq, "signer", " = ");
    #[cfg(feature = "legacy-query-compat")]
    text_in!(where_.signer_in, "signer", false);
    text_cmp!(where_.port_address_eq, "port_address", " = ");
    text_cmp!(where_.from_eq, r#""from""#, " = ");
    text_cmp!(where_.to_eq, r#""to""#, " = ");
    text_cmp!(where_.from_dapp_eq, "from_dapp", " = ");
    text_cmp!(where_.to_dapp_eq, "to_dapp", " = ");
    bigint_cmp!(where_.from_chain_id_eq, "from_chain_id", " = ");
    bigint_cmp!(where_.to_chain_id_eq, "to_chain_id", " = ");
    bigint_cmp!(where_.src_chain_id_eq, "src_chain_id", " = ");
    bigint_cmp!(where_.target_chain_id_eq, "target_chain_id", " = ");
    bigint_cmp!(where_.msg_index_eq, "msg_index", " = ");
    #[cfg(feature = "legacy-query-compat")]
    {
        bigint_cmp!(where_.msg_index_gt, "msg_index", " > ");
        bigint_cmp!(where_.msg_index_gte, "msg_index", " >= ");
        bigint_cmp!(where_.msg_index_lt, "msg_index", " < ");
        bigint_cmp!(where_.msg_index_lte, "msg_index", " <= ");
        bigint_cmp!(where_.index_eq, r#""index""#, " = ");
        bigint_cmp!(where_.index_gt, r#""index""#, " > ");
        bigint_cmp!(where_.index_gte, r#""index""#, " >= ");
        bigint_cmp!(where_.index_lt, r#""index""#, " < ");
        bigint_cmp!(where_.index_lte, r#""index""#, " <= ");
        bool_cmp!(where_.oracle_assigned_eq, "oracle_assigned", " = ");
        bool_cmp!(where_.relayer_assigned_eq, "relayer_assigned", " = ");
    }

    if let Some(groups) = where_.and.as_ref() {
        push_logical_groups(query, has_condition, groups, " AND ");
    }
    if let Some(groups) = where_.or.as_ref() {
        push_logical_groups(query, has_condition, groups, " OR ");
    }
}

fn push_logical_groups<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    has_condition: &mut bool,
    groups: &'a [LegacyWhereInput],
    op: &'static str,
) {
    if groups.is_empty() {
        return;
    }
    push_and(query, has_condition);
    query.push("(");
    let mut first = true;
    for group in groups {
        if first {
            first = false;
        } else {
            query.push(op);
        }
        query.push("(");
        let mut group_has_condition = false;
        push_where_group(query, group, &mut group_has_condition);
        if !group_has_condition {
            query.push("TRUE");
        }
        query.push(")");
    }
    query.push(")");
}

fn push_and(query: &mut QueryBuilder<'_, Postgres>, has_condition: &mut bool) {
    if *has_condition {
        query.push(" AND ");
    }
    *has_condition = true;
}

fn push_order_by(
    query: &mut QueryBuilder<'_, Postgres>,
    table: &'static str,
    order_by: Option<&[LegacyOrderByInput]>,
) {
    let orders = order_by.unwrap_or(&[]);
    query.push(" ORDER BY ");
    if orders.is_empty() {
        query.push(format!("{table}.block_number ASC, id ASC"));
        return;
    }
    let mut separated = query.separated(", ");
    for order in orders {
        let (column, direction, qualify) = match order {
            LegacyOrderByInput::IdAsc => ("id", "ASC", false),
            LegacyOrderByInput::IdDesc => ("id", "DESC", false),
            LegacyOrderByInput::BlockNumberAsc => ("block_number", "ASC", true),
            LegacyOrderByInput::BlockNumberDesc => ("block_number", "DESC", true),
            LegacyOrderByInput::BlockTimestampAsc => ("block_timestamp", "ASC", true),
            LegacyOrderByInput::BlockTimestampDesc => ("block_timestamp", "DESC", true),
            LegacyOrderByInput::ChainIdAsc => ("chain_id", "ASC", true),
            LegacyOrderByInput::ChainIdDesc => ("chain_id", "DESC", true),
            LegacyOrderByInput::LogIndexAsc => ("log_index", "ASC", true),
            LegacyOrderByInput::LogIndexDesc => ("log_index", "DESC", true),
            LegacyOrderByInput::TransactionIndexAsc => ("transaction_index", "ASC", true),
            LegacyOrderByInput::TransactionIndexDesc => ("transaction_index", "DESC", true),
            LegacyOrderByInput::MsgHashAsc => ("msg_hash", "ASC", false),
            LegacyOrderByInput::MsgHashDesc => ("msg_hash", "DESC", false),
            LegacyOrderByInput::MsgIdAsc => ("msg_id", "ASC", false),
            LegacyOrderByInput::MsgIdDesc => ("msg_id", "DESC", false),
            #[cfg(feature = "legacy-query-compat")]
            LegacyOrderByInput::IndexAsc => (r#""index""#, "ASC", true),
            #[cfg(feature = "legacy-query-compat")]
            LegacyOrderByInput::IndexDesc => (r#""index""#, "DESC", true),
            #[cfg(feature = "legacy-query-compat")]
            LegacyOrderByInput::MsgIndexAsc => ("msg_index", "ASC", true),
            #[cfg(feature = "legacy-query-compat")]
            LegacyOrderByInput::MsgIndexDesc => ("msg_index", "DESC", true),
        };
        if qualify {
            separated.push(format!("{table}.{column} {direction}"));
        } else {
            separated.push(format!("{column} {direction}"));
        }
    }
}

#[cfg(all(test, feature = "legacy-query-compat"))]
mod tests {
    use super::*;

    #[test]
    fn test_push_order_by_qualifies_numeric_columns_to_avoid_text_alias_sorting() {
        let mut query =
            QueryBuilder::<Postgres>::new("SELECT block_number::TEXT AS block_number FROM events");
        push_order_by(
            &mut query,
            "events",
            Some(&[
                LegacyOrderByInput::BlockNumberDesc,
                LegacyOrderByInput::BlockTimestampDesc,
                LegacyOrderByInput::ChainIdDesc,
                LegacyOrderByInput::LogIndexDesc,
                LegacyOrderByInput::TransactionIndexDesc,
                LegacyOrderByInput::MsgIndexDesc,
                LegacyOrderByInput::IndexDesc,
            ]),
        );

        assert_eq!(
            query.sql(),
            r#"SELECT block_number::TEXT AS block_number FROM events ORDER BY events.block_number DESC, events.block_timestamp DESC, events.chain_id DESC, events.log_index DESC, events.transaction_index DESC, events.msg_index DESC, events."index" DESC"#
        );
    }
}
