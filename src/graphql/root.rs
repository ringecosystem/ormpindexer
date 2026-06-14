use async_graphql::{Context, Object, Result as GraphqlResult};
use sqlx::PgPool;

use super::query::*;
use super::router::GraphqlState;
use super::types::*;

pub struct QueryRoot;

#[Object(rename_fields = "camelCase")]
impl QueryRoot {
    async fn ormp_hash_imported_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<ORMPHashImported>> {
        query_ormp_hash_imported_by_id(pool(ctx)?, id).await
    }

    async fn ormp_hash_importeds(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<ORMPHashImported>> {
        query_ormp_hash_importeds(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_hash_importeds_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<ORMPHashImportedPage> {
        query_ormp_hash_importeds_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_message_accepted_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<ORMPMessageAccepted>> {
        query_ormp_message_accepted_by_id(pool(ctx)?, id).await
    }

    async fn ormp_message_accepteds(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<ORMPMessageAccepted>> {
        query_ormp_message_accepteds(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_message_accepteds_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<ORMPMessageAcceptedPage> {
        query_ormp_message_accepteds_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_message_assigned_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<ORMPMessageAssigned>> {
        query_ormp_message_assigned_by_id(pool(ctx)?, id).await
    }

    async fn ormp_message_assigneds(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<ORMPMessageAssigned>> {
        query_ormp_message_assigneds(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_message_assigneds_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<ORMPMessageAssignedPage> {
        query_ormp_message_assigneds_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_message_dispatched_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<ORMPMessageDispatched>> {
        query_ormp_message_dispatched_by_id(pool(ctx)?, id).await
    }

    async fn ormp_message_dispatcheds(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<ORMPMessageDispatched>> {
        query_ormp_message_dispatcheds(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn ormp_message_dispatcheds_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<ORMPMessageDispatchedPage> {
        query_ormp_message_dispatcheds_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn msgport_message_recv_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<MsgportMessageRecv>> {
        query_msgport_message_recv_by_id(pool(ctx)?, id).await
    }

    async fn msgport_message_recvs(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<MsgportMessageRecv>> {
        query_msgport_message_recvs(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn msgport_message_recvs_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<MsgportMessageRecvPage> {
        query_msgport_message_recvs_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn msgport_message_sent_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<MsgportMessageSent>> {
        query_msgport_message_sent_by_id(pool(ctx)?, id).await
    }

    async fn msgport_message_sents(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<MsgportMessageSent>> {
        query_msgport_message_sents(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn msgport_message_sents_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<MsgportMessageSentPage> {
        query_msgport_message_sents_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn signature_pub_signature_submittion_by_id(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> GraphqlResult<Option<SignaturePubSignatureSubmittion>> {
        query_signature_pub_signature_submittion_by_id(pool(ctx)?, id).await
    }

    async fn signature_pub_signature_submittions(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<Vec<SignaturePubSignatureSubmittion>> {
        query_signature_pub_signature_submittions(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }

    async fn signature_pub_signature_submittions_page(
        &self,
        ctx: &Context<'_>,
        where_: Option<LegacyWhereInput>,
        order_by: Option<Vec<LegacyOrderByInput>>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> GraphqlResult<SignaturePubSignatureSubmittionPage> {
        query_signature_pub_signature_submittions_page(
            pool(ctx)?,
            where_.as_ref(),
            order_by.as_deref(),
            offset,
            limit,
        )
        .await
    }
}

fn pool<'a>(ctx: &'a Context<'_>) -> GraphqlResult<&'a PgPool> {
    Ok(&ctx.data::<GraphqlState>()?.pool)
}
