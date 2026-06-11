use async_graphql::http::{GraphiQLPlugin, GraphiQLSource};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use sqlx::PgPool;

use super::QueryRoot;

pub type IndexerGraphqlSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn build_schema(pool: PgPool) -> IndexerGraphqlSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(GraphqlState { pool })
        .finish()
}

pub fn build_router(schema: IndexerGraphqlSchema) -> Router {
    Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphiql", get(graphql_graphiql))
        .with_state(schema)
}

async fn graphql_handler(
    State(schema): State<IndexerGraphqlSchema>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(request.into_inner()).await.into()
}

async fn graphql_graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .version("3.9.0")
            .title("ORMP Indexer GraphiQL")
            .plugins(&[graphiql_explorer_plugin()])
            .finish(),
    )
}

fn graphiql_explorer_plugin<'a>() -> GraphiQLPlugin<'a> {
    GraphiQLPlugin {
        name: "GraphiQLPluginExplorer",
        constructor: "GraphiQLPluginExplorer.explorerPlugin",
        head_assets: Some(
            r#"<link rel="stylesheet" href="https://unpkg.com/@graphiql/plugin-explorer@3.0.0/dist/style.css" />"#,
        ),
        body_assets: Some(
            r#"<script
      src="https://unpkg.com/@graphiql/plugin-explorer@3.0.0/dist/index.umd.js"
      crossorigin
    ></script>"#,
        ),
        ..Default::default()
    }
}

pub(super) struct GraphqlState {
    pub(super) pool: PgPool,
}
