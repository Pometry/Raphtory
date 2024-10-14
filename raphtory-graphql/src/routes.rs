use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use poem::{
    handler,
    http::StatusCode,
    web::{Html, Json},
    IntoResponse,
};
use serde::Serialize;

#[derive(Serialize)]
struct Health {
    healthy: bool,
}

#[handler]
pub(crate) async fn health() -> impl IntoResponse {
    let health = Health { healthy: true };
    (StatusCode::OK, Json(health))
}

#[handler]
pub(crate) async fn graphql_playground() -> impl IntoResponse {
    let config = GraphQLPlaygroundConfig::new("/")
        .subscription_endpoint("/ws")
        .with_setting("request.credentials", "include");
    Html(playground_source(config))
}

#[handler]
pub(crate) async fn ui() -> impl IntoResponse {
    Html(include_str!("../resources/index.html"))
}
