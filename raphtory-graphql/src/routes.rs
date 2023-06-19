use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use poem::http::StatusCode;
use poem::web::{Html, Json};
use poem::{handler, IntoResponse};
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
    Html(playground_source(
        GraphQLPlaygroundConfig::new("/").subscription_endpoint("/ws"),
    ))
}
