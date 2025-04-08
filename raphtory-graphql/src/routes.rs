use default_env::default_env;
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
pub(crate) async fn ui() -> impl IntoResponse {
    Html(include_str!(env!("RAPHTORY_UI_INDEX_PATH",)))
}
