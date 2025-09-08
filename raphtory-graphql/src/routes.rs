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

#[derive(Serialize)]
struct Version {
    version: String,
}

#[handler]
pub(crate) async fn health() -> impl IntoResponse {
    let health = Health { healthy: true };
    (StatusCode::OK, Json(health))
}

#[handler]
pub(crate) async fn version() -> impl IntoResponse {
    let v = Version {
        version: String::from(raphtory::version()),
    };
    (StatusCode::OK, Json(v))
}

#[handler]
pub(crate) async fn ui() -> impl IntoResponse {
    Html(include_str!(env!("RAPHTORY_UI_INDEX_PATH")))
}
