use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use opentelemetry::trace::TraceContextExt;
use poem::http::StatusCode;
use poem::web::{Html, Json};
use poem::{handler, IntoResponse};
use serde::Serialize;
use tracing::{info, span, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

// pub(crate) async fn graphql_handler(
//     Extension(schema): Extension<ServiceSchema>,
//     req: GraphQLRequest,
// ) -> GraphQLResponse {
//     let span = span!(Level::INFO, "graphql_execution");
//
//     info!("Processing GraphQL request");
//
//     let response = async move { schema.execute(req.into_inner()).await }
//         .instrument(span.clone())
//         .await;
//
//     info!("Processing GraphQL request finished");
//
//     response
//         .extension(
//             "traceId",
//             async_graphql::Value::String(format!(
//                 "{}",
//                 span.context().span().span_context().trace_id()
//             )),
//         )
//         .into()
// }
