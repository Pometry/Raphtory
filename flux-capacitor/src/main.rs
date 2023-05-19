use crate::data::Metadata;
use crate::model::QueryRoot;
use crate::observability::metrics::create_prometheus_recorder;
use crate::observability::tracing::create_tracer_from_env;
use crate::routes::{graphql_playground, health};
use async_graphql_poem::GraphQL;
use dotenv::dotenv;
use dynamic_graphql::App;
use poem::listener::TcpListener;
use poem::{get, Route, Server};
use raphtory::db::view_api::GraphViewOps;
use std::future::ready;
use tokio::signal;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

mod data;
mod model;
mod observability;
mod routes;

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    opentelemetry::global::shutdown_tracer_provider();
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    // let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
    //     .data(Metadata::lotr())
    //     .finish();

    // let prometheus_recorder = create_prometheus_recorder();

    let registry = Registry::default().with(tracing_subscriber::fmt::layer().pretty());

    match create_tracer_from_env() {
        Some(tracer) => registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .expect("Failed to register tracer with registry"),
        None => registry
            .try_init()
            .expect("Failed to register tracer with registry"),
    }

    use async_graphql::dynamic::Schema;

    #[derive(App)]
    struct App(QueryRoot);
    let schema = App::create_schema()
        .data(Metadata::lotr())
        .finish()
        .unwrap();
    let app = Route::new()
        .at("/", get(graphql_playground).post(GraphQL::new(schema)))
        .at("/health", get(health));
    // .at("/metrics", get(move || ready(prometheus_recorder.render())))
    // .route_layer(middleware::from_fn(track_metrics))

    println!("Playground: http://localhost:8000");
    Server::new(TcpListener::bind("0.0.0.0:8000"))
        .run_with_graceful_shutdown(app, shutdown_signal(), None)
        .await
        .unwrap();
}
