#![allow(dead_code)]

use crate::{
    data::Data,
    model::{algorithm::Algorithm, QueryRoot},
    observability::tracing::create_tracer_from_env,
    routes::{graphql_playground, health},
};
use async_graphql_poem::GraphQL;
use dynamic_graphql::App;
use poem::{get, listener::TcpListener, middleware::Cors, EndpointExt, Route, Server};
use raphtory::db::api::view::internal::DynamicGraph;
use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
};
use tokio::{io::Result as IoResult, runtime::Runtime, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub struct RaphtoryServer {
    data: Data,
    sender: Sender<()>,
}

impl RaphtoryServer {
    pub fn sender(&self) -> Sender<()> {
        self.sender.clone()
    }

    pub fn from_map(graphs: HashMap<String, DynamicGraph>) -> (Self, Receiver<()>) {
        let data = Data::from_map(graphs);
        let (sender, receiver) = mpsc::channel::<()>();
        (Self { data, sender }, receiver)
    }

    pub fn from_directory(graph_directory: &str) -> (Self, Receiver<()>) {
        let data = Data::from_directory(graph_directory);
        let (sender, receiver) = mpsc::channel::<()>();
        (Self { data, sender }, receiver)
    }

    pub fn from_map_and_directory(
        graphs: HashMap<String, DynamicGraph>,
        graph_directory: &str,
    ) -> (Self, Receiver<()>) {
        let data = Data::from_map_and_directory(graphs, graph_directory);
        let (sender, receiver) = mpsc::channel::<()>();
        (Self { data, sender }, receiver)
    }

    pub fn register_algorithm<T: Algorithm>(self, name: &str) -> Self {
        crate::model::algorithm::PLUGIN_ALGOS
            .lock()
            .unwrap()
            .insert(name.to_string(), T::register_algo);
        self
    }

    pub async fn run(self) -> IoResult<()> {
        self.run_with_port(1736, None).await
    }

    pub async fn run_with_port(self, port: u16, recv: Option<Receiver<()>>) -> IoResult<()> {
        let registry = Registry::default().with(tracing_subscriber::fmt::layer().pretty());
        let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO"));

        match create_tracer_from_env() {
            Some(tracer) => registry
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .with(env_filter)
                .try_init(),
            None => registry.with(env_filter).try_init(),
        }
        .unwrap_or(());

        #[derive(App)]
        struct App(QueryRoot);

        // it is important that this runs after algorithms have been pushed to PLUGIN_ALGOS static variable
        let schema_builder = App::create_schema();
        let schema_builder = schema_builder.data(self.data);
        let schema = schema_builder.finish().unwrap();
        let app = Route::new()
            .at("/", get(graphql_playground).post(GraphQL::new(schema)))
            .at("/health", get(health))
            .with(Cors::new());

        println!("Playground: http://localhost:{port}");

        Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run_with_graceful_shutdown(app, shutdown_signal(recv), None)
            .await
    }

    pub fn run_and_forget(self, port: u16, recv: Option<Receiver<()>>) -> TokioRuntime {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let rt = runtime.clone();
        println!("Starting server on port {}", port);
        std::thread::spawn(move || {
            runtime.clone().spawn(async move {
                self.run_with_port(port, recv).await.unwrap();
            });
        });
        println!("Server running on port {}", port);
        TokioRuntime(rt)
    }
}

pub struct TokioRuntime(Arc<tokio::runtime::Runtime>);

async fn shutdown_signal(recv: Option<Receiver<()>>) {
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

    if let Some(recv) = recv {
        let done = async move {
            println!("Waiting for shutdown signal");
            if let Ok(_) = recv.recv() {
                println!("Received shutdown signal")
            } else {
                println!("Failed to receive shutdown signal")
            }
            println!("Done Waiting!")
        };
        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
            _ = done => {
                println!("Received shutdown signal")
            },
        }
    } else {
        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }
    }

    opentelemetry::global::shutdown_tracer_provider();
}
