#![allow(dead_code)]

use crate::{
    data::Data,
    model::{algorithm::Algorithm, App},
    observability::tracing::create_tracer_from_env,
    routes::{graphql_playground, health},
};
use async_graphql_poem::GraphQL;
use poem::{get, listener::TcpListener, middleware::Cors, EndpointExt, Route, Server};
use raphtory::db::graph::edge::EdgeView;
use raphtory::db::graph::vertex::VertexView;
use raphtory::prelude::Graph;
use raphtory::vectors::Embedding;
use raphtory::vectors::Vectorizable;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::path::Path;
use tokio::{io::Result as IoResult, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub struct RaphtoryServer {
    data: Data,
}

impl RaphtoryServer {
    pub fn from_map(graphs: HashMap<String, Graph>) -> Self {
        let data = Data::from_map(graphs);
        Self { data }
    }

    pub fn from_directory(graph_directory: &str) -> Self {
        let data = Data::from_directory(graph_directory);
        Self { data }
    }

    pub fn from_map_and_directory(graphs: HashMap<String, Graph>, graph_directory: &str) -> Self {
        let data = Data::from_map_and_directory(graphs, graph_directory);
        Self { data }
    }

    pub async fn with_vectorized<F, U, N, E>(
        self,
        graph_names: Vec<String>,
        embedding: F,
        cache_dir: &Path,
        templates: Option<(N, E)>,
    ) -> Self
    where
        F: Fn(Vec<String>) -> U + Send + Sync + Copy + 'static,
        U: Future<Output = Vec<Embedding>> + Send + 'static,
        N: Fn(&VertexView<Graph>) -> String + Sync + Send + Copy + 'static,
        E: Fn(&EdgeView<Graph>) -> String + Sync + Send + Copy + 'static,
    {
        {
            let graphs_map = self.data.graphs.read();
            let mut stores_map = self.data.vector_stores.write();

            for graph_name in graph_names {
                let graph_cache = cache_dir.join(&graph_name);
                let graph = graphs_map.get(&graph_name).unwrap().deref().clone();

                println!("Loading embeddings for {graph_name} using cache from {graph_cache:?}");
                let vectorized = match templates {
                    Some((node_template, edge_template)) => {
                        graph
                            .vectorize_with_templates(
                                Box::new(embedding),
                                &graph_cache,
                                node_template,
                                edge_template,
                            )
                            .await
                    }
                    None => graph.vectorize(Box::new(embedding), &graph_cache).await,
                };
                stores_map.insert(graph_name, vectorized);
            }
        }

        println!("Embeddings were loaded successfully");

        self
    }

    pub fn register_algorithm<T: Algorithm>(self, name: &str) -> Self {
        crate::model::algorithm::PLUGIN_ALGOS
            .lock()
            .unwrap()
            .insert(name.to_string(), T::register_algo);
        self
    }

    pub async fn run(self) -> IoResult<()> {
        self.run_with_port(1736).await
    }

    pub async fn run_with_port(self, port: u16) -> IoResult<()> {
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
            .run_with_graceful_shutdown(app, shutdown_signal(), None)
            .await
    }
}

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
