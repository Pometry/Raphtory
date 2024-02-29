#![allow(dead_code)]

use crate::{
    data::Data,
    model::{
        algorithms::{algorithm::Algorithm, algorithm_entry_point::AlgorithmEntryPoint},
        App,
    },
    observability::tracing::create_tracer_from_env,
    routes::{graphql_playground, health},
};
use async_graphql_poem::GraphQL;
use itertools::Itertools;
use poem::{get, listener::TcpListener, middleware::Cors, EndpointExt, Route, Server};
use raphtory::{
    db::api::view::{DynamicGraph, IntoDynamic, MaterializedGraph},
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorisable::Vectorisable,
        EmbeddingFunction,
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::Path, sync::Arc};
use tokio::{
    io::Result as IoResult,
    signal,
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use tracing::{metadata::ParseLevelError, Level};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, FmtSubscriber,
    Registry,
};

/// A struct for defining and running a Raphtory GraphQL server
pub struct RaphtoryServer {
    data: Data,
}

// Define a struct for log configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct LogConfig {
    log_level: String,
}

impl RaphtoryServer {
    /// Return a server object with graphs loaded from a map `graphs`
    pub fn from_map(graphs: HashMap<String, MaterializedGraph>) -> Self {
        let data = Data::from_map(graphs);
        Self { data }
    }

    /// Return a server object with graphs loaded from a directory `graph_directory`
    pub fn from_directory(graph_directory: &str) -> Self {
        let data = Data::from_directory(graph_directory);
        Self { data }
    }

    /// Return a server object with graphs loaded from a map `graphs` and a directory `graph_directory`
    pub fn from_map_and_directory(
        graphs: HashMap<String, MaterializedGraph>,
        graph_directory: &str,
    ) -> Self {
        let data = Data::from_map_and_directory(graphs, graph_directory);
        Self { data }
    }

    /// Vectorise a subset of the graphs of the server.
    ///
    /// Arguments:
    ///   * `graph_names` - the names of the graphs to vectorise. All if None is provided.
    ///   * `embedding` - the embedding function to translate documents to embeddings.
    ///   * `cache` - the directory to use as cache for the embeddings.
    ///   * `template` - the template to use for creating documents.
    ///
    /// Returns:
    ///    A new server object containing the vectorised graphs.
    pub async fn with_vectorised<F, T>(
        self,
        graph_names: Option<Vec<String>>,
        embedding: F,
        cache: &Path,
        template: Option<T>,
    ) -> Self
    where
        F: EmbeddingFunction + Clone + 'static,
        T: DocumentTemplate<DynamicGraph> + 'static,
    {
        let graphs = &self.data.graphs;
        let stores = &self.data.vector_stores;

        let template = template
            .map(|template| Arc::new(template) as Arc<dyn DocumentTemplate<DynamicGraph>>)
            .unwrap_or(Arc::new(DefaultTemplate));

        let graph_names = graph_names.unwrap_or_else(|| {
            let graphs = graphs.read();
            let all_graph_names = graphs.iter().map(|(graph_name, _)| graph_name).cloned();
            all_graph_names.collect_vec()
        });

        for graph_name in graph_names {
            let graph_cache = cache.join(&graph_name);
            let graph = graphs.read().get(&graph_name).unwrap().clone();
            println!("Loading embeddings for {graph_name} using cache from {graph_cache:?}");
            let vectorised = graph
                .into_dynamic()
                .vectorise_with_template(
                    Box::new(embedding.clone()),
                    Some(graph_cache),
                    true,
                    template.clone(),
                    true,
                )
                .await;
            stores.write().insert(graph_name, vectorised);
        }
        println!("Embeddings were loaded successfully");

        self
    }

    pub fn register_algorithm<
        'a,
        E: AlgorithmEntryPoint<'a> + 'static,
        A: Algorithm<'a, E> + 'static,
    >(
        self,
        name: &str,
    ) -> Self {
        E::lock_plugins().insert(name.to_string(), Box::new(A::register_algo));
        self
    }

    /// Start the server on the default port and return a handle to it.
    pub fn start(self, log_config_or_level: &str) -> RunningRaphtoryServer {
        self.start_with_port(1736, log_config_or_level)
    }

    /// Start the server on the port `port` and return a handle to it.
    pub fn start_with_port(self, port: u16, log_config_or_level: &str) -> RunningRaphtoryServer {
        fn parse_log_level(input: &str) -> Option<String> {
            // Parse log level from string
            let level: Result<Level, ParseLevelError> = input.trim().parse();
            match level {
                Ok(level) => Some(level.to_string()),
                Err(_) => None,
            }
        }

        fn setup_logger_with_level(log_level: String) {
            let filter = EnvFilter::try_new(log_level)
                .unwrap_or_else(|_| EnvFilter::try_new("info").unwrap()); // Default to info if the provided level is invalid
            let subscriber = FmtSubscriber::builder()
                .with_env_filter(filter)
                .with_span_events(FmtSpan::CLOSE)
                .finish();
            if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
                eprintln!(
                    "Log level cannot be updated within the same runtime environment: {}",
                    err
                );
            }
        }

        fn setup_logger_from_config(log_config_path: &str) {
            let config_content =
                fs::read_to_string(log_config_path).expect("Failed to read log config file");
            let config: LogConfig =
                toml::from_str(&config_content).expect("Failed to deserialize log config");

            let filter = EnvFilter::new(&config.log_level);
            let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
            if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
                eprintln!(
                    "Log level cannot be updated within the same runtime environment: {}",
                    err
                );
            }
        }

        fn configure_logger(log_config_or_level: &str) {
            if let Some(log_level) = parse_log_level(log_config_or_level) {
                setup_logger_with_level(log_level);
            } else {
                setup_logger_from_config(log_config_or_level);
            }
        }

        configure_logger(log_config_or_level);

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

        let (signal_sender, signal_receiver) = mpsc::channel(1);

        println!("Playground: http://localhost:{port}");
        let server_task = Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run_with_graceful_shutdown(app, server_termination(signal_receiver), None);
        let server_result = tokio::spawn(server_task);

        RunningRaphtoryServer {
            signal_sender,
            server_result,
        }
    }

    /// Run the server on the default port until completion.
    pub async fn run(self, log_config_or_level: &str) -> IoResult<()> {
        self.start(log_config_or_level).wait().await
    }

    /// Run the server on the port `port` until completion.
    pub async fn run_with_port(self, port: u16, log_config_or_level: &str) -> IoResult<()> {
        self.start_with_port(port, log_config_or_level).wait().await
    }
}

/// A Raphtory server handler
pub struct RunningRaphtoryServer {
    signal_sender: Sender<()>,
    server_result: JoinHandle<IoResult<()>>,
}

impl RunningRaphtoryServer {
    /// Stop the server.
    pub async fn stop(&self) {
        let _ignored = self.signal_sender.send(()).await;
    }

    /// Wait until server completion.
    pub async fn wait(self) -> IoResult<()> {
        self.server_result
            .await
            .expect("Coudln't join tokio task for the server")
    }

    // TODO: make this optional with some python feature flag
    pub fn _get_sender(&self) -> &Sender<()> {
        &self.signal_sender
    }
}

async fn server_termination(mut internal_signal: Receiver<()>) {
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

    let internal_terminate = async {
        internal_signal.recv().await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = internal_terminate => {},
    }

    opentelemetry::global::shutdown_tracer_provider();
}

#[cfg(test)]
mod server_tests {
    extern crate chrono;
    use crate::server::RaphtoryServer;
    use chrono::prelude::*;
    use raphtory::prelude::{Graph, GraphViewOps};
    use std::collections::HashMap;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_server_stop() {
        let g = Graph::new().materialize().unwrap();
        let graphs = HashMap::from([("test".to_owned(), g)]);
        let server = RaphtoryServer::from_map(graphs);
        println!("calling start at time {}", Local::now());
        let handler = server.start_with_port(1737, "info");
        sleep(Duration::from_secs(1)).await;
        println!("Calling stop at time {}", Local::now());
        handler.stop().await;
        handler.wait().await.unwrap()
    }
}
