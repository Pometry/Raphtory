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
use poem::{get, listener::TcpListener, middleware::Cors, EndpointExt, Route, Server};
use raphtory::{
    db::api::view::MaterializedGraph,
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorisable::Vectorisable,
        EmbeddingFunction,
    },
};
use std::{collections::HashMap, ops::Deref, path::Path, sync::Arc};
use tokio::{
    io::Result as IoResult,
    signal,
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

/// A struct for defining and running a Raphtory GraphQL server
pub struct RaphtoryServer {
    data: Data,
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
    ///   * `graph_names` - the names of the graphs to vectorise.
    ///   * `embedding` - the embedding function to translate documents to embeddings.
    ///   * `cache` - the directory to use as cache for the embeddings.
    ///   * `template` - the template to use for creating documents.
    ///
    /// Returns:
    ///    A new server object containing the vectorised graphs.
    pub async fn with_vectorised<F, T>(
        self,
        graph_names: Vec<String>,
        embedding: F,
        cache: &Path,
        template: Option<T>,
    ) -> Self
    where
        F: EmbeddingFunction + Clone + 'static,
        T: DocumentTemplate<MaterializedGraph> + 'static,
    {
        let graphs = &self.data.graphs;
        let stores = &self.data.vector_stores;

        let template = template
            .map(|template| Arc::new(template) as Arc<dyn DocumentTemplate<MaterializedGraph>>)
            .unwrap_or(Arc::new(DefaultTemplate));

        for graph_name in graph_names {
            let graph_cache = cache.join(&graph_name);
            let graph = graphs.read().get(&graph_name).unwrap().deref().clone();
            println!("Loading embeddings for {graph_name} using cache from {graph_cache:?}");
            let vectorised = graph
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

    pub fn register_algorithm<'a, E: AlgorithmEntryPoint<'a> + 'static, A: Algorithm<'a, E>>(
        self,
        name: &str,
    ) -> Self {
        E::lock_plugins().insert(name.to_string(), A::register_algo);
        self
    }

    /// Start the server on the default port and return a handle to it.
    pub fn start(self) -> RunningRaphtoryServer {
        self.start_with_port(1736)
    }

    /// Start the server on the port `port` and return a handle to it.
    pub fn start_with_port(self, port: u16) -> RunningRaphtoryServer {
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
    pub async fn run(self) -> IoResult<()> {
        self.start().wait().await
    }

    /// Run the server on the port `port` until completion.
    pub async fn run_with_port(self, port: u16) -> IoResult<()> {
        self.start_with_port(port).wait().await
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
        let handler = server.start_with_port(1737);
        sleep(Duration::from_secs(1)).await;
        println!("Calling stop at time {}", Local::now());
        handler.stop().await;
        handler.wait().await.unwrap()
    }
}
