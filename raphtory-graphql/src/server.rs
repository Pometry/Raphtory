#![allow(dead_code)]

use crate::{
    config::app_config::{load_config, AppConfig},
    data::{Data, EmbeddingConf},
    model::{
        plugins::{entry_point::EntryPoint, operation::Operation},
        App,
    },
    observability::open_telemetry::OpenTelemetry,
    routes::{graphql_playground, health},
    server::ServerError::SchemaError,
};
use async_graphql_poem::GraphQL;
use config::ConfigError;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{Tracer, TracerProvider as TP};
use poem::{
    get,
    listener::TcpListener,
    middleware::{CookieJarManager, CookieJarManagerEndpoint, Cors, CorsEndpoint},
    EndpointExt, Route, Server,
};
use raphtory::vectors::{template::DocumentTemplate, EmbeddingFunction};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    io,
    io::Result as IoResult,
    signal,
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
    task,
    task::JoinHandle,
};
use tracing::{debug, error, info};
use tracing_subscriber::{
    fmt, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, Registry,
};
use url::ParseError;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Config error: {0}")]
    ConfigError(#[from] ConfigError),
    #[error("Cache error: {0}")]
    CacheError(String),
    #[error("No client id provided")]
    MissingClientId,
    #[error("No client secret provided")]
    MissingClientSecret,
    #[error("No tenant id provided")]
    MissingTenantId,
    #[error("Parse error: {0}")]
    FailedToParseUrl(#[from] ParseError),
    #[error("Failed to fetch JWKS")]
    FailedToFetchJWKS,
    #[error("Failed to load schema: {0}")]
    SchemaError(String),
    #[error("Failed to create endpoints: {0}")]
    EndpointError(String),
}

impl From<ServerError> for io::Error {
    fn from(error: ServerError) -> Self {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

/// A struct for defining and running a Raphtory GraphQL server
pub struct GraphServer {
    data: Data,
    config: AppConfig,
}

impl GraphServer {
    pub fn new(
        work_dir: PathBuf,
        app_config: Option<AppConfig>,
        config_path: Option<PathBuf>,
    ) -> IoResult<Self> {
        if !work_dir.exists() {
            fs::create_dir_all(&work_dir).unwrap();
        }
        let config =
            load_config(app_config, config_path).map_err(|err| ServerError::ConfigError(err))?;
        let data = Data::new(work_dir.as_path(), &config);
        Ok(Self { data, config })
    }

    pub fn set_embeddings<F: EmbeddingFunction + Clone + 'static>(
        mut self,
        embedding: F,
        cache: &Path, // TODO: maybe now that we are storing vectors we could bin the cache!!!
        // or maybe it could be in a standard location like /tmp/raphtory/embedding_cache
        global_template: Option<DocumentTemplate>,
    ) -> Self {
        let cache = Some(PathBuf::from(cache).into()).into();
        self.data.embedding_conf = Some(EmbeddingConf {
            function: Arc::new(embedding),
            cache,
            global_template,
            individual_templates: Default::default(),
        });
        self
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
    pub fn with_vectorised_graphs(
        mut self,
        graph_names: Vec<String>,
        template: DocumentTemplate,
    ) -> Self {
        if let Some(embedding_conf) = &mut self.data.embedding_conf {
            for graph_name in graph_names {
                embedding_conf
                    .individual_templates
                    .insert(graph_name.into(), template.clone());
            }
        }
        self
    }

    pub fn register_query_plugin<
        'a,
        E: EntryPoint<'a> + 'static + Send,
        A: Operation<'a, E> + 'static + Send,
    >(
        self,
        name: &str,
    ) -> Self {
        E::lock_plugins().insert(name.to_string(), Box::new(A::register_operation));
        self
    }

    pub fn register_mutation_plugin<
        'a,
        E: EntryPoint<'a> + 'static + Send,
        A: Operation<'a, E> + 'static + Send,
    >(
        self,
        name: &str,
    ) -> Self {
        E::lock_plugins().insert(name.to_string(), Box::new(A::register_operation));
        self
    }

    /// Start the server on the default port and return a handle to it.
    pub async fn start(self) -> IoResult<RunningGraphServer> {
        self.start_with_port(1736).await
    }

    /// Start the server on the port `port` and return a handle to it.
    pub async fn start_with_port(self, port: u16) -> IoResult<RunningGraphServer> {
        self.data.vectorise_all_graphs_that_are_not().await?;

        let config = &self.config;
        let filter = config.logging.get_log_env();
        let tracer_name = config.tracing.otlp_tracing_service_name.clone();
        let tp = config.tracing.tracer_provider();

        // Create the base registry
        let registry = Registry::default().with(filter).with(
            fmt::layer().pretty().with_span_events(FmtSpan::NONE), //(FULL, NEW, ENTER, EXIT, CLOSE)
        );
        match tp.clone() {
            Some(tp) => {
                registry
                    .with(
                        tracing_opentelemetry::layer().with_tracer(tp.tracer(tracer_name.clone())),
                    )
                    .try_init()
                    .ok();
            }

            None => {
                registry.try_init().ok();
            }
        };
        // it is important that this runs after algorithms have been pushed to PLUGIN_ALGOS static variable
        let app: CorsEndpoint<CookieJarManagerEndpoint<Route>> = self
            .generate_endpoint(tp.clone().map(|tp| tp.tracer(tracer_name)))
            .await?;

        let (signal_sender, signal_receiver) = mpsc::channel(1);

        info!("Playground live at: http://0.0.0.0:{port}");
        let server_task = Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run_with_graceful_shutdown(app, server_termination(signal_receiver, tp), None);
        let server_result = tokio::spawn(server_task);

        Ok(RunningGraphServer {
            signal_sender,
            server_result,
        })
    }

    async fn generate_endpoint(
        self,
        tracer: Option<Tracer>,
    ) -> Result<CorsEndpoint<CookieJarManagerEndpoint<Route>>, ServerError> {
        let schema_builder = App::create_schema();
        let schema_builder = schema_builder.data(self.data);
        let schema = schema_builder;
        let schema = if let Some(t) = tracer {
            schema.extension(OpenTelemetry::new(t)).finish()
        } else {
            schema.finish()
        }
        .map_err(|e| SchemaError(e.to_string()))?;

        let app = Route::new()
            .at("/", get(graphql_playground).post(GraphQL::new(schema)))
            .at("/health", get(health))
            .with(CookieJarManager::new())
            .with(Cors::new());
        Ok(app)
    }

    /// Run the server on the default port until completion.
    pub async fn run(self) -> IoResult<()> {
        self.start().await?.wait().await
    }

    /// Run the server on the port `port` until completion.
    pub async fn run_with_port(self, port: u16) -> IoResult<()> {
        self.start_with_port(port).await?.wait().await
    }
}

/// A Raphtory server handler
pub struct RunningGraphServer {
    signal_sender: Sender<()>,
    server_result: JoinHandle<IoResult<()>>,
}

impl RunningGraphServer {
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

async fn server_termination(mut internal_signal: Receiver<()>, tp: Option<TP>) {
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
    match tp {
        None => {}
        Some(p) => {
            task::spawn_blocking(move || {
                let res = p.shutdown();
                if let Err(e) = res {
                    debug!("Failed to shut down tracing provider: {:?}", e);
                }
            })
            .await
            .unwrap();
        }
    }
}

#[cfg(test)]
mod server_tests {
    extern crate chrono;

    use crate::server::GraphServer;
    use chrono::prelude::*;
    use raphtory_api::core::utils::logging::global_info_logger;
    use tokio::time::{sleep, Duration};
    use tracing::info;

    #[tokio::test]
    async fn test_server_start_stop() {
        global_info_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, None).unwrap();
        info!("Calling start at time {}", Local::now());
        let handler = server.start_with_port(0);
        sleep(Duration::from_secs(1)).await;
        info!("Calling stop at time {}", Local::now());
        handler.await.unwrap().stop().await
    }
}
