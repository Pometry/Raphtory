use crate::{
    auth::{AuthenticatedGraphQL, MutationAuth},
    config::app_config::{load_config, AppConfig},
    data::Data,
    model::{
        plugins::{entry_point::EntryPoint, operation::Operation},
        App,
    },
    observability::open_telemetry::OpenTelemetry,
    paths::ExistingGraphFolder,
    routes::{health, version, PublicFilesEndpoint},
    server::ServerError::SchemaError,
};
use config::ConfigError;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{Tracer, TracerProvider as TP};
use poem::{
    get,
    listener::TcpListener,
    middleware::{Compression, CompressionEndpoint, Cors, CorsEndpoint},
    web::CompressionLevel,
    EndpointExt, Route, Server,
};
use raphtory::{
    errors::GraphResult,
    vectors::{
        cache::{CachedEmbeddingModel, VectorCache},
        storage::OpenAIEmbeddings,
        template::DocumentTemplate,
    },
};
use serde_json::json;
use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
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

pub const DEFAULT_PORT: u16 = 1736;

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
        io::Error::other(error)
    }
}

/// A struct for defining and running a Raphtory GraphQL server
#[derive(Clone)]
pub struct GraphServer {
    data: Data,
    config: AppConfig,
}

pub fn register_query_plugin<
    'a,
    E: EntryPoint<'a> + 'static + Send,
    A: Operation<'a, E> + 'static + Send,
>(
    name: &str,
) {
    E::lock_plugins().insert(name.to_string(), Box::new(A::register_operation));
}

pub fn register_mutation_plugin<
    'a,
    E: EntryPoint<'a> + 'static + Send,
    A: Operation<'a, E> + 'static + Send,
>(
    name: &str,
) {
    E::lock_plugins().insert(name.to_string(), Box::new(A::register_operation));
}

impl GraphServer {
    /// Creates a new server and returns a corresponding GraphServer object.
    ///
    /// Returns:
    ///     IoResult:
    pub async fn new(
        work_dir: PathBuf,
        app_config: Option<AppConfig>,
        config_path: Option<PathBuf>,
    ) -> IoResult<Self> {
        if !work_dir.exists() {
            create_dir_all(&work_dir)?;
        }
        let config = load_config(app_config, config_path).map_err(ServerError::ConfigError)?;
        let data = Data::new(work_dir.as_path(), &config).await?;
        Ok(Self { data, config })
    }

    /// Turn off index for all graphs
    pub fn turn_off_index(&mut self) {
        self.data.create_index = false; // FIXME: why does this exist yet?
    }

    /// Vectorise all the graphs in the server working directory.
    ///
    /// Arguments:
    ///   * name - the name of the graph to vectorise.
    ///   * template - the template to use for creating documents.
    ///
    /// Returns:
    /// A new server object containing the vectorised graphs.
    pub async fn vectorise_all_graphs(
        &self,
        template: &DocumentTemplate,
        embeddings: OpenAIEmbeddings,
    ) -> GraphResult<()> {
        let model = self.data.vector_cache.openai(embeddings).await?;
        for folder in self.data.get_all_graph_folders() {
            self.data
                .vectorise_folder(&folder, template, model.clone()) // TODO: avoid clone, just ask for a ref
                .await?;
        }
        Ok(())
    }

    /// Vectorise the graph 'name'in the server working directory.
    ///
    /// Arguments:
    ///   * path - the path of the graph to vectorise.
    ///   * template - the template to use for creating documents.
    pub async fn vectorise_graph(
        &self,
        path: &str,
        template: DocumentTemplate,
        embeddings: OpenAIEmbeddings,
    ) -> GraphResult<()> {
        let model = self.data.vector_cache.openai(embeddings).await?;
        let folder = ExistingGraphFolder::try_from(self.data.work_dir.clone(), path)?;
        self.data.vectorise_folder(&folder, &template, model).await
    }

    /// Start the server on the default port and return a handle to it.
    pub async fn start(&self) -> IoResult<RunningGraphServer> {
        self.start_with_port(DEFAULT_PORT).await
    }

    /// Start the server on the given port and return a handle to it.
    pub async fn start_with_port(&self, port: u16) -> IoResult<RunningGraphServer> {
        // set up opentelemetry first of all
        let config = self.config.clone();
        let filter = config.logging.get_log_env();
        let tracer_name = config.tracing.otlp_tracing_service_name.clone();
        let tp = config.tracing.tracer_provider()?;
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

        let work_dir = self.data.work_dir.clone();

        // it is important that this runs after algorithms have been pushed to PLUGIN_ALGOS static variable
        let app = self
            .generate_endpoint(tp.clone().map(|tp| tp.tracer(tracer_name)))
            .await?;

        let (signal_sender, signal_receiver) = mpsc::channel(1);

        info!("UI listening on 0.0.0.0:{port}, live at: http://localhost:{port}");
        debug!(
            "Server configurations: {}",
            json!({
                "config": config,
                "work_dir": work_dir
            })
        );

        let server_task = Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run_with_graceful_shutdown(app, server_termination(signal_receiver, tp), None);
        let server_result = tokio::spawn(server_task);

        Ok(RunningGraphServer {
            signal_sender,
            server_result,
        })
    }

    async fn generate_endpoint(
        &self,
        tracer: Option<Tracer>,
    ) -> Result<CompressionEndpoint<CorsEndpoint<Route>>, ServerError> {
        let schema_builder = App::create_schema();
        let schema_builder = schema_builder.data(self.data.clone());
        let schema_builder = schema_builder.extension(MutationAuth);
        let trace_level = self.config.tracing.tracing_level.clone();
        let schema = if let Some(t) = tracer {
            schema_builder
                .extension(OpenTelemetry::new(t, trace_level))
                .finish()
        } else {
            schema_builder.finish()
        }
        .map_err(|e| SchemaError(e.to_string()))?;

        let app = Route::new()
            .nest(
                "/",
                PublicFilesEndpoint::new(
                    self.config.public_dir.clone(),
                    AuthenticatedGraphQL::new(schema, self.config.auth.clone()),
                ),
            )
            .at("/health", get(health))
            .at("/version", get(version))
            .with(Cors::new())
            .with(Compression::new().with_quality(CompressionLevel::Fastest));
        Ok(app)
    }

    /// Run the server on the default port until completion.
    pub async fn run(self) -> IoResult<()> {
        self.start().await?.wait().await
    }

    /// Run the server on the given port until completion.
    pub async fn run_with_port(self, port: u16) -> IoResult<()> {
        self.start_with_port(port).await?.wait().await
    }
}

/// A Raphtory server handler
#[derive(Debug)]
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
            .expect("Couldn't join tokio task for the server")
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
    use crate::server::GraphServer;
    use chrono::prelude::*;
    use raphtory::{
        prelude::{AdditionOps, Graph, StableEncode, NO_PROPS},
        vectors::{
            embeddings::EmbeddingResult, storage::OpenAIEmbeddings, template::DocumentTemplate,
            Embedding,
        },
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};
    use tracing::info;

    #[tokio::test]
    async fn test_server_start_stop() {
        global_info_logger();
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, None)
            .await
            .unwrap();
        info!("Calling start at time {}", Local::now());
        let handler = server.start_with_port(0);
        sleep(Duration::from_secs(1)).await;
        info!("Calling stop at time {}", Local::now());
        handler.await.unwrap().stop().await
    }

    #[derive(thiserror::Error, Debug)]
    enum SomeError {
        #[error("A variant of this error")]
        Variant,
    }

    #[tokio::test]
    async fn test_server_start_with_failing_embedding() {
        let tmp_dir = tempdir().unwrap();
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.encode(tmp_dir.path().join("g")).unwrap();

        global_info_logger();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, None)
            .await
            .unwrap();
        let template = DocumentTemplate {
            node_template: Some("{{ name }}".to_owned()),
            ..Default::default()
        };
        let model = OpenAIEmbeddings {
            api_base: Some("wrong-api-base".to_owned()),
            ..Default::default()
        };
        server.vectorise_all_graphs(&template, model).await.unwrap();
        let handler = server.start_with_port(0);
        sleep(Duration::from_secs(5)).await;
        handler.await.unwrap().stop().await
    }
}
