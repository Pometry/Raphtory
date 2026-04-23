use crate::{
    auth::{AuthenticatedGraphQL, MutationAuth},
    auth_policy::AuthorizationPolicy,
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
    GQLError,
};
use config::ConfigError;
use once_cell::sync::Lazy;
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
    db::api::storage::storage::Config,
    vectors::{storage::OpenAIEmbeddings, template::DocumentTemplate},
};
use serde_json::json;
use std::{
    fs::create_dir_all,
    future::Future,
    ops::Deref,
    path::{Path, PathBuf},
    pin::Pin,
    sync::RwLock,
    task::{Context, Poll},
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
use tracing::{debug, info, warn};
use tracing_subscriber::{
    fmt, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, Registry,
};
use url::ParseError;

pub const DEFAULT_PORT: u16 = 1736;

type ServerExtensionFn = Box<dyn Fn(GraphServer, Option<&Path>) -> GraphServer + Send + Sync>;

static SERVER_EXTENSION: Lazy<RwLock<Option<ServerExtensionFn>>> = Lazy::new(|| RwLock::new(None));

pub fn register_server_extension(f: ServerExtensionFn) {
    *SERVER_EXTENSION.write().unwrap() = Some(f);
}

pub fn apply_server_extension(server: GraphServer, path: Option<&Path>) -> GraphServer {
    match SERVER_EXTENSION.read().unwrap().as_ref() {
        Some(ext) => ext(server, path),
        None => server,
    }
}

pub fn has_server_extension() -> bool {
    SERVER_EXTENSION.read().unwrap().is_some()
}

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

type SchemaDataInjector = std::sync::Arc<
    dyn Fn(async_graphql::dynamic::SchemaBuilder) -> async_graphql::dynamic::SchemaBuilder
        + Send
        + Sync,
>;

/// A struct for defining and running a Raphtory GraphQL server
#[derive(Clone)]
pub struct GraphServer {
    data: Data,
    config: AppConfig,
    schema_data: Vec<SchemaDataInjector>,
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
        graph_config: Config,
    ) -> IoResult<Self> {
        if !work_dir.exists() {
            create_dir_all(&work_dir)?;
        }
        let config = load_config(app_config, config_path).map_err(ServerError::ConfigError)?;
        let data = Data::new(work_dir.as_path(), &config, graph_config);
        Ok(Self {
            data,
            config,
            schema_data: Vec::new(),
        })
    }

    /// Returns the working directory for this server.
    pub fn work_dir(&self) -> &Path {
        &self.data.work_dir
    }

    pub fn turn_off_index(&mut self) {
        self.data.create_index = false; // FIXME: why does this exist yet?
    }

    /// Set the authorization policy used for graph access checks.
    pub fn with_auth_policy(mut self, policy: std::sync::Arc<dyn AuthorizationPolicy>) -> Self {
        self.data.set_auth_policy(policy);
        self
    }

    /// Inject arbitrary typed data into the GQL schema (accessible via `ctx.data::<T>()`).
    pub fn with_schema_data<T: std::any::Any + Send + Sync + 'static>(mut self, data: T) -> Self {
        let data = std::sync::Arc::new(std::sync::Mutex::new(Some(data)));
        self.schema_data.push(std::sync::Arc::new(move |sb| {
            let data = data
                .lock()
                .unwrap()
                .take()
                .expect("schema data injector called more than once");
            sb.data(data)
        }));
        self
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
    ) -> Result<(), GQLError> {
        let vector_cache = self.data.vector_cache.resolve().await?;
        let model = vector_cache.openai(embeddings.into()).await?;
        for folder in self.data.get_all_graph_folders() {
            self.data
                .vectorise_folder(&folder, template, model.clone()) // TODO: avoid clone, just ask for a ref
                .await?;
        }
        Ok(())
    }

    /// Vectorise the graph 'name' in the server working directory.
    ///
    /// Arguments:
    ///   * path - the path of the graph to vectorise.
    ///   * template - the template to use for creating documents.
    pub async fn vectorise_graph(
        &self,
        path: &str,
        template: &DocumentTemplate,
        embeddings: OpenAIEmbeddings,
    ) -> Result<(), GQLError> {
        let vetor_cache = self.data.vector_cache.resolve();
        let model = vetor_cache.await?.openai(embeddings.into()).await?;
        let folder = ExistingGraphFolder::try_from(self.data.work_dir.clone(), path)?;
        self.data.vectorise_folder(&folder, template, model).await
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

        // Otherwise evictions are only triggered when the cache is actively touched
        let cache_clone = self.data.cache.clone();
        let cache_task: AbortOnDrop<()> = AbortOnDrop(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                cache_clone.run_pending_tasks().await;
            }
        }));

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
        let server_result = AbortOnDrop(tokio::spawn(server_task));

        Ok(RunningGraphServer {
            signal_sender,
            server_result,
            cache_task,
        })
    }

    async fn generate_endpoint(
        &self,
        tracer: Option<Tracer>,
    ) -> Result<CompressionEndpoint<CorsEndpoint<Route>>, ServerError> {
        let schema_cfg = &self.config.schema;
        let mut schema_builder = App::create_schema()
            .data(self.data.clone())
            .data(self.config.concurrency.clone());
        for inject in &self.schema_data {
            schema_builder = inject(schema_builder);
        }
        schema_builder = schema_builder.extension(MutationAuth);
        if let Some(depth) = schema_cfg.max_query_depth {
            schema_builder = schema_builder.limit_depth(depth);
        }
        if let Some(complexity) = schema_cfg.max_query_complexity {
            schema_builder = schema_builder.limit_complexity(complexity);
        }
        if let Some(recursive_depth) = schema_cfg.max_recursive_depth {
            schema_builder = schema_builder.limit_recursive_depth(recursive_depth);
        }
        if let Some(max_directives) = schema_cfg.max_directives_per_field {
            schema_builder = schema_builder.limit_directives(max_directives);
        }
        if schema_cfg.disable_introspection {
            schema_builder = schema_builder.disable_introspection();
        }
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
                    AuthenticatedGraphQL::new(schema, self.config.clone()),
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

#[derive(Debug)]
pub struct AbortOnDrop<T>(pub JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Deref for AbortOnDrop<T> {
    type Target = JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// A Raphtory server handler
#[derive(Debug)]
pub struct RunningGraphServer {
    signal_sender: Sender<()>,
    server_result: AbortOnDrop<IoResult<()>>,
    cache_task: AbortOnDrop<()>,
}

impl RunningGraphServer {
    /// Stop the server.
    pub async fn stop(&self) {
        self.cache_task.abort();
        let _ignored = self.signal_sender.send(()).await;
    }

    /// Wait until server completion.
    pub async fn wait(self) -> IoResult<()> {
        self.server_result.await.expect("Server panicked")
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
        db::api::storage::storage::Config,
        prelude::{AdditionOps, Graph, StableEncode, NO_PROPS},
        vectors::{storage::OpenAIEmbeddings, template::DocumentTemplate},
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};
    use tracing::info;

    #[tokio::test]
    async fn test_server_start_stop() {
        global_info_logger();
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, None, Config::default())
            .await
            .unwrap();
        info!("Calling start at time {}", Local::now());
        let handler = server.start_with_port(0);
        sleep(Duration::from_secs(1)).await;
        info!("Calling stop at time {}", Local::now());
        handler.await.unwrap().stop().await
    }

    #[tokio::test]
    async fn test_server_start_with_failing_embedding() {
        let tmp_dir = tempdir().unwrap();
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
        graph.encode(tmp_dir.path().join("g")).unwrap();

        global_info_logger();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, None, Config::default())
            .await
            .unwrap();
        let template = DocumentTemplate {
            node_template: Some("{{ name }}".to_owned()),
            ..Default::default()
        };
        let model = OpenAIEmbeddings::new("whatever", "wrong-api-base");
        let result = server.vectorise_all_graphs(&template, model).await;
        assert!(result.is_err());
        let handler = server.start_with_port(0);
        sleep(Duration::from_secs(5)).await;
        handler.await.unwrap().stop().await
    }
}
