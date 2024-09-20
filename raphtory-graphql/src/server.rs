#![allow(dead_code)]

use crate::{
    data::Data,
    model::App,
    observability::tracing::create_tracer_from_env,
    routes::{graphql_playground, health},
};
use async_graphql_poem::GraphQL;
use itertools::Itertools;
use poem::{
    get,
    listener::TcpListener,
    middleware::{CookieJarManager, CookieJarManagerEndpoint, Cors, CorsEndpoint},
    EndpointExt, Route, Server,
};
use raphtory::{
    db::api::view::IntoDynamic,
    vectors::{template::DocumentTemplate, vectorisable::Vectorisable, EmbeddingFunction},
};

use crate::{
    model::plugins::{
        mutation::Mutation, mutation_entry_point::MutationEntryPoint, query::Query,
        query_entry_point::QueryEntryPoint,
    },
    server_config::{load_config, AppConfig, LoggingConfig},
};
use config::ConfigError;
use std::{
    fs,
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
    task::JoinHandle,
};
use tracing_subscriber::{
    layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, FmtSubscriber, Registry,
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
}

impl From<ServerError> for io::Error {
    fn from(error: ServerError) -> Self {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

/// A struct for defining and running a Raphtory GraphQL server
pub struct GraphServer {
    data: Data,
    configs: AppConfig,
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
        let configs =
            load_config(app_config, config_path).map_err(|err| ServerError::ConfigError(err))?;
        let data = Data::new(work_dir.as_path(), &configs);

        Ok(Self { data, configs })
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
    pub async fn with_vectorised<F: EmbeddingFunction + Clone + 'static>(
        self,
        graph_names: Option<Vec<String>>,
        embedding: F,
        cache: &Path,
        template: DocumentTemplate,
    ) -> IoResult<Self> {
        let graphs = &self.data.global_plugins.graphs;
        let stores = &self.data.global_plugins.vectorised_graphs;

        graphs.write().extend(
            self.data
                .get_graphs_from_work_dir()
                .map_err(|err| ServerError::CacheError(err.to_string()))?,
        );

        let graph_names = graph_names.unwrap_or_else(|| {
            let all_graph_names = {
                let read_guard = graphs.read();
                read_guard
                    .iter()
                    .map(|(graph_name, _)| graph_name.to_string())
                    .collect_vec()
            };
            all_graph_names
        });

        for graph_name in graph_names {
            let graph_cache = cache.join(&graph_name);
            let graph = graphs.read().get(&graph_name).unwrap().clone();
            println!("Loading embeddings for {graph_name} using cache from {graph_cache:?}");
            let vectorised = graph
                .into_dynamic()
                .vectorise(
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

        Ok(self)
    }

    pub fn register_algorithm<'a, E: QueryEntryPoint<'a> + 'static, A: Query<'a, E> + 'static>(
        self,
        name: &str,
    ) -> Self {
        E::lock_plugins().insert(name.to_string(), Box::new(A::register_query));
        self
    }

    pub fn register_mutation_plugins<
        'a,
        E: MutationEntryPoint<'a> + 'static,
        A: Mutation<'a, E> + 'static,
    >(
        self,
        name: &str,
    ) -> Self {
        E::lock_plugins().insert(name.to_string(), Box::new(A::register_algo));
        self
    }

    /// Start the server on the default port and return a handle to it.
    pub async fn start(self) -> IoResult<RunningGraphServer> {
        self.start_with_port(1736).await
    }

    /// Start the server on the port `port` and return a handle to it.
    pub async fn start_with_port(self, port: u16) -> IoResult<RunningGraphServer> {
        fn configure_logger(configs: &LoggingConfig) {
            let log_level = &configs.log_level;
            let filter = EnvFilter::new(log_level);
            let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
            if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
                eprintln!(
                    "Log level cannot be updated within the same runtime environment: {}",
                    err
                );
            }
        }

        configure_logger(&self.configs.logging);

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

        let app: CorsEndpoint<CookieJarManagerEndpoint<Route>> = self.generate_endpoint().await?;

        let (signal_sender, signal_receiver) = mpsc::channel(1);

        println!("Playground: http://localhost:{port}");
        let server_task = Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run_with_graceful_shutdown(app, server_termination(signal_receiver), None);
        let server_result = tokio::spawn(server_task);

        Ok(RunningGraphServer {
            signal_sender,
            server_result,
        })
    }

    async fn generate_endpoint(self) -> IoResult<CorsEndpoint<CookieJarManagerEndpoint<Route>>> {
        let schema_builder = App::create_schema();
        let schema_builder = schema_builder.data(self.data);
        let schema = schema_builder.finish().unwrap();

        let app = Route::new()
            .at("/", get(graphql_playground).post(GraphQL::new(schema)))
            .at("/health", get(health))
            .with(CookieJarManager::new())
            .with(Cors::new());
        Ok(app)
    }

    // async fn generate_microsoft_endpoint_with_auth(
    //     self,
    //     enable_tracing: bool,
    //     port: u16,
    // ) -> IoResult<CorsEndpoint<CookieJarManagerEndpoint<Route>>> {
    //     let schema_builder = App::create_schema();
    //     let schema_builder = schema_builder.data(self.data);
    //     let schema = if enable_tracing {
    //         let schema_builder = schema_builder.extension(ApolloTracing);
    //         schema_builder.finish().unwrap()
    //     } else {
    //         schema_builder.finish().unwrap()
    //     };
    //
    //     dotenv().ok();
    //     let client_id = self
    //         .configs
    //         .auth
    //         .client_id
    //         .ok_or(ServerError::MissingClientId)?;
    //     let client_secret = self
    //         .configs
    //         .auth
    //         .client_secret
    //         .ok_or(ServerError::MissingClientSecret)?;
    //     let tenant_id = self
    //         .configs
    //         .auth
    //         .tenant_id
    //         .ok_or(ServerError::MissingTenantId)?;
    //
    //     let client_id = ClientId::new(client_id);
    //     let client_secret = ClientSecret::new(client_secret);
    //
    //     let auth_url = AuthUrl::new(format!(
    //         "https://login.microsoftonline.com/{}/oauth2/v2.0/authorize",
    //         tenant_id.clone()
    //     ))
    //     .map_err(|e| ServerError::FailedToParseUrl(e))?;
    //     let token_url = TokenUrl::new(format!(
    //         "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
    //         tenant_id.clone()
    //     ))
    //     .map_err(|e| ServerError::FailedToParseUrl(e))?;
    //
    //     println!("Loading client");
    //     let client = BasicClient::new(
    //         client_id.clone(),
    //         Some(client_secret.clone()),
    //         auth_url,
    //         Some(token_url),
    //     )
    //     .set_redirect_uri(
    //         RedirectUrl::new(format!(
    //             "http://localhost:{}/auth/callback",
    //             port.to_string()
    //         ))
    //         .map_err(|e| ServerError::FailedToParseUrl(e))?,
    //     );
    //
    //     println!("Fetching JWKS");
    //     let jwks = get_jwks()
    //         .await
    //         .map_err(|_| ServerError::FailedToFetchJWKS)?;
    //
    //     let app_state = AppState {
    //         oauth_client: Arc::new(client),
    //         csrf_state: Arc::new(Mutex::new(HashMap::new())),
    //         pkce_verifier: Arc::new(Mutex::new(HashMap::new())),
    //         jwks: Arc::new(jwks),
    //     };
    //
    //     let token_middleware = TokenMiddleware::new(Arc::new(app_state.clone()));
    //
    //     println!("Making app");
    //     let app = Route::new()
    //         .at(
    //             "/",
    //             get(graphql_playground)
    //                 .post(GraphQL::new(schema))
    //                 .with(token_middleware.clone()),
    //         )
    //         .at("/health", get(health))
    //         .at("/login", login.data(app_state.clone()))
    //         .at("/auth/callback", auth_callback.data(app_state.clone()))
    //         .at(
    //             "/verify",
    //             verify
    //                 .data(app_state.clone())
    //                 .with(token_middleware.clone()),
    //         )
    //         .at("/logout", logout.with(token_middleware.clone()))
    //         .with(CookieJarManager::new())
    //         .with(Cors::new());
    //     println!("App done");
    //     Ok(app)
    // }

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

    use crate::server::GraphServer;
    use chrono::prelude::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_server_start_stop() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, None).unwrap();
        println!("Calling start at time {}", Local::now());
        let handler = server.start_with_port(0);
        sleep(Duration::from_secs(1)).await;
        println!("Calling stop at time {}", Local::now());
        handler.await.unwrap().stop().await
    }
}
