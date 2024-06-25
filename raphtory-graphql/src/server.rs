#![allow(dead_code)]

use crate::{
    azure_auth::{
        common::{auth_callback, get_jwks, login, logout, verify, AppState},
        token_middleware::TokenMiddleware,
    },
    data::Data,
    model::{
        algorithms::{algorithm::Algorithm, algorithm_entry_point::AlgorithmEntryPoint},
        App,
    },
    observability::tracing::create_tracer_from_env,
    routes::{graphql_playground, health},
};
use async_graphql::extensions::ApolloTracing;
use async_graphql_poem::GraphQL;
use dotenv::dotenv;
use itertools::Itertools;
use oauth2::{basic::BasicClient, AuthUrl, ClientId, ClientSecret, RedirectUrl, TokenUrl};
use poem::{
    get,
    listener::TcpListener,
    middleware::{CookieJarManager, CookieJarManagerEndpoint, Cors, CorsEndpoint},
    EndpointExt, Route, Server,
};
use raphtory::{
    db::api::view::{DynamicGraph, IntoDynamic, MaterializedGraph},
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorisable::Vectorisable,
        EmbeddingFunction,
    },
};

use crate::{
    data::get_graphs_from_work_dir,
    server_config::{load_config, AppConfig, CacheConfig, LoggingConfig},
};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
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
    configs: AppConfig,
}

impl RaphtoryServer {
    pub fn new(
        work_dir: &Path,
        graphs: Option<HashMap<String, MaterializedGraph>>,
        graph_paths: Option<Vec<PathBuf>>,
        cache_config: Option<CacheConfig>,
        config_path: Option<&Path>,
    ) -> Self {
        if !work_dir.exists() {
            fs::create_dir_all(work_dir).unwrap();
        }

        let configs = load_config(cache_config, config_path).expect("Failed to load configs");

        let data = Data::new(work_dir, graphs, graph_paths, &configs);

        Self { data, configs }
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
        let work_dir = Path::new(&self.data.work_dir);
        let graphs = &self.data.global_plugins.graphs;
        let stores = &self.data.global_plugins.vectorised_graphs;

        graphs
            .write()
            .extend(get_graphs_from_work_dir(work_dir).expect("Failed to load graphs"));

        let template = template
            .map(|template| Arc::new(template) as Arc<dyn DocumentTemplate<DynamicGraph>>)
            .unwrap_or(Arc::new(DefaultTemplate));

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
    pub async fn start(
        self,
        log_level: Option<&str>,
        enable_tracing: bool,
        enable_auth: bool,
    ) -> RunningRaphtoryServer {
        self.start_with_port(1736, log_level, enable_tracing, enable_auth)
            .await
    }

    /// Start the server on the port `port` and return a handle to it.
    pub async fn start_with_port(
        self,
        port: u16,
        log_level: Option<&str>,
        enable_tracing: bool,
        enable_auth: bool,
    ) -> RunningRaphtoryServer {
        fn parse_log_level(input: &str) -> Option<String> {
            // Parse log level from string
            let level: Result<Level, ParseLevelError> = input.trim().parse();
            match level {
                Ok(level) => Some(level.to_string()),
                Err(_) => None,
            }
        }

        fn setup_logger_from_loglevel(log_level: String) {
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

        fn setup_logger_from_config(configs: &LoggingConfig) {
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

        fn configure_logger(log_level: Option<&str>, configs: &LoggingConfig) {
            if let Some(log_level) = log_level {
                if let Some(log_level) = parse_log_level(log_level) {
                    setup_logger_from_loglevel(log_level);
                } else {
                    setup_logger_from_config(configs);
                }
            } else {
                setup_logger_from_config(configs);
            }
        }

        configure_logger(log_level, &self.configs.logging);

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

        let app: CorsEndpoint<CookieJarManagerEndpoint<Route>> = if enable_auth {
            println!("Generating endpoint with auth");
            self.generate_microsoft_endpoint_with_auth(enable_tracing, port)
                .await
        } else {
            self.generate_endpoint(enable_tracing).await
        };

        let (signal_sender, signal_receiver) = mpsc::channel(1);

        println!("Playground: http://localhost:{port}");
        let server_task = Server::new(TcpListener::bind(format!("127.0.0.1:{port}")))
            .run_with_graceful_shutdown(app, server_termination(signal_receiver), None);
        let server_result = tokio::spawn(server_task);

        RunningRaphtoryServer {
            signal_sender,
            server_result,
        }
    }

    async fn generate_endpoint(
        self,
        enable_tracing: bool,
    ) -> CorsEndpoint<CookieJarManagerEndpoint<Route>> {
        let schema_builder = App::create_schema();
        let schema_builder = schema_builder.data(self.data);
        let schema = if enable_tracing {
            let schema_builder = schema_builder.extension(ApolloTracing);
            schema_builder.finish().unwrap()
        } else {
            schema_builder.finish().unwrap()
        };

        let app = Route::new()
            .at("/", get(graphql_playground).post(GraphQL::new(schema)))
            .at("/health", get(health))
            .with(CookieJarManager::new())
            .with(Cors::new());
        app
    }

    async fn generate_microsoft_endpoint_with_auth(
        self,
        enable_tracing: bool,
        port: u16,
    ) -> CorsEndpoint<CookieJarManagerEndpoint<Route>> {
        let schema_builder = App::create_schema();
        let schema_builder = schema_builder.data(self.data);
        let schema = if enable_tracing {
            let schema_builder = schema_builder.extension(ApolloTracing);
            schema_builder.finish().unwrap()
        } else {
            schema_builder.finish().unwrap()
        };

        dotenv().ok();
        println!("Loading env");
        let client_id_str = env::var("CLIENT_ID").expect("CLIENT_ID not set");
        let client_secret_str = env::var("CLIENT_SECRET").expect("CLIENT_SECRET not set");
        let tenant_id_str = env::var("TENANT_ID").expect("TENANT_ID not set");

        let client_id = ClientId::new(client_id_str);
        let client_secret = ClientSecret::new(client_secret_str);

        let auth_url = AuthUrl::new(format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/authorize",
            tenant_id_str.clone()
        ))
        .expect("Invalid authorization endpoint URL");
        let token_url = TokenUrl::new(format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            tenant_id_str.clone()
        ))
        .expect("Invalid token endpoint URL");

        println!("Loading client");
        let client = BasicClient::new(
            client_id.clone(),
            Some(client_secret.clone()),
            auth_url,
            Some(token_url),
        )
        .set_redirect_uri(
            RedirectUrl::new(format!(
                "http://localhost:{}/auth/callback",
                port.to_string()
            ))
            .expect("Invalid redirect URL"),
        );

        println!("Fetching JWKS");
        let jwks = get_jwks().await.expect("Failed to fetch JWKS");

        let app_state = AppState {
            oauth_client: Arc::new(client),
            csrf_state: Arc::new(Mutex::new(HashMap::new())),
            pkce_verifier: Arc::new(Mutex::new(HashMap::new())),
            jwks: Arc::new(jwks),
        };

        let token_middleware = TokenMiddleware::new(Arc::new(app_state.clone()));

        println!("Making app");
        let app = Route::new()
            .at(
                "/",
                get(graphql_playground)
                    .post(GraphQL::new(schema))
                    .with(token_middleware.clone()),
            )
            .at("/health", get(health))
            .at("/login", login.data(app_state.clone()))
            .at("/auth/callback", auth_callback.data(app_state.clone()))
            .at(
                "/verify",
                verify
                    .data(app_state.clone())
                    .with(token_middleware.clone()),
            )
            .at("/logout", logout.with(token_middleware.clone()))
            .with(CookieJarManager::new())
            .with(Cors::new());
        println!("App done");
        app
    }

    /// Run the server on the default port until completion.
    pub async fn run(self, log_level: Option<&str>, enable_tracing: bool) -> IoResult<()> {
        self.start(log_level, enable_tracing, false)
            .await
            .wait()
            .await
    }

    pub async fn run_with_auth(
        self,
        log_level: Option<&str>,
        enable_tracing: bool,
    ) -> IoResult<()> {
        self.start(log_level, enable_tracing, true)
            .await
            .wait()
            .await
    }

    /// Run the server on the port `port` until completion.
    pub async fn run_with_port(
        self,
        port: u16,
        log_level: Option<&str>,
        enable_tracing: bool,
    ) -> IoResult<()> {
        self.start_with_port(port, log_level, enable_tracing, false)
            .await
            .wait()
            .await
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
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_server_stop() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let server = RaphtoryServer::new(tmp_dir.path(), None, None, None, None);
        println!("calling start at time {}", Local::now());
        let handler = server.start_with_port(0, None, false, false);
        sleep(Duration::from_secs(1)).await;
        println!("Calling stop at time {}", Local::now());
        handler.await.stop().await
    }
}
