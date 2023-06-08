use crate::data::Data;
use crate::model::algorithm::Algorithm;
use crate::model::QueryRoot;
use crate::observability::tracing::create_tracer_from_env;
use crate::routes::{graphql_playground, health};
use async_graphql_poem::GraphQL;
use dynamic_graphql::App;
use poem::listener::TcpListener;
use poem::middleware::Cors;
use poem::{get, EndpointExt, Route, Server};
use tokio::io::Result as IoResult;
use tokio::signal;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

pub struct RaphtoryServer {
    data: Data,
}

impl RaphtoryServer {
    pub fn new(graph_directory: &str) -> Self {
        let data = Data::load(graph_directory);
        Self { data }
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

        match create_tracer_from_env() {
            Some(tracer) => registry
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .expect("Failed to register tracer with registry"),
            None => registry
                .try_init()
                .expect("Failed to register tracer with registry"),
        }

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
