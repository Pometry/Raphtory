#![allow(dead_code)]

use crate::{
    data::Data,
    model::{algorithm::Algorithm, App},
    observability::tracing::create_tracer_from_env,
    routes::{graphql_playground, health},
    vectors::{GraphEntity, VectorStore},
};
use async_graphql_poem::GraphQL;
use chrono::NaiveDateTime;
use itertools::Itertools;
use poem::{get, listener::TcpListener, middleware::Cors, EndpointExt, Route, Server};
use raphtory::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, LayerOps, VertexViewOps},
};
use std::{collections::HashMap, ops::Deref, path::Path};
use tokio::{io::Result as IoResult, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub struct RaphtoryServer {
    data: Data,
}

impl RaphtoryServer {
    pub fn from_map(graphs: HashMap<String, DynamicGraph>) -> Self {
        let data = Data::from_map(graphs);
        Self { data }
    }

    pub fn from_directory(graph_directory: &str) -> Self {
        let data = Data::from_directory(graph_directory);
        Self { data }
    }

    pub fn from_map_and_directory(
        graphs: HashMap<String, DynamicGraph>,
        graph_directory: &str,
    ) -> Self {
        let data = Data::from_map_and_directory(graphs, graph_directory);
        Self { data }
    }

    pub async fn with_vectorized(self, graph_names: Vec<String>, cache_dir: &Path) -> Self {
        {
            let graphs_map = self.data.graphs.read();
            let mut stores_map = self.data.vector_stores.write();

            for graph_name in graph_names {
                let graph_cache = cache_dir.join(&graph_name);
                let graph = graphs_map.get(&graph_name).unwrap().deref().clone();

                println!("Loading embeddings for {graph_name} using cache from {graph_cache:?}");
                let vector_store =
                    VectorStore::load_graph(graph, &graph_cache, &node_template, &edge_template)
                        .await;
                stores_map.insert(graph_name, vector_store);
            }
        }

        println!("Embeddings were loeaded successfully");

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

fn node_template(vertex: &VertexView<DynamicGraph>) -> String {
    let name = vertex
        .properties()
        .get("name")
        .map(|prop| prop.to_string())
        .unwrap_or_else(|| vertex.name());
    let node_type = vertex.properties().get("type").map(|prop| prop.to_string());
    let definition = match node_type {
        Some(node_type) => match node_type.as_str() {
            "user" => format!("{name} is an employee of Pometry"),
            "issue" => format!("{name} is an issue created by the Pometry team"),
            "sprint" => format!("{name} is a sprint carried out by the Pometry team"),
            unknown_type => format!("{name} is a {unknown_type}"),
        },
        None => format!("{name} is an unknown entity"),
    };

    let property_list = vertex.generate_property_list(vec!["type".to_owned()]);

    format!("{definition} with the following details:\n{property_list}")
}

fn edge_template(edge: &EdgeView<DynamicGraph>) -> String {
    let vertex_name = |vertex: VertexView<DynamicGraph>| {
        vertex
            .properties()
            .get("name")
            .map(|prop| prop.to_string())
            .unwrap_or_else(|| vertex.name())
    };
    let src = vertex_name(edge.src());
    let dst = vertex_name(edge.dst());

    edge.layer_names()
        .iter()
        .map(|layer| {
            let fact = match layer.as_str() {
                "reported" => format!("{src} was assigned to report issue {dst}"),
                "created" => format!("{src} created issue {dst}"),
                "author" => format!("{src} authored issue {dst}"),
                "has" => format!("{dst} was included in sprint {src}"),
                "assigned" => format!("{src} was assigned to work on issue {dst}"),
                "blocks" => format!("{src} blocks {dst}"),
                "clones" => format!("issue {src} was cloned into {dst}"),
                "splits" => format!("issue {src} was split"),
                "_default" => format!("{src} had an unknown relationship with {dst}"),
                _ => format!("{src} had an unknown relationship with {dst}"),
            };

            let format_time = |millis: &i64| {
                NaiveDateTime::from_timestamp_millis(*millis)
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            };

            let times = edge
                .layer(layer)
                .unwrap()
                .history()
                .iter()
                .map(format_time)
                .next()
                .unwrap();
            //.join(",");

            format!("{fact} at time: {times}")
        })
        .intersperse("\n".to_owned())
        .collect()
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
