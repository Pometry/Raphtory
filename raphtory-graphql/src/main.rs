use crate::server::RaphtoryServer;
use std::env;
use tracing::Level;

mod data;
mod model;
mod observability;
mod routes;
mod server;

#[tokio::main]
async fn main() {
    let graph_directory = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    let config_path = "config.toml";

    RaphtoryServer::from_directory(&graph_directory)
        .run(config_path)
        .await
        .unwrap()
}
