use crate::server::RaphtoryServer;
use std::env;

mod azure_auth;
mod data;
mod model;
mod observability;
mod routes;
mod server;

extern crate base64_compat as base64_compat;

#[tokio::main]
async fn main() {
    let graph_directory = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    let config_path = "config.toml";

    // RaphtoryServer::from_directory(&graph_directory)
    //     .run(config_path, false)
    //     .await
    //     .unwrap()

    RaphtoryServer::from_directory(&graph_directory)
        .run_with_auth(config_path, false)
        .await
        .unwrap()
}
