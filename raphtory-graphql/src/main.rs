use crate::server::RaphtoryServer;
use dotenv::dotenv;
use std::env;

mod data;
mod model;
mod observability;
mod routes;
mod server;

fn default_cache_dir() -> String {
    "".to_owned()
}

#[tokio::main]
async fn main() {
    let graph_directory = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    RaphtoryServer::from_directory(&graph_directory)
        .run()
        .await
        .unwrap()
}
