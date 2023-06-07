use crate::server::RaphtoryServer;
use dotenv::dotenv;
use std::env;

mod data;
mod model;
mod observability;
mod routes;
mod server;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let graph_directory = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    RaphtoryServer::new(&graph_directory).run().await.unwrap()
}
