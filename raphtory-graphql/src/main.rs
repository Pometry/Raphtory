use std::io::Result as IoResult;

use crate::server::GraphServer;
mod auth;
mod cli;
mod config;
mod data;
mod embeddings;
mod graph;
mod model;
mod observability;
mod paths;
mod rayon;
mod routes;
mod server;
mod url_encode;

#[tokio::main]
async fn main() -> IoResult<()> {
    cli::cli().await
}
