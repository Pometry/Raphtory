use crate::server::RaphtoryServer;
use std::{
    env,
    path::{Path, PathBuf},
};

mod azure_auth;
mod data;
mod model;
mod observability;
mod routes;
mod server;
mod server_config;

extern crate base64_compat as base64_compat;

#[tokio::main]
async fn main() {
    let work_dir = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    let work_dir = PathBuf::from(&work_dir);

    let args: Vec<String> = env::args().collect();
    let use_auth = args.contains(&"--server".to_string());

    if use_auth {
        RaphtoryServer::new(work_dir, None, None)
            .run_with_auth(false)
            .await
            .unwrap();
    } else {
        RaphtoryServer::new(work_dir, None, None)
            .run(false)
            .await
            .unwrap();
    }
}
