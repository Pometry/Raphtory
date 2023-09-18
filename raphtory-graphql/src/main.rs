use crate::server::RaphtoryServer;
use clap::Parser;
use dotenv::dotenv;
use raphtory::prelude::Graph;
use raphtory::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, LayerOps, VertexViewOps},
    vectors::GraphEntity,
};
use std::{env, path::PathBuf};

mod data;
mod embeddings;
mod model;
mod observability;
mod routes;
mod server;

#[derive(Parser, Debug)]
struct Args {
    /// graphs to vectorize for similarity search
    #[arg(short, long, num_args = 0.., value_delimiter = ' ')]
    vectorize: Vec<String>,

    /// directory to use to store the embbeding cache
    #[arg(short, long, default_value_t = ("".to_string()))]
    cache: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let graphs_to_vectorize = args.vectorize;
    let cache_dir = args.cache;
    // let graphs_to_vectorize = vec!["jira".to_owned()];
    // let cache_dir = "/tmp/jira-cache-gte-small-batching";
    assert!(
        graphs_to_vectorize.len() == 0 || cache_dir != "",
        "Setting up a cache directory is mandatory if some graphs need to be vectorized"
    );

    dotenv().ok();
    let graph_directory = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    RaphtoryServer::from_directory(&graph_directory)
        // .with_vectorized(
        //     graphs_to_vectorize,
        //     embeddings::openai_embedding,
        //     &PathBuf::from(cache_dir),
        //     None,
        // )
        // .await // FIXME: re-enable, probably have two separate methods: with_vectorized, with_templates
        // FIXME: maybe we should vectorize the graphs only when run() is called
        .run()
        .await
        .unwrap()
}
