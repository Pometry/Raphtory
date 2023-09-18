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
        .with_vectorized(
            graphs_to_vectorize,
            embeddings::openai_embedding,
            &PathBuf::from(cache_dir),
            Some((node_template, edge_template)),
        )
        .await
        // FIXME: maybe we should vectorize the graphs only when run() is called
        .run()
        .await
        .unwrap()
}

use itertools::Itertools;

fn node_template(vertex: &VertexView<Graph>) -> String {
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

    let property_list =
        vertex.generate_property_list(&format_time, vec!["type"], vec!["description", "goal"]);

    let doc = format!("{definition} with the following details:\n{property_list}");
    // println!("----------------\n{doc}\n----------------\n");
    doc
}

fn edge_template(edge: &EdgeView<Graph>) -> String {
    let vertex_name = |vertex: VertexView<Graph>| {
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
                "author" => format!("{src} edited the details of issue {dst}"),
                "has" => format!("{dst} was included in sprint {src}"),
                "assigned" => format!("{src} was assigned to work on issue {dst}"),
                "blocks" => format!("{src} blocks {dst}"),
                "clones" => format!("issue {src} was cloned into {dst}"),
                "splits" => format!("issue {src} was split"),
                "_default" => format!("{src} had an unknown relationship with {dst}"),
                _ => format!("{src} had an unknown relationship with {dst}"),
            };

            let times = edge
                .layer(layer)
                .unwrap()
                .history()
                .iter()
                .copied()
                .map(format_time)
                .next()
                .unwrap();
            //.join(",");

            format!("{fact} at time: {times}")
        })
        .intersperse("\n".to_owned())
        .collect()
}

use chrono::NaiveDateTime;

fn format_time(millis: i64) -> String {
    if millis == 0 {
        "unknown time".to_owned()
    } else {
        NaiveDateTime::from_timestamp_millis(millis)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    }
}
