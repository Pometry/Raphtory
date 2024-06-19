use common::run_graph_ops_benches;
use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::{
    core::utils::hashing::calculate_hash,
    graph_loader::{
        example::sx_superuser_graph::{sx_superuser_file, sx_superuser_graph, TEdge},
        source::csv_loader::CsvLoader,
    },
    prelude::*,
};

mod common;

pub fn graph(c: &mut Criterion) {
    let group_name = "analysis_graph";
    let graph = sx_superuser_graph().unwrap();
    let layered_graph = layered_sx_super_user_graph(Some(10)).unwrap();

    run_graph_ops_benches(c, group_name, graph, layered_graph);
}

/// Load the SX SuperUser dataset into a graph and return it
///
/// Returns:
///
/// - A Result containing the graph or an error, with edges randomly assigned to layers
fn layered_sx_super_user_graph(
    num_layers: Option<usize>,
) -> Result<Graph, Box<dyn std::error::Error>> {
    let graph = Graph::new();
    CsvLoader::new(sx_superuser_file()?)
        .set_delimiter(" ")
        .load_into_graph(&graph, |edge: TEdge, g: &Graph| {
            if let Some(layer) = num_layers
                .map(|num_layers| calculate_hash(&(edge.src_id, edge.dst_id)) % num_layers as u64)
                .map(|id| id.to_string())
            {
                g.add_edge(
                    edge.time,
                    edge.src_id,
                    edge.dst_id,
                    NO_PROPS,
                    Some(layer.as_str()),
                )
                .expect("Error: Unable to add edge");
            } else {
                g.add_edge(edge.time, edge.src_id, edge.dst_id, NO_PROPS, None)
                    .expect("Error: Unable to add edge");
            }
        })?;

    Ok(graph)
}

criterion_group!(benches, graph);
criterion_main!(benches);
