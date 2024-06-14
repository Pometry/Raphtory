use common::{run_analysis_benchmarks, run_materialize};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{seq::*, SeedableRng};
use raphtory::{
    core::utils::hashing::calculate_hash,
    db::api::view::*,
    graph_loader::{
        example::sx_superuser_graph::{sx_superuser_file, sx_superuser_graph, TEdge},
        source::csv_loader::CsvLoader,
    },
    prelude::*,
};

mod common;

pub fn graph(c: &mut Criterion) {
    let group_name = "analysis_graph";
    let mut graph_group = c.benchmark_group(group_name);
    let graph = sx_superuser_graph().unwrap();
    let make_graph = || graph.clone();
    run_analysis_benchmarks(&mut graph_group, make_graph, None);
    graph_group.finish();

    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);

    let group_name = "analysis_graph_window_100";
    let make_graph = || graph.window(i64::MIN, i64::MAX);
    let mut graph_window_group_100 = c.benchmark_group(group_name);
    graph_window_group_100.sample_size(10);
    run_analysis_benchmarks(&mut graph_window_group_100, make_graph, None);
    graph_window_group_100.finish();

    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);

    // graph windowed
    let group_name = "analysis_graph_window_10";
    let mut graph_window_group_10 = c.benchmark_group(group_name);
    let latest = graph.latest_time().expect("non-empty graph");
    let earliest = graph.earliest_time().expect("non-empty graph");
    let start = latest - (latest - earliest) / 10;
    graph_window_group_10.sample_size(10);
    let make_graph = || graph.window(start, latest + 1);
    run_analysis_benchmarks(&mut graph_window_group_10, make_graph, None);
    graph_window_group_10.finish();
    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);

    // subgraph
    let mut rng = rand::rngs::StdRng::seed_from_u64(73);
    let nodes = graph
        .nodes()
        .into_iter()
        .choose_multiple(&mut rng, graph.count_nodes() / 10)
        .into_iter()
        .map(|n| n.id())
        .collect::<Vec<_>>();
    let subgraph = graph.subgraph(nodes);
    let group_name = "analysis_subgraph_10pc";
    let mut subgraph_10 = c.benchmark_group(group_name);
    subgraph_10.sample_size(10);

    let make_graph = || subgraph.clone();
    run_analysis_benchmarks(&mut subgraph_10, make_graph, None);
    subgraph_10.finish();
    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);

    // subgraph windowed
    let group_name = "analysis_subgraph_10pc_windowed";
    let mut subgraph_10_windowed = c.benchmark_group(group_name);
    subgraph_10_windowed.sample_size(10);

    let make_graph = || subgraph.window(start, latest + 1);
    run_analysis_benchmarks(&mut subgraph_10_windowed, make_graph, None);
    subgraph_10_windowed.finish();
    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);

    // layered graph windowed
    let graph = layered_sx_super_user_graph(Some(10)).unwrap();
    let group_name = "analysis_graph_window_50_layered";
    let mut graph_window_layered_group_50 = c.benchmark_group(group_name);
    let latest = graph.latest_time().expect("non-empty graph");
    let earliest = graph.earliest_time().expect("non-empty graph");
    let start = latest - (latest - earliest) / 2;
    graph_window_layered_group_50.sample_size(10);
    let make_graph = || {
        graph
            .window(start, latest + 1)
            .layers(["0", "1", "2", "3", "4"])
            .unwrap()
    };
    run_analysis_benchmarks(&mut graph_window_layered_group_50, make_graph, None);
    graph_window_layered_group_50.finish();
    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);

    let graph = graph.persistent_graph();

    let group_name = "persistent_analysis_graph_window_50_layered";
    let mut graph_window_layered_group_50 = c.benchmark_group(group_name);
    let latest = graph.latest_time().expect("non-empty graph");
    let earliest = graph.earliest_time().expect("non-empty graph");
    let start = latest - (latest - earliest) / 2;
    graph_window_layered_group_50.sample_size(10);
    let make_graph = || {
        graph
            .window(start, latest + 1)
            .layers(["0", "1", "2", "3", "4"])
            .unwrap()
    };
    run_analysis_benchmarks(&mut graph_window_layered_group_50, make_graph, None);
    graph_window_layered_group_50.finish();
    bench_materialise(&format!("{group_name}_materialise"), c, make_graph);
}

fn bench_materialise<F, G>(name: &str, c: &mut Criterion, make_graph: F)
where
    F: Fn() -> G,
    G: StaticGraphViewOps,
{
    let mut mat_graph_group = c.benchmark_group(name);
    mat_graph_group.sample_size(10);
    run_materialize(&mut mat_graph_group, make_graph, None);
    mat_graph_group.finish();
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
