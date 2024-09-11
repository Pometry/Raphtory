use criterion::Criterion;
use raphtory::{graph_loader::sx_superuser_graph::sx_superuser_graph, prelude::Graph};
use raphtory_benchmark::common::bench_materialise;

pub fn bench() {
    let graph = sx_superuser_graph().unwrap();
}

pub fn run_materialise_bench(c: &mut Criterion, graph_name: &str, graph: Graph) {
    let mut graph_group = c.benchmark_group(graph_name);
    let make_graph = || graph.clone();
    graph_group.finish();

    bench_materialise(&format!("{graph_name}_materialise"), c, make_graph);
}
