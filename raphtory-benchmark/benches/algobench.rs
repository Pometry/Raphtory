use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use raphtory::{
    algorithms::{
        centrality::{hits::hits, pagerank::unweighted_page_rank},
        components::weakly_connected_components,
        metrics::clustering_coefficient::{
            global_clustering_coefficient::global_clustering_coefficient,
            local_clustering_coefficient::local_clustering_coefficient,
        },
        motifs::{
            global_temporal_three_node_motifs::global_temporal_three_node_motif,
            local_triangle_count::local_triangle_count,
        },
    },
    graphgen::random_attachment::random_attachment,
    prelude::*,
};
use raphtory_benchmark::common::bench;
use rayon::prelude::*;
use core::num;
use std::hint::black_box;

fn build_sparse_graph_with_hub(num_nodes: usize, add_hub: bool) -> Graph {
    let graph = Graph::new();

    // Ensure all 100k nodes are present, including isolated vertices.
    for node_id in 0..num_nodes {
        graph
            .add_node(node_id as i64, node_id as u64, NO_PROPS, None, None)
            .unwrap();
    }

    // Baseline sparse graph: each node has 2 outgoing edges (< 5).
    for node_id in 0..num_nodes {
        let src = node_id as u64;
        let dst1 = ((node_id + 1) % num_nodes) as u64;
        let dst2 = ((node_id + 2) % num_nodes) as u64;

        graph
            .add_edge(node_id as i64, src, dst1, NO_PROPS, None)
            .unwrap();
        graph
            .add_edge((num_nodes + node_id) as i64, src, dst2, NO_PROPS, None)
            .unwrap();
    }

    // Hub-skewed variant: one node has 5,000 outgoing edges.
    if add_hub {
        for dst in 1..=(num_nodes - 1) {
            graph
                .add_edge((2 * num_nodes + dst) as i64, 0u64, dst as u64, NO_PROPS, None)
                .unwrap();
        }
    }

    graph
}


pub fn local_triangle_count_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_triangle_count");
    group.sample_size(10);
    bench(&mut group, "local_triangle_count", None, |b| {
        let g = raphtory::graph_loader::lotr_graph::lotr_graph();
        let windowed_graph = g.window(i64::MIN, i64::MAX);

        b.iter(|| {
            let node_ids = windowed_graph.nodes().id().collect::<Vec<_>>();

            node_ids.into_par_iter().for_each(|v| {
                local_triangle_count(&windowed_graph, v).unwrap();
            });
        })
    });

    group.finish();
}

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_clustering_coefficient");

    bench(&mut group, "local_clustering_coefficient", None, |b| {
        let g: Graph = raphtory::graph_loader::lotr_graph::lotr_graph();

        b.iter(|| local_clustering_coefficient(&g, "Gandalf"))
    });

    group.finish();
}

pub fn graphgen_large_clustering_coeff(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_clustering_coeff");
    // generate graph
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 500000, 4, Some(seed));

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(60));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_clustering_coeff", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = global_clustering_coefficient(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_pagerank(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_pagerank");
    // generate graph
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 500000, 4, Some(seed));

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_pagerank", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = unweighted_page_rank(graph, Some(100), None, None, true, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_concomp(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_concomp");
    // generate graph
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 500000, 4, Some(seed));

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(60));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_concomp", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = weakly_connected_components(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn temporal_motifs(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_motifs");

    bench(&mut group, "temporal_motifs", None, |b| {
        let g: Graph = raphtory::graph_loader::lotr_graph::lotr_graph();

        b.iter(|| global_temporal_three_node_motif(&g, 100, None))
    });

    group.finish();
}

pub fn multithreaded_hits_sparse_vs_hub(c: &mut Criterion) {
    let mut group = c.benchmark_group("multithreaded_hits_sparse_vs_hub");

    let num_nodes = 200_000usize;
    let threads = Some(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
    );

    let sparse_graph = build_sparse_graph_with_hub(num_nodes, false);
    let hub_graph = build_sparse_graph_with_hub(num_nodes, true);

    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));

    group.bench_with_input(
        BenchmarkId::new("hits_sparse_under_5_edges", num_nodes),
        &sparse_graph,
        |b, graph| {
            b.iter(|| {
                let result = hits(graph, 20, threads);
                black_box(result);
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("hits_sparse_with_5000_edge_hub", num_nodes),
        &hub_graph,
        |b, graph| {
            b.iter(|| {
                let result = hits(graph, 20, threads);
                black_box(result);
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis,
    graphgen_large_clustering_coeff,
    graphgen_large_pagerank,
    graphgen_large_concomp,
    temporal_motifs,
    multithreaded_hits_sparse_vs_hub,
);
criterion_main!(benches);
