use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use raphtory::{
    algorithms::{
        centrality::pagerank::unweighted_page_rank,
        components::weakly_connected_components,
        metrics::{
            clustering_coefficient::clustering_coefficient,
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
use rayon::prelude::*;
use raphtory_benchmark::common::bench;

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
                let result = clustering_coefficient(graph);
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
                let result = weakly_connected_components(graph, 20, None);
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

criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis,
    graphgen_large_clustering_coeff,
    graphgen_large_pagerank,
    graphgen_large_concomp,
    temporal_motifs,
);
criterion_main!(benches);
