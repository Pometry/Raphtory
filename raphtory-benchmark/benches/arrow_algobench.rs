use criterion::{criterion_group, criterion_main};

#[cfg(feature = "storage")]
pub mod arrow_bench {
    use criterion::{black_box, BenchmarkId, Criterion, SamplingMode};
    use raphtory::{
        algorithms::{
            centrality::pagerank::unweighted_page_rank,
            components::weakly_connected_components,
            metrics::clustering_coefficient::{
                global_clustering_coefficient::global_clustering_coefficient,
                local_clustering_coefficient::local_clustering_coefficient,
            },
            motifs::local_triangle_count::local_triangle_count,
        },
        graphgen::random_attachment::random_attachment,
        prelude::*,
    };
    use raphtory_benchmark::common::bench;
    use rayon::prelude::*;
    use tempfile::TempDir;

    pub fn local_triangle_count_analysis(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_triangle_count");
        group.sample_size(10);
        bench(&mut group, "local_triangle_count", None, |b| {
            let g = raphtory::graph_loader::lotr_graph::lotr_graph();
            let test_dir = TempDir::new().unwrap();
            let g = g.persist_as_disk_graph(test_dir.path()).unwrap();
            let windowed_graph = g.window(i64::MIN, i64::MAX);

            b.iter(|| {
                let node_ids = windowed_graph.nodes().collect();

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
            let g: Graph = Graph::new();

            let vs = vec![
                (1, 2, 1),
                (1, 3, 2),
                (1, 4, 3),
                (3, 1, 4),
                (3, 4, 5),
                (3, 5, 6),
                (4, 5, 7),
                (5, 6, 8),
                (5, 8, 9),
                (7, 5, 10),
                (8, 5, 11),
                (1, 9, 12),
                (9, 1, 13),
                (6, 3, 14),
                (4, 8, 15),
                (8, 3, 16),
                (5, 10, 17),
                (10, 5, 18),
                (10, 8, 19),
                (1, 11, 20),
                (11, 1, 21),
                (9, 11, 22),
                (11, 9, 23),
            ];

            for (src, dst, t) in &vs {
                g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
            }

            let test_dir = TempDir::new().unwrap();
            let g = g.persist_as_disk_graph(test_dir.path()).unwrap();

            let windowed_graph = g.window(0, 5);
            b.iter(|| local_clustering_coefficient(&windowed_graph, 1))
        });

        group.finish();
    }

    pub fn graphgen_large_clustering_coeff(c: &mut Criterion) {
        let mut group = c.benchmark_group("graphgen_large_clustering_coeff");
        // generate graph
        let graph = Graph::new();
        let seed: [u8; 32] = [1; 32];
        random_attachment(&graph, 500000, 4, Some(seed));

        let test_dir = TempDir::new().unwrap();
        let graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

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

        let test_dir = TempDir::new().unwrap();
        let graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();
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
        let test_dir = TempDir::new().unwrap();
        let graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

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
}

#[cfg(feature = "storage")]
pub use arrow_bench::*;

#[cfg(feature = "storage")]
criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis,
    graphgen_large_clustering_coeff,
    graphgen_large_pagerank,
    graphgen_large_concomp,
);

#[cfg(feature = "storage")]
criterion_main!(benches);
