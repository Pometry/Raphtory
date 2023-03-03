use criterion::{criterion_group, criterion_main, Criterion};
use docbrown_db::algorithms::local_clustering_coefficient::local_clustering_coefficient;
use docbrown_db::algorithms::local_triangle_count::local_triangle_count;
use docbrown_db::graph::Graph;

pub fn local_triangle_count_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_triangle_count");

    group.bench_function("local_tri_count", |b| {
        let g: Graph = Graph::new(1);
        let windowed_graph = g.window(0, 5);

        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        b.iter(|| local_triangle_count(&windowed_graph, 1))
    });

    group.finish();
}

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_clustering_coefficient");

    group.bench_function("local_tri_count", |b| {
        let g: Graph = Graph::new(1);
        let windowed_graph = g.window(0, 5);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        b.iter(|| local_clustering_coefficient(&windowed_graph, 1))
    });

    group.finish();
}

criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis
);
criterion_main!(benches);
