use criterion::{criterion_group, criterion_main, Criterion};
use docbrown_db::graph::Graph;
use algorithms::{local_triangle_count::local_triangle_count, local_clustering_coefficient::local_clustering_coefficient};

pub fn local_triangle_count_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_triangle_count");

    group.bench_function("local_tri_count", |b| {
        let g: Graph = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        b.iter(|| {
        local_triangle_count(&g, 1, 1, 5)
        })

    });

    group.finish();
}E

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_clustering_coefficient");

    group.bench_function("local_tri_count", |b| {
        let g: Graph = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        b.iter(|| {
        local_clustering_coefficient(&g, 1, 1, 5)
        })

    });

    group.finish();
}

criterion_group!(benches, local_triangle_count_analysis, local_clustering_coefficient_analysis);
criterion_main!(benches);