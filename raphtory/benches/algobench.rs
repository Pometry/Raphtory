use crate::common::bench;
use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::algorithms::local_clustering_coefficient::local_clustering_coefficient;
use raphtory::algorithms::local_triangle_count::local_triangle_count;
use raphtory::db::graph::Graph;
use raphtory::db::view_api::*;
use rayon::prelude::*;
mod common;

//TODO swap to new trianglecount
// pub fn global_triangle_count_analysis(c: &mut Criterion) {
//     let mut group = c.benchmark_group("global_triangle_count");
//     group.sample_size(10);
//     bench(&mut group, "global_triangle_count", None, |b| {
//         let g = raphtory_db::graph_loader::lotr_graph::lotr_graph(1);
//         let windowed_graph = g.window(i64::MIN, i64::MAX);
//         b.iter(|| {
//             global_triangle_count(&windowed_graph).unwrap();
//         });
//     });
//
//     group.finish();
// }

pub fn local_triangle_count_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_triangle_count");
    group.sample_size(10);
    bench(&mut group, "local_triangle_count", None, |b| {
        let g = raphtory::graph_loader::lotr_graph::lotr_graph(1);
        let windowed_graph = g.window(i64::MIN, i64::MAX);

        b.iter(|| {
            let vertex_ids = windowed_graph.vertices().id().collect::<Vec<_>>();

            vertex_ids.into_par_iter().for_each(|v| {
                local_triangle_count(&windowed_graph, v).unwrap();
            });
        })
    });

    group.finish();
}

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_clustering_coefficient");

    bench(&mut group, "local_clustering_coefficient", None, |b| {
        let g: Graph = Graph::new(1);
        let windowed_graph = g.window(0, 5);

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
            g.add_edge(*t, *src, *dst, &vec![], None).unwrap();
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
