use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use raphtory::{algorithms::centrality::hits::{old_hits, hits, new_hits}, graphgen::random_attachment::random_attachment, prelude::Graph};
use std::hint::black_box;

pub fn hits_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("hits_comparison");

    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 5_000_000, 4, Some(seed));

    let iterations = 20usize;

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(60));
    group.sample_size(10);

    group.bench_function("old_hits", |b| {
        b.iter(|| {
            let result = old_hits(&graph, black_box(iterations), None);
            black_box(result);
        })
    });

    group.bench_function("hits", |b| {
        b.iter(|| {
            let result = hits(&graph, black_box(iterations), None);
            black_box(result);
        })
    });

    group.bench_function("new_hits", |b| {
        b.iter(|| {
            let result = new_hits(&graph, black_box(iterations));
            black_box(result);
        })
    });

    group.finish();
}

criterion_group!(benches, hits_comparison);
criterion_main!(benches);