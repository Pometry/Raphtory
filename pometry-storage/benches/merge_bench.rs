use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use itertools::Itertools;
use pometry_storage_private::merge::merge_chunks::IndexedView;
use rand::Rng;

fn merge_chunks_single(left: &[usize], right: &[usize], chunk_size: usize) {
    left.iter()
        .merge(right.iter())
        .chunks(chunk_size)
        .into_iter()
        .map(|c| c.copied().collect::<Vec<_>>())
        .for_each(|v| {
            black_box(v);
        })
}

fn merge_chunks_multi_nocp<'a>(left: &'a [usize], right: &'a [usize], chunk_size: usize) {
    left.merge_chunks_by::<Vec<_>>(right, chunk_size, |l, r| l.cmp(r), |v| *v)
        .for_each(|v| {
            black_box(v);
        })
}

fn bench_merge_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Merge");
    let mut rng = rand::thread_rng();

    let mut left = vec![0usize; 10_000_000];
    let mut right = vec![0usize; 10_000_000];
    let chunksizes = [1000, 10_000, 100_000, 1_000_000];
    rng.fill(&mut left[..]);
    rng.fill(&mut right[..]);
    left.sort();
    right.sort();
    for chunk_size in chunksizes {
        let input = (&left, &right, chunk_size);
        group.bench_with_input(
            BenchmarkId::new("Single-threaded", chunk_size),
            &input,
            |b, (left, right, chunk_size)| b.iter(|| merge_chunks_single(left, right, *chunk_size)),
        );
        group.bench_with_input(
            BenchmarkId::new("Multi-threaded no copy", chunk_size),
            &input,
            |b, (left, right, chunk_size)| {
                b.iter(|| merge_chunks_multi_nocp(left, right, *chunk_size))
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_merge_chunks);
criterion_main!(benches);
