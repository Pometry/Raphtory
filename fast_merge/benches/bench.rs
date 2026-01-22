use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use fast_merge::FastMergeExt;
use itertools::Itertools;
use rand::{random, rng, rngs::SmallRng, Rng, SeedableRng};

fn bench(criterion: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(42);
    let data: Vec<Vec<u64>> = (0..10)
        .map(|_| {
            let size = rng.random_range(0..100);
            let mut inner: Vec<_> = (&mut rng).random_iter().take(size).collect();
            inner.sort();
            inner
        })
        .collect();
    let mut merge_and_first = criterion.benchmark_group("merge sorted vecs and get first element");
    for size in 0..=data.len() {
        merge_and_first.bench_with_input(
            BenchmarkId::new("kmerge", size),
            &size,
            |bencher, size| bencher.iter(|| data.iter().take(*size).kmerge().next()),
        );
        merge_and_first.bench_with_input(BenchmarkId::new("fast", size), &size, |bencher, size| {
            bencher.iter(|| data.iter().take(*size).fast_merge().next())
        });
    }
    merge_and_first.finish();

    let mut merge = criterion.benchmark_group("create merged iterator");
    for size in 0..=data.len() {
        merge.bench_with_input(BenchmarkId::new("kmerge", size), &size, |bencher, size| {
            bencher.iter(|| data.iter().take(*size).kmerge())
        });
        merge.bench_with_input(BenchmarkId::new("fast", size), &size, |bencher, size| {
            bencher.iter(|| data.iter().take(*size).fast_merge())
        });
    }
    merge.finish();

    let mut first_element = criterion.benchmark_group("get first element");
    for size in 0..=data.len() {
        first_element.bench_with_input(BenchmarkId::new("kmerge", size), &size, |bencher, size| {
            bencher.iter_batched_ref(
                || data.iter().take(*size).kmerge(),
                |iter| iter.next(),
                BatchSize::SmallInput,
            )
        });
        first_element.bench_with_input(BenchmarkId::new("fast", size), &size, |bencher, size| {
            bencher.iter_batched_ref(
                || data.iter().take(*size).fast_merge(),
                |iter| iter.next(),
                BatchSize::SmallInput,
            )
        });
    }
    first_element.finish();

    let mut iterate = criterion.benchmark_group("iterate over all elements");
    for size in 0..=data.len() {
        iterate.bench_with_input(BenchmarkId::new("kmerge", size), &size, |bencher, size| {
            bencher.iter_batched_ref(
                || data.iter().take(*size).kmerge(),
                |iter| {
                    for v in iter {
                        black_box(v);
                    }
                },
                BatchSize::SmallInput,
            )
        });
        iterate.bench_with_input(BenchmarkId::new("fast", size), &size, |bencher, size| {
            bencher.iter_batched_ref(
                || data.iter().take(*size).fast_merge(),
                |iter| {
                    for v in iter {
                        black_box(v);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    iterate.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
