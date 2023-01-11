use std::collections::BTreeSet;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use docbrown::lsm::LSMSet;
use docbrown::lsm::SortedVec;
use rand::{distributions::Uniform, Rng};

fn btree_set_u64(c: &mut Criterion) {

    let mut group = c.benchmark_group("btree_set_u64_range_insert");
    for size in [10, 100, 300, 500, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let mut rng = rand::thread_rng();
        let range = Uniform::new(u64::MIN, u64::MAX);

        let init_vals: Vec<u64> = (&mut rng).sample_iter(&range).take(*size).collect();

        let range2 = Uniform::new(usize::MIN, usize::MAX);
        let ids: Vec<usize> = (&mut rng).sample_iter(&range2).take(*size).collect();

        group.bench_with_input(
            BenchmarkId::new("BTreeSet with u64", size),
            &init_vals,
            |b, vals| {
                b.iter(|| {
                    let mut bs = BTreeSet::default();
                    for v in vals.iter() {
                        bs.range(*v..).next();
                        bs.insert(v);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("LSMTree with u64", size),
            &init_vals,
            |b, vals| {
                b.iter(|| {
                    let mut bs = LSMSet::default();
                    for v in vals.iter() {
                        bs.find(*v);
                        bs.insert(*v);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("SortedVec with u64", size),
            &init_vals,
            |b, vals| {
                b.iter(|| {
                    let mut bs = SortedVec::new();
                    for v in vals.iter() {
                        bs.find(*v);
                        bs.insert(*v);
                    }
                });
            },
        );


    }
    group.finish();
}

criterion_group!(benches, btree_set_u64);
criterion_main!(benches);
