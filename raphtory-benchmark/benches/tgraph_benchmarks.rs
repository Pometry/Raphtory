use std::collections::BTreeSet;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{distributions::Uniform, Rng};
use sorted_vector_map::SortedVectorSet;
use raphtory::core::tgraph::vertices::structure::adjset::AdjSet;

fn btree_set_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_set_u64_range_insert");
    for size in [10, 100, 300, 500, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let mut rng = rand::thread_rng();
        let range = Uniform::new(u64::MIN, u64::MAX);
        let init_vals: Vec<u64> = (&mut rng).sample_iter(&range).take(*size).collect();

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
            BenchmarkId::new("SortedVec with u64", size),
            &init_vals,
            |b, vals| {
                b.iter(|| {
                    let mut bs = SortedVectorSet::new();
                    for v in vals.iter() {
                        bs.get(v);
                        bs.insert(*v);
                    }
                });
            },
        );
    }
    group.finish();
}

fn bm_tadjset(c: &mut Criterion) {
    let mut group = c.benchmark_group("tadjset");
    for size in [10, 100, 1000, 10_000, 100_000, 1_000_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let mut rng = rand::thread_rng();
        let range = Uniform::new(0, size * 10);
        let init_srcs: Vec<usize> = (&mut rng)
            .sample_iter(&range)
            .take(*size as usize)
            .collect();
        let init_dsts: Vec<usize> = (&mut rng)
            .sample_iter(&range)
            .take(*size as usize)
            .collect();
        let t_range = Uniform::new(1646838523i64, 1678374523);
        let init_time: Vec<i64> = (&mut rng)
            .sample_iter(&t_range)
            .take(*size as usize)
            .collect();

        let mut tadjset = AdjSet::default();

        group.bench_with_input(
            BenchmarkId::new("TAdjSet insert", size),
            &(init_time.clone(), init_srcs, init_dsts),
            |b, (time, srcs, dsts)| {
                b.iter(|| {
                    for i in 0..time.len() {
                        tadjset.push(srcs[i], dsts[i]);
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, btree_set_u64, bm_tadjset);
criterion_main!(benches);
