use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{distributions::Uniform, seq::SliceRandom, Rng};
use raphtory::core::{
    entities::vertices::structure::adjset::AdjSet,
    storage::timeindex::{AsTime, TimeIndex, TimeIndexEntry, TimeIndexOps},
};
use sorted_vector_map::SortedVectorSet;
use std::collections::BTreeSet;

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

fn time_index_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_index READ");

    for size in [10, 20, 30, 50, 80, 130, 210, 340, 550, 890, 1440].iter() {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(i64::MIN, i64::MAX);
        let init_vals: Vec<i64> = (&mut rng).sample_iter(&range).take(*size).collect();

        let t_entries = init_vals
            .iter()
            .map(|t| {
                let v: usize = rng.gen_range(0..10);
                TimeIndexEntry::new(*t, v)
            })
            .collect::<Vec<_>>();

        // read part
        group.bench_with_input(
            BenchmarkId::new("TimeIndex<i64> active", size),
            &init_vals,
            |b, vals| {
                let mut bs = TimeIndex::<i64>::default();
                for v in vals.iter() {
                    bs.insert(*v);
                }

                // for each index, pick a random window that contains it such that the start of the window is less than the element but at least 0
                // the end of the window is greater than the element but at most the length of the vector

                let mut sorted_vals = vals.clone();
                sorted_vals.sort();

                let windows = sorted_vals
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        let window_size = rng.gen_range(0..sorted_vals.len());
                        let half = window_size / 2;

                        let (left, over) = i.overflowing_sub(half);
                        let i_start = if over { 0 } else { 0.max(left) };

                        let (right, over) = i.overflowing_add(half);
                        let i_end = if over {
                            sorted_vals.len() - 1
                        } else {
                            (sorted_vals.len() - 1).min(right)
                        };

                        sorted_vals[i_start]..sorted_vals[i_end]
                    })
                    .collect::<Vec<_>>();

                b.iter(|| {
                    for window in windows.iter() {
                        bs.active(window.clone());
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("TimeIndex<TimeIndexEntry> active", size),
            &t_entries,
            |b, vals| {
                let mut bs = TimeIndex::<TimeIndexEntry>::default();
                for v in vals.iter() {
                    bs.insert(*v);
                }

                // for each index, pick a random window that contains it such that the start of the window is less than the element but at least 0
                // the end of the window is greater than the element but at most the length of the vector

                let mut sorted_vals = vals.clone();
                sorted_vals.sort();

                let windows = sorted_vals
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        let window_size = rng.gen_range(0..sorted_vals.len());
                        let half = window_size / 2;

                        let (left, over) = i.overflowing_sub(half);
                        let i_start = if over { 0 } else { 0.max(left) };

                        let (right, over) = i.overflowing_add(half);
                        let i_end = if over {
                            sorted_vals.len() - 1
                        } else {
                            (sorted_vals.len() - 1).min(right)
                        };

                        sorted_vals[i_start]..sorted_vals[i_end]
                    })
                    .map(|range| *range.start.t()..*range.end.t())
                    .collect::<Vec<_>>();

                b.iter(|| {
                    for window in windows.iter() {
                        bs.active(window.clone());
                    }
                });
            },
        );
    }
    group.finish();
}

fn time_index_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_index WRITE");

    for size in [10, 20, 30, 50, 80, 130, 210, 340, 550, 890, 1440].iter() {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(i64::MIN, i64::MAX);
        let init_vals: Vec<i64> = (&mut rng).sample_iter(&range).take(*size).collect();

        let t_entries = init_vals
            .iter()
            .map(|t| {
                let v: usize = rng.gen_range(0..10);
                TimeIndexEntry::new(*t, v)
            })
            .collect::<Vec<_>>();

        // write part
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("TimeIndex<i64> insert", size),
            &init_vals,
            |b, vals| {
                b.iter(|| {
                    let mut bs = TimeIndex::<i64>::default();
                    for v in vals.iter() {
                        bs.insert(*v);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("TimeIndex<TimeIndexEntry> insert", size),
            &t_entries,
            |b, vals| {
                b.iter(|| {
                    let mut bs = TimeIndex::<TimeIndexEntry>::default();
                    for v in vals.iter() {
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
    for size in [10, 100 /*, 1000, 10_000, 100_000, 1_000_000*/].iter() {
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

criterion_group!(
    benches,
    btree_set_u64,
    bm_tadjset,
    time_index_read,
    time_index_write
);
criterion_main!(benches);
