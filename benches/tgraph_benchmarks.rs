use std::{collections::BTreeSet, iter};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use docbrown::lsm::LSMSet;
use docbrown::{edge::Edge, lsm::SortedVec};
use rand::{distributions::Uniform, Rng};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Edge2 {
    pub v: usize, // physical id of the other vertex
    pub e_meta: Option<usize>, // physical id of the edge metadata
                  // pub remote: bool
}

fn btree_set_u64(c: &mut Criterion) {
    static TEN: usize = 10;

    let mut group = c.benchmark_group("btree_set_u64_range_insert");
    for size in [10, 100, 300, 500, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let mut rng = rand::thread_rng();
        let range = Uniform::new(u64::MIN, u64::MAX);

        let init_vals: Vec<u64> = (&mut rng).sample_iter(&range).take(*size).collect();

        let range2 = Uniform::new(usize::MIN, usize::MAX);
        let ids: Vec<usize> = (&mut rng).sample_iter(&range2).take(*size).collect();

        let init_edges: Vec<Edge> = ids
            .iter()
            .zip((&mut rng).sample_iter(&range2))
            .take(*size)
            .map(|(a, b)| Edge::local(*a, b))
            .collect();

        let init_edges2: Vec<Edge2> = ids
            .iter()
            .zip((&mut rng).sample_iter(&range2))
            .take(*size)
            .map(|(a, b)| Edge2 {
                v: *a,
                e_meta: Some(b),
            })
            .collect();

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

        group.bench_with_input(
            BenchmarkId::new("BTreeSet with Edge", size),
            &init_edges,
            |b, vals| {
                b.iter(|| {
                    let mut bs = BTreeSet::default();
                    for v in vals.iter() {
                        let key = Edge::empty(v.v);
                        bs.range(key..).next();
                        bs.insert(v);
                    }
                })
            },
        );

            // group.bench_with_input(
            //     BenchmarkId::new("LSMTree with Edge", size),
            //     &init_edges,
            //     |b, vals| {
            //         b.iter(|| {
            //             let mut bs = LSMSet::new();
            //             for v in vals.iter() {
            //                 bs.find(*v);
            //                 bs.insert(*v);
            //             }
            //         })
            //     },
            // );

        group.bench_with_input(
            BenchmarkId::new("SortedVec with Edge", size),
            &init_edges,
            |b, vals| {
                b.iter(|| {
                    let mut bs = SortedVec::new();
                    for v in vals.iter() {
                        bs.find(*v);
                        bs.insert(*v);
                    }
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, btree_set_u64);
criterion_main!(benches);
