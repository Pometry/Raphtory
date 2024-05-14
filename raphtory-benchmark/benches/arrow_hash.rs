use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use polars_arrow::array::PrimitiveArray;
use rand::{distributions::Uniform, seq::IteratorRandom, Rng};

use raphtory::arrow::{
    arrow_hmap::ArrowHashMap,
    global_order::{GlobalMap, GlobalOrder, SortedGIDs},
    GID,
};

fn arrow_hash_b(c: &mut Criterion) {
    let mut group = c.benchmark_group("Global Map Benchmark");
    for size in [10, 100, 1_000, 10_000, 100_000].iter() {
        println!("Running for size: {}", size);
        group.throughput(Throughput::Elements(*size as u64));

        let mut rng = rand::thread_rng();
        let range = Uniform::new(i64::MIN, i64::MAX);
        let mut init_vals: Vec<i64> = (&mut rng).sample_iter(&range).take(*size).collect();

        init_vals.sort_unstable();
        init_vals.dedup();

        {
            let map = ArrowHashMap::from_sorted_dedup(
                PrimitiveArray::from_vec(init_vals.clone()).boxed(),
            )
            .expect("Failed to create ArrowHashMap");

            bench_map("ArrowHashMap", &mut group, size, &map, &init_vals);
        }

        {
            let map: SortedGIDs =
                SortedGIDs::try_from((None, PrimitiveArray::from_vec(init_vals.clone()).boxed()))
                    .expect("Failed to create SortedGIDs");

            bench_map("SortedGIDs", &mut group, size, &map, &init_vals);
        }

        {
            let map: GlobalMap = init_vals.iter().copied().map(GID::I64).collect();
            bench_map("GlobalMap", &mut group, size, &map, &init_vals);
        }
    }
    group.finish()
}

fn bench_map(
    name: &str,
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    size: &usize,
    map: &impl GlobalOrder,
    init_vals: &Vec<i64>,
) {
    group.bench_with_input(
        BenchmarkId::new(name, size),
        &(&map, init_vals),
        |b, &(map, keys)| {
            let mut rng = rand::thread_rng();
            let sample = keys
                .iter()
                .copied()
                .choose_multiple(&mut rng, keys.len() / 10);

            b.iter(|| {
                for key in sample.iter() {
                    let _ = map.find(&GID::I64(*key));
                }
            });
        },
    );
}

criterion_group!(benches, arrow_hash_b);
criterion_main!(benches);
