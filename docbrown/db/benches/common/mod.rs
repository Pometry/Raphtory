use criterion::{measurement::WallTime, BatchSize, Bencher, BenchmarkGroup, BenchmarkId};
use docbrown_db::graph::Graph;
use rand::{distributions::Uniform, Rng};

fn make_index_gen() -> Box<dyn Iterator<Item = u64>> {
    let rng = rand::thread_rng();
    let range = Uniform::new(u64::MIN, u64::MAX);
    Box::new(rng.sample_iter(range))
}

fn make_time_gen() -> Box<dyn Iterator<Item = i64>> {
    let rng = rand::thread_rng();
    let range = Uniform::new(i64::MIN, i64::MAX);
    Box::new(rng.sample_iter(range))
}

pub fn bootstrap_graph(num_shards: usize, num_vertices: usize) -> Graph {
    let graph = Graph::new(4);
    let mut indexes = make_index_gen();
    let mut times = make_time_gen();
    let num_edges = num_vertices / 2;
    for _ in 0..num_edges {
        let source = indexes.next().unwrap();
        let target = indexes.next().unwrap();
        let time = times.next().unwrap();
        graph.add_edge(time, source, target, &vec![]);
    }
    graph
}

fn bench<F>(group: &mut BenchmarkGroup<WallTime>, name: &str, parameter: Option<usize>, mut task: F)
where
    F: FnMut(&mut Bencher<'_, WallTime>),
{
    match parameter {
        Some(parameter) => group.bench_with_input(
            BenchmarkId::new(name, parameter),
            &parameter,
            |b: &mut Bencher, _| task(b),
        ),
        None => group.bench_function(name, task),
    };
}

pub fn run_ingestion_benchmarks<F>(
    group: &mut BenchmarkGroup<WallTime>,
    mut make_graph: F,
    parameter: Option<usize>,
) where
    F: FnMut() -> Graph,
{
    let mut indexes = make_index_gen();
    let mut times = make_time_gen();
    let mut index_sample = || indexes.next().unwrap();
    let mut time_sample = || times.next().unwrap();

    bench(
        group,
        "existing vertex varying time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), time_sample()),
                |(g, t): &mut (Graph, i64)| g.add_vertex(*t, 0, &vec![]),
                BatchSize::SmallInput,
            )
        },
    );
    bench(
        group,
        "new vertex constant time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), index_sample()),
                |(g, v): &mut (Graph, u64)| g.add_vertex(0, *v, &vec![]),
                BatchSize::SmallInput,
            )
        },
    );
    bench(
        group,
        "existing edge varying time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), time_sample()),
                |(g, t)| g.add_edge(*t, 0, 0, &vec![]),
                BatchSize::SmallInput,
            )
        },
    );
    bench(
        group,
        "new edge constant time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), index_sample(), index_sample()),
                |(g, s, d)| g.add_edge(0, *s, *d, &vec![]),
                BatchSize::SmallInput,
            )
        },
    );
}

fn times(n: usize) -> impl Iterator {
    std::iter::repeat(()).take(n)
}

pub fn run_large_ingestion_benchmarks<F>(
    group: &mut BenchmarkGroup<WallTime>,
    mut make_graph: F,
    parameter: Option<usize>,
) where
    F: FnMut() -> Graph,
{
    let mut times_gen = make_time_gen();
    let mut time_sample = || times_gen.next().unwrap();

    let updates = 1000;

    bench (
        group,
        "1k fixed edge updates with varying time",
        Option::parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), time_sample()),
                |(g, t)|
                    for _ in times(updates) {
                        g.add_edge(*t, 0, 0, &vec![])
                    },
                BatchSize::SmallInput,
            )
        },
    );

    bench (
        group,
        "1k random edge additions to a graph with 10k nodes",
        Option::parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), make_index_gen(), make_index_gen(), time_sample()),
                |(g, src_gen, dst_gen, t)|
                    for _ in times(updates) {
                        g.add_edge(src_gen.next().unwrap() as i64, dst_gen.next().unwrap(), *t as u64, &vec![])
                    },
                BatchSize::SmallInput,
            )
        },
    );
}

pub fn run_analysis_benchmarks<F>(
    group: &mut BenchmarkGroup<WallTime>,
    mut make_graph: F,
    parameter: Option<usize>,
) where
    F: FnMut() -> Graph,
{
    let mut graph = make_graph();
    bench(group, "edges_len", parameter, |b: &mut Bencher| {
        b.iter(|| graph.edges_len())
    });
    bench(group, "len", parameter, |b: &mut Bencher| {
        b.iter(|| graph.len())
    });
}
