use std::fmt::Debug;

mod load;
mod query1;
mod query2;
mod query3;
mod query4;
mod query_x;

const NUM_THREADS: usize = 16;

fn main() {
    let graph = load::load_graph_from_params(std::env::args());

    measure("query1", || query1::run(&graph));
    measure("query2", || query2::run(&graph));
    measure("query3", || query3::run(&graph));
    measure("query4", || query4::run2(&graph));
    measure("queryX", || query_x::run(&graph));
}

fn measure<B: Debug>(name: &str, f: impl Fn() -> B) -> B {
    let now = std::time::Instant::now();
    let result = f();
    let elapsed = now.elapsed();
    println!(
        "Running query {}: time: {:?}, result: {:?}",
        name, elapsed, result
    );

    result
}

pub(crate) fn thread_pool(n_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap()
}
