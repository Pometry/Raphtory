use std::fmt::Debug;

pub(crate) mod query1;
pub(crate) mod query2;
pub(crate) mod query3;
pub(crate) mod query3b;
pub(crate) mod query3c;
pub(crate) mod query4;
mod loader;
mod tests;

const NUM_THREADS: usize = 16;

fn thread_pool(n_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap()
}

fn measure<B: Debug>(name: &str, f: impl Fn() -> B) -> B {
    let now = std::time::Instant::now();
    let result = f();
    let elapsed = now.elapsed();

    println!("Running query {}: time: {:?}, result: {:?}", name, elapsed, result);
    result
}
