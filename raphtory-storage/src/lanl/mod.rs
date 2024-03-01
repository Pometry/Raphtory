use std::fmt::Debug;

pub mod query1;
pub mod query2;
pub mod query3;
pub mod query3b;
pub mod query3c;
pub mod query4;
pub mod loader;
mod tests;

const NUM_THREADS: usize = 16;

fn thread_pool(n_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap()
}

pub fn measure<B: Debug>(name: &str, f: impl Fn() -> B) -> B {
    let now = std::time::Instant::now();
    let result = f();
    let elapsed = now.elapsed();

    println!("Running query {}: time: {:?}, result: {:?}", name, elapsed, result);
    result
}
