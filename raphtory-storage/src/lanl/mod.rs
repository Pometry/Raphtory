use std::{fmt::Debug, time::Instant};

pub mod exfiltration;
pub mod query1;
pub mod query2;
pub mod query3;
pub mod query3b;
pub mod query3c;
pub mod query4;
mod tests;

const NUM_THREADS: usize = 16;

fn thread_pool(n_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap()
}

fn measure<T, F>(name: &str, f: F, print_result: bool) -> T
where
    F: FnOnce() -> T,
    T: Debug,
{
    let start_time = Instant::now();
    let result = f();
    let elapsed_time = start_time.elapsed();

    if print_result {
        let elapsed_ms = elapsed_time.as_millis();
        if elapsed_ms < 1000 {
            println!(
                "Running {}: time: {}ms, result: {:?}",
                name, elapsed_ms, result
            );
        } else {
            let elapsed_sec = elapsed_time.as_secs_f64();
            println!(
                "Running {}: time: {:.3}s, result: {:?}",
                name, elapsed_sec, result
            );
        }
    } else {
        let elapsed_ms = elapsed_time.as_millis();
        if elapsed_ms < 1000 {
            println!("Running {}: time: {}ms", name, elapsed_ms);
        } else {
            let elapsed_sec = elapsed_time.as_secs_f64();
            println!("Running {}: time: {:.3}s", name, elapsed_sec);
        }
    }

    result
}
