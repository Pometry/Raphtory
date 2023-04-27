use std::sync::Arc;

use once_cell::sync::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub mod eval_vertex;
pub mod task;
pub mod task_runner;
pub(crate) mod context;


pub static POOL: Lazy<Arc<ThreadPool>> = Lazy::new(|| {
    let num_threads = std::env::var("DOCBROWN_MAX_THREADS")
        .map(|s| {
            s.parse::<usize>()
                .expect("DOCBROWN_MAX_THREADS must be a number")
        })
        .unwrap_or_else(|_| {
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                .get()
        });

    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    Arc::new(pool)
});

pub fn custom_pool(n_threads: usize) -> Arc<ThreadPool> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap();

    Arc::new(pool)
}