use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;

pub mod dataframe;
pub mod df_loaders;
mod layer_col;
pub mod node_col;
pub mod prop_handler;

/// `RAPHTORY_LOAD_THREADS` overrides its size (default: one per core).
pub(crate) static LOAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let num_threads = std::env::var("RAPHTORY_LOAD_THREADS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|idx| format!("PS Bulk Load Thread-{idx}"))
        .build()
        .unwrap()
});
