use std::sync::LazyLock;

use rayon::{ThreadPool, ThreadPoolBuilder};

pub mod csv_loader;
pub mod json_loader;
pub mod neo4j_loader;

pub mod parquet_loaders;

pub(crate) static ENCODE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .thread_name(|idx| format!("PS Encode Thread-{idx}"))
        .build()
        .unwrap()
});
