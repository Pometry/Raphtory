use std::sync::LazyLock;

use rayon::{ThreadPool, ThreadPoolBuilder};

pub mod arrow;
pub mod csv_loader;
pub mod json_loader;
pub mod neo4j_loader;

pub mod parquet_loaders;


static LOAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| ThreadPoolBuilder::new().thread_name(|idx| format!("PS Bulk Load Thread-{idx}")).build().unwrap());