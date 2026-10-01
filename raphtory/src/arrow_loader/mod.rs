use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;

pub mod dataframe;
pub mod df_loaders;
mod layer_col;
pub mod node_col;
pub mod prop_handler;


pub(crate) static LOAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .thread_name(|idx| format!("PS Bulk Load Thread-{idx}"))
        .build()
        .unwrap()
});
