pub(crate) mod incremental;
pub mod metadata;
pub(crate) mod parquet;
mod proto_ext;
mod serialise;
mod graph_folder;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

pub use proto::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode};
pub use graph_folder::{GraphFolder, GraphReader, GRAPH_FILE_NAME, META_FILE_NAME};

