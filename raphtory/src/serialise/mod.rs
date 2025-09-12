pub(crate) mod incremental;
pub mod metadata;
pub(crate) mod parquet;
pub mod proto;
mod serialise;
mod graph_folder;

pub use proto::proto_generated::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode};
pub use graph_folder::{GraphFolder, GraphReader, GRAPH_FILE_NAME, META_FILE_NAME};
