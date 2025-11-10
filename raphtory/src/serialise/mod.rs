mod graph_folder;
pub mod metadata;
#[cfg(feature = "arrow")]
pub(crate) mod parquet;

#[cfg(feature = "proto")]
pub mod proto;
mod serialise;

pub use graph_folder::{GraphFolder, GRAPH_PATH, INDEX_PATH, META_PATH, VECTORS_PATH};
pub use serialise::{StableDecode, StableEncode};

#[cfg(feature = "proto")]
pub use proto::proto_generated::Graph as ProtoGraph;
