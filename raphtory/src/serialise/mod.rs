mod graph_folder;
pub mod metadata;
pub(crate) mod parquet;
pub mod proto;
mod serialise;

pub use graph_folder::{GraphFolder, GRAPH_PATH, META_PATH};
pub use proto::proto_generated::Graph as ProtoGraph;
pub use serialise::{StableDecode, StableEncode};
