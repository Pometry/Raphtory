pub mod metadata;
pub(crate) mod parquet;
pub mod proto;
mod serialise;
mod graph_folder;

pub use proto::proto_generated::Graph as ProtoGraph;
pub use serialise::{StableDecode, StableEncode};
pub use graph_folder::{GraphFolder, GraphReader, GRAPH_PATH, META_PATH};
