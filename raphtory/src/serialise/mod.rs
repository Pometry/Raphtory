mod graph_folder;
pub mod metadata;

pub(crate) mod parquet;

#[cfg(feature = "proto")]
pub mod proto;
mod serialise;

pub use graph_folder::*;
pub use serialise::{StableDecode, StableEncode};

#[cfg(feature = "proto")]
pub use proto::proto_generated::Graph as ProtoGraph;
