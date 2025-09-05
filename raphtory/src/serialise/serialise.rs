use super::graph_folder::GraphFolder;
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    errors::GraphError,
    serialise::{
        proto::{ProtoDecoder, ProtoEncoder},
        proto::proto_generated,
    },
};
use prost::Message;

pub trait StableEncode: ProtoEncoder {
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_proto().encode_to_vec()
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();
        folder.write_graph(self)
    }
}

impl<T: ProtoEncoder> StableEncode for T {}

pub trait StableDecode: ProtoDecoder {
    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        let graph = proto_generated::Graph::decode(bytes)?;
        Self::decode_from_proto(&graph)
    }

    fn decode(path: impl Into<GraphFolder>) -> Result<Self, GraphError> {
        let folder: GraphFolder = path.into();
        let bytes = folder.read_graph()?;
        let graph = Self::decode_from_bytes(bytes.as_ref())?;

        #[cfg(feature = "search")]
        graph.load_index(&folder)?;

        Ok(graph)
    }
}

impl<T: ProtoDecoder> StableDecode for T {}

pub trait CacheOps: Sized {
    /// Write graph to file and append future updates to the same file.
    ///
    /// If the file already exists, it's contents are overwritten
    fn cache(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;

    /// Persist the new updates by appending them to the cache file.
    fn write_updates(&self) -> Result<(), GraphError>;

    /// Load graph from file and append future updates to the same file
    fn load_cached(path: impl Into<GraphFolder>) -> Result<Self, GraphError>;
}

