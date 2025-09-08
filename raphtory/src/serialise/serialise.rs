use super::graph_folder::GraphFolder;
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::api::view::StaticGraphViewOps, db::api::mutation::AdditionOps,
    errors::GraphError,
    serialise::proto::{proto_generated, ProtoDecoder, ProtoEncoder}
};
use prost::Message;

pub trait StableEncode: StaticGraphViewOps + AdditionOps {
    fn encode_to_bytes(&self) -> Vec<u8>;

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;
}

impl<T: ProtoEncoder + StaticGraphViewOps + AdditionOps> StableEncode for T {
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_proto().encode_to_vec()
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();
        folder.write_graph(self)
    }
}

pub trait StableDecode: StaticGraphViewOps + AdditionOps {
    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError>;

    fn decode(path: impl Into<GraphFolder>) -> Result<Self, GraphError>;
}

impl<T: ProtoDecoder + StaticGraphViewOps + AdditionOps> StableDecode for T {
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

