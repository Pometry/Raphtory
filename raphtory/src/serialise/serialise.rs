use super::graph_folder::GraphFolder;
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::api::view::StaticGraphViewOps, db::api::mutation::AdditionOps,
    errors::GraphError,
    serialise::parquet::{ParquetDecoder, ParquetEncoder},
};

pub trait StableEncode: StaticGraphViewOps + AdditionOps {
    // Encode the graph into bytes
    fn encode_to_bytes(&self) -> Vec<u8>;

    // Encode the graph to the given path
    fn encode_to_path(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;

    // Encode the graph along with any metadata/indexes to the given path
    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;
}

impl<T: ParquetEncoder + StaticGraphViewOps + AdditionOps> StableEncode for T {
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_parquet_to_bytes().unwrap()
    }

    fn encode_to_path(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();
        self.encode_parquet(&folder.root_folder)?;
        Ok(())
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();
        folder.write_graph(self)
    }
}

pub trait StableDecode: StaticGraphViewOps + AdditionOps {
    // Decode the graph from the given bytes array
    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError>;

    // Decode the graph from the given path
    fn decode_from_path(path: impl Into<GraphFolder>) -> Result<Self, GraphError>;

    // Decode the graph along with any metadata/indexes from the given path
    fn decode(path: impl Into<GraphFolder>) -> Result<Self, GraphError>;
}

impl<T: ParquetDecoder + StaticGraphViewOps + AdditionOps> StableDecode for T {
    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        let graph = Self::decode_parquet_from_bytes(bytes)?;
        Ok(graph)
    }

    fn decode_from_path(path: impl Into<GraphFolder>) -> Result<Self, GraphError> {
        let folder: GraphFolder = path.into();
        let graph = Self::decode_parquet(folder.root_folder)?;
        Ok(graph)
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

