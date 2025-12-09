use crate::{
    db::api::{mutation::AdditionOps, view::StaticGraphViewOps},
    errors::GraphError,
    serialise::{
        parquet::{ParquetDecoder, ParquetEncoder},
        GraphFolder,
    },
};
use std::{fs, fs::File, path::Path};
use tempfile;

#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::serialise::GraphPaths;

pub trait StableEncode: StaticGraphViewOps + AdditionOps {
    /// Encode the graph into bytes.
    fn encode_to_bytes(&self) -> Vec<u8>;

    /// Encode the graph into the given path.
    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;
}

impl<T: ParquetEncoder + StaticGraphViewOps + AdditionOps> StableEncode for T {
    fn encode_to_bytes(&self) -> Vec<u8> {
        // Encode to a temp zip file and return the bytes
        let tempdir = tempfile::tempdir().unwrap();
        let zip_path = tempdir.path().join("graph.zip");
        let folder = GraphFolder::new_as_zip(&zip_path);

        self.encode(&folder).unwrap();
        fs::read(&zip_path).unwrap()
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();

        if folder.write_as_zip_format {
            let file = File::create_new(&folder.root())?;
            self.encode_parquet_to_zip(file)?;
            #[cfg(feature = "search")]
            self.persist_index_to_disk_zip(&folder)?;
            folder.write_metadata(self)?;
        } else {
            let write_folder = folder.init_write()?;
            self.encode_parquet(write_folder.graph_path()?)?;
            #[cfg(feature = "search")]
            self.persist_index_to_disk(&write_folder)?;
            write_folder.data_path()?.write_metadata(self)?;
            write_folder.finish()?;
        }
        Ok(())
    }
}

pub trait StableDecode: StaticGraphViewOps + AdditionOps {
    // Decode the graph from the given bytes array.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode_from_bytes(
        bytes: &[u8],
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;

    // Decode the graph from the given path.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode(
        path: impl Into<GraphFolder>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;

    /// Returns true if the graph can be decoded from the given path.
    fn is_decodable(path: impl AsRef<Path>) -> bool;
}

impl<T: ParquetDecoder + StaticGraphViewOps + AdditionOps> StableDecode for T {
    fn decode_from_bytes(
        bytes: &[u8],
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        // Write bytes to a temp zip file and decode
        let tempdir = tempfile::tempdir()?;
        let zip_path = tempdir.path().join("graph.zip");
        let folder = GraphFolder::new_as_zip(&zip_path);
        std::fs::write(&zip_path, bytes)?;

        let graph = Self::decode(&folder, path_for_decoded_graph)?;

        Ok(graph)
    }

    fn decode(
        path: impl Into<GraphFolder>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let graph;
        let folder: GraphFolder = path.into();

        if folder.is_zip() {
            let reader = std::fs::File::open(&folder.root())?;
            graph = Self::decode_parquet_from_zip(reader, path_for_decoded_graph)?;
        } else {
            graph = Self::decode_parquet(&folder.graph_path()?, path_for_decoded_graph)?;
        }

        #[cfg(feature = "search")]
        graph.load_index(&folder)?;

        Ok(graph)
    }

    fn is_decodable(path: impl AsRef<Path>) -> bool {
        Self::is_parquet_decodable(path)
    }
}
