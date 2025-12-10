#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::api::{mutation::AdditionOps, view::StaticGraphViewOps},
    errors::GraphError,
    serialise::{
        get_zip_graph_path,
        metadata::GraphMetadata,
        parquet::{ParquetDecoder, ParquetEncoder},
        GraphFolder, GraphPaths, Metadata, RelativePath, DATA_PATH, DEFAULT_DATA_PATH,
        DEFAULT_GRAPH_PATH, GRAPH_PATH, META_PATH,
    },
};
use std::{
    fs,
    fs::File,
    io::{Cursor, Read, Seek, Write},
    path::Path,
};
use tempfile;
use zip::{write::SimpleFileOptions, ZipArchive, ZipWriter};

pub trait StableEncode: StaticGraphViewOps + AdditionOps {
    fn encode_to_zip<W: Write + Seek>(&self, writer: ZipWriter<W>) -> Result<(), GraphError>;
    /// Encode the graph into bytes.
    fn encode_to_bytes(&self) -> Result<Vec<u8>, GraphError>;

    /// Encode the graph into the given path.
    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;
}

impl<T: ParquetEncoder + StaticGraphViewOps + AdditionOps> StableEncode for T {
    fn encode_to_zip<W: Write + Seek>(&self, mut writer: ZipWriter<W>) -> Result<(), GraphError> {
        let graph_meta = GraphMetadata::from_graph(self);
        writer.start_file(META_PATH, SimpleFileOptions::default())?;
        writer.write(&serde_json::to_vec(&RelativePath {
            path: DEFAULT_DATA_PATH.to_string(),
        })?)?;
        writer.start_file(
            [DEFAULT_DATA_PATH, META_PATH].join("/"),
            SimpleFileOptions::default(),
        )?;
        writer.write(&serde_json::to_vec(&Metadata {
            path: DEFAULT_GRAPH_PATH.to_string(),
            meta: graph_meta,
        })?)?;
        let graph_prefix = [DEFAULT_DATA_PATH, DEFAULT_GRAPH_PATH].join("/");
        self.encode_parquet_to_zip(&mut writer, graph_prefix)?;
        // TODO: Encode Index to zip
        writer.finish()?;
        Ok(())
    }

    fn encode_to_bytes(&self) -> Result<Vec<u8>, GraphError> {
        let mut bytes = Vec::new();
        let writer = ZipWriter::new(Cursor::new(&mut bytes));
        self.encode_to_zip(writer)?;
        Ok(bytes)
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();

        if folder.write_as_zip_format {
            let file = File::create_new(&folder.root())?;
            self.encode_to_zip(ZipWriter::new(file))?;
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

    fn decode_from_zip<R: Read + Seek>(
        reader: ZipArchive<R>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;

    // Decode the graph from the given path.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode(
        path: impl Into<GraphFolder>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;
}

impl<T: ParquetDecoder + StaticGraphViewOps + AdditionOps> StableDecode for T {
    fn decode_from_bytes(
        bytes: &[u8],
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let cursor = Cursor::new(bytes);
        Self::decode_from_zip(ZipArchive::new(cursor)?, path_for_decoded_graph)
    }

    fn decode_from_zip<R: Read + Seek>(
        mut reader: ZipArchive<R>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let graph_prefix = get_zip_graph_path(&mut reader)?;
        let graph =
            Self::decode_parquet_from_zip(&mut reader, path_for_decoded_graph, graph_prefix)?;

        //TODO: graph.load_index_from_zip(&mut reader, prefix)

        Ok(graph)
    }

    fn decode(
        path: impl Into<GraphFolder>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let graph;
        let folder: GraphFolder = path.into();

        if folder.is_zip() {
            let reader = ZipArchive::new(File::open(&folder.root())?)?;
            graph = Self::decode_from_zip(reader, path_for_decoded_graph)?;
        } else {
            graph = Self::decode_parquet(&folder.graph_path()?, path_for_decoded_graph)?;
            #[cfg(feature = "search")]
            graph.load_index(&folder)?;
        }
        Ok(graph)
    }
}
