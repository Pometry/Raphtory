use crate::{
    db::api::{
        mutation::AdditionOps, storage::storage::PersistenceStrategy, view::StaticGraphViewOps,
    },
    errors::GraphError,
    serialise::{
        metadata::build_graph_metadata,
        parquet::{ParquetDecoder, ParquetEncoder},
    },
};
use raphtory_api::core::storage::graph_folder::{
    get_zip_graph_path, GraphFolder, GraphPaths, Metadata, RelativePath, DEFAULT_DATA_PATH,
    DEFAULT_GRAPH_PATH, GRAPH_META_PATH, ROOT_META_PATH,
};
use std::{
    fs::File,
    io::{Cursor, Read, Seek, Write},
};
use storage::{Args, Extension};
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
        let graph_meta = build_graph_metadata(self);
        writer.start_file(ROOT_META_PATH, SimpleFileOptions::default())?;
        writer.write_all(&serde_json::to_vec(&RelativePath {
            path: DEFAULT_DATA_PATH.to_string(),
        })?)?;
        writer.start_file(
            [DEFAULT_DATA_PATH, GRAPH_META_PATH].join("/"),
            SimpleFileOptions::default(),
        )?;
        writer.write_all(&serde_json::to_vec(&Metadata {
            path: DEFAULT_GRAPH_PATH.to_string(),
            meta: graph_meta,
        })?)?;
        let graph_prefix = [DEFAULT_DATA_PATH, DEFAULT_GRAPH_PATH].join("/");
        self.encode_parquet_to_zip(&mut writer, graph_prefix)?;
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
            let file = File::create_new(folder.root())?;
            self.encode_to_zip(ZipWriter::new(file))?;
        } else {
            let write_folder = folder.init_write()?;
            self.encode_parquet(write_folder.graph_path()?)?;
            let data_folder = write_folder.data_path()?;
            let meta = Metadata {
                path: data_folder.relative_graph_path()?,
                meta: build_graph_metadata(self),
            };
            data_folder.write_metadata(meta)?;
            write_folder.finish()?;
        }
        Ok(())
    }
}

pub trait StableDecode: StaticGraphViewOps + AdditionOps {
    // Decode the graph from the given bytes array.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode_from_bytes_with_config(bytes: &[u8], args: Args) -> Result<Self, GraphError>;

    fn decode_from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        Self::decode_from_bytes_with_config(bytes, Args::default())
    }

    fn decode_from_bytes_at(
        bytes: &[u8],
        target: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError>;

    fn decode_from_zip_with_config<R: Read + Seek>(
        reader: ZipArchive<R>,
        args: Args,
    ) -> Result<Self, GraphError>;

    fn decode_from_zip<R: Read + Seek>(reader: ZipArchive<R>) -> Result<Self, GraphError> {
        Self::decode_from_zip_with_config(reader, Args::default())
    }

    fn decode_from_zip_at<R: Read + Seek>(
        reader: ZipArchive<R>,
        target: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError>;

    // Decode the graph from the given path.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        Self::decode_with_config(path, Args::default())
    }

    fn decode_with_config(
        path: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError>;

    fn decode_at(
        path: &(impl GraphPaths + ?Sized),
        target: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError>;
}

impl<T: ParquetDecoder + StaticGraphViewOps + AdditionOps> StableDecode for T {
    fn decode_from_bytes_with_config(bytes: &[u8], args: Args) -> Result<Self, GraphError> {
        let cursor = Cursor::new(bytes);
        Self::decode_from_zip_with_config(ZipArchive::new(cursor)?, args)
    }

    fn decode_from_bytes_at(
        bytes: &[u8],
        target: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        let cursor = Cursor::new(bytes);
        Self::decode_from_zip_at(ZipArchive::new(cursor)?, target, args)
    }

    fn decode_from_zip_with_config<R: Read + Seek>(
        mut reader: ZipArchive<R>,
        args: Args,
    ) -> Result<Self, GraphError> {
        let graph_prefix = get_zip_graph_path(&mut reader)?;
        let graph = Self::decode_parquet_from_zip(&mut reader, None, graph_prefix, args)?;

        Ok(graph)
    }

    fn decode_from_zip_at<R: Read + Seek>(
        mut reader: ZipArchive<R>,
        target: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        if !Extension::disk_storage_enabled() {
            return Err(GraphError::DiskGraphNotEnabled);
        }
        target.init()?;
        let graph_prefix = get_zip_graph_path(&mut reader)?;
        let graph = Self::decode_parquet_from_zip(
            &mut reader,
            Some(target.graph_path()?.as_path()),
            graph_prefix,
            args,
        )?;
        let meta = Metadata {
            path: target.relative_graph_path()?,
            meta: build_graph_metadata(&graph),
        };
        target.write_metadata(meta)?;
        Ok(graph)
    }

    fn decode_with_config(
        path: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        if path.is_zip() {
            let reader = path.read_zip()?;
            Self::decode_from_zip_with_config(reader, args)
        } else {
            Self::decode_parquet(&path.graph_path()?, None, args)
        }
    }

    fn decode_at(
        path: &(impl GraphPaths + ?Sized),
        target: &(impl GraphPaths + ?Sized),
        args: Args,
    ) -> Result<Self, GraphError> {
        target.init()?;
        let graph;
        if path.is_zip() {
            let reader = path.read_zip()?;
            graph = Self::decode_from_zip_at(reader, target, args)?;
        } else {
            graph = Self::decode_parquet(
                path.graph_path()?,
                Some(target.graph_path()?.as_path()),
                args,
            )?;
        }
        let meta = Metadata {
            path: target.relative_graph_path()?,
            meta: build_graph_metadata(&graph),
        };
        target.write_metadata(meta)?;
        Ok(graph)
    }
}
