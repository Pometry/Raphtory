use memmap2::Mmap;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

pub(crate) mod incremental;
pub mod metadata;
pub(crate) mod parquet;
mod proto_ext;
mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

#[cfg(feature = "search")]
use crate::prelude::SearchableGraphOps;
use crate::{
    core::utils::errors::GraphError,
    db::api::view::{internal::InternalStorageOps, MaterializedGraph},
    prelude::GraphViewOps,
    serialise::metadata::GraphMetadata,
};
pub use proto::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode, InternalStableDecode};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, ErrorKind, Read, Write},
    path::{Path, PathBuf},
};
use tracing::info;

const GRAPH_FILE_NAME: &str = "graph";
const META_FILE_NAME: &str = ".raph";

#[derive(Clone, Debug)]
pub struct GraphFolder {
    root_folder: PathBuf,
    prefer_zip_format: bool,
}

pub enum GraphReader {
    Zip(Vec<u8>),
    Folder(Mmap),
}

impl AsRef<[u8]> for GraphReader {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Zip(bytes) => bytes.as_ref(),
            Self::Folder(mmap) => mmap.as_ref(),
        }
    }
}

impl GraphFolder {
    pub fn new_as_zip(path: impl AsRef<Path>) -> Self {
        let folder: GraphFolder = path.into();
        Self {
            prefer_zip_format: true,
            ..folder
        }
    }

    // TODO: make it private again once we stop using it from the graphql crate
    pub fn get_graph_path(&self) -> PathBuf {
        self.root_folder.join(GRAPH_FILE_NAME)
    }

    pub fn get_meta_path(&self) -> PathBuf {
        self.root_folder.join(META_FILE_NAME)
    }

    // TODO: make private once possible
    pub fn get_vectors_path(&self) -> PathBuf {
        self.root_folder.join("vectors")
    }

    // TODO: make private once possible
    pub fn get_base_path(&self) -> &Path {
        &self.root_folder
    }

    pub fn read_graph(&self) -> Result<GraphReader, io::Error> {
        if self.root_folder.is_file() {
            let file = File::open(&self.root_folder)?;
            let mut archive = ZipArchive::new(file)?;
            let mut entry = archive.by_name(GRAPH_FILE_NAME)?;
            let mut buf = vec![];
            entry.read_to_end(&mut buf)?;
            Ok(GraphReader::Zip(buf))
        } else {
            let file = File::open(self.get_graph_path())?;
            let buf = unsafe { memmap2::MmapOptions::new().map(&file)? };
            Ok(GraphReader::Folder(buf))
        }
    }

    pub fn write_graph(&self, graph: &impl StableEncode) -> Result<(), GraphError> {
        self.write_graph_data(graph)?;
        self.write_metadata(graph)?;

        #[cfg(feature = "search")]
        self.write_index(graph)?;

        Ok(())
    }

    #[cfg(feature = "search")]
    fn write_index(&self, graph: &impl StableEncode) -> Result<(), GraphError> {
        if self.prefer_zip_format {
            graph.persist_index_to_disk_zip(&self.root_folder)
        } else {
            graph.persist_index_to_disk(&self.root_folder)
        }
    }

    fn write_graph_data(&self, graph: &impl StableEncode) -> Result<(), io::Error> {
        let bytes = graph.encode_to_vec();
        if self.prefer_zip_format {
            let file = File::create(&self.root_folder)?;
            let mut zip = ZipWriter::new(file);
            zip.start_file::<_, ()>(GRAPH_FILE_NAME, FileOptions::default())?;
            zip.write_all(&bytes)
        } else {
            self.ensure_clean_root_dir()?;
            let mut file = File::create(self.get_graph_path())?;
            file.write_all(&bytes)
        }
    }

    pub fn read_metadata(&self) -> Result<GraphMetadata, io::Error> {
        match self.try_read_metadata() {
            Ok(data) => Ok(data),
            Err(e) => {
                match e.kind() {
                    // In the case that the file is not found or invalid, try creating it then re-reading
                    ErrorKind::NotFound | ErrorKind::InvalidData | ErrorKind::UnexpectedEof => {
                        info!(
                            "Metadata file does not exist or is invalid. Attempting to recreate..."
                        );
                        let graph = MaterializedGraph::decode(self)?;
                        self.write_metadata(&graph)?;
                        self.try_read_metadata()
                    }
                    _ => Err(e),
                }
            }
        }
    }

    pub fn try_read_metadata(&self) -> Result<GraphMetadata, io::Error> {
        if self.root_folder.is_file() {
            let file = File::open(&self.root_folder)?;
            let mut archive = ZipArchive::new(file)?;
            let zip_file = archive.by_name(META_FILE_NAME)?;
            let reader = BufReader::new(zip_file);
            let metadata = serde_json::from_reader(reader)?;
            Ok(metadata)
        } else {
            let file = File::open(self.get_meta_path())?;
            let reader = BufReader::new(file);
            let metadata = serde_json::from_reader(reader)?;
            Ok(metadata)
        }
    }

    fn write_metadata<'graph>(&self, graph: &impl GraphViewOps<'graph>) -> Result<(), GraphError> {
        let node_count = graph.count_nodes();
        let edge_count = graph.count_edges();
        let properties = graph.properties();
        let metadata = GraphMetadata {
            node_count,
            edge_count,
            properties: properties.as_vec(),
        };
        if self.prefer_zip_format {
            let file = File::options()
                .read(true)
                .write(true)
                .open(&self.root_folder)?;
            let mut zip = ZipWriter::new_append(file)?;
            zip.start_file::<_, ()>(META_FILE_NAME, FileOptions::default())?;
            Ok(serde_json::to_writer(zip, &metadata)?)
        } else {
            let path = self.get_meta_path();
            let file = File::create(path.clone())?;
            Ok(serde_json::to_writer(file, &metadata)?)
        }
    }

    pub(crate) fn get_appendable_graph_file(&self) -> Result<File, GraphError> {
        let path = self.get_graph_path();
        Ok(OpenOptions::new().append(true).open(path)?)
    }

    fn ensure_clean_root_dir(&self) -> Result<(), GraphError> {
        if self.root_folder.exists() {
            let non_empty = self.root_folder.read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphError::NonEmptyGraphFolder(self.root_folder.clone()));
            }
        } else {
            fs::create_dir(&self.root_folder)?
        }
        File::create_new(self.root_folder.join(META_FILE_NAME))?;
        Ok(())
    }
}

impl<P: AsRef<Path>> From<P> for GraphFolder {
    fn from(value: P) -> Self {
        let path: &Path = value.as_ref();
        Self {
            root_folder: path.to_path_buf(),
            prefer_zip_format: false,
        }
    }
}

impl From<&GraphFolder> for GraphFolder {
    fn from(value: &GraphFolder) -> Self {
        value.clone()
    }
}

// this mod focuses on the zip format, as the folder format is
// the default and is largely exercised in other places
#[cfg(test)]
mod zip_tests {
    use super::StableEncode;
    use crate::{
        prelude::{AdditionOps, CacheOps, Graph, NO_PROPS},
        serialise::{metadata::GraphMetadata, GraphFolder},
    };
    use raphtory_api::core::utils::logging::global_info_logger;

    #[test]
    fn test_load_cached_from_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        graph.encode(GraphFolder::new_as_zip(&temp_file)).unwrap();
        let result = Graph::load_cached(&temp_file);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_metadata_from_noninitialized_zip() {
        global_info_logger();

        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let folder = GraphFolder::new_as_zip(&temp_file);
        folder.write_graph_data(&graph).unwrap();

        let err = folder.try_read_metadata();
        assert!(err.is_err());

        let result = folder.read_metadata().unwrap();
        assert_eq!(
            result,
            GraphMetadata {
                node_count: 1,
                edge_count: 0,
                properties: vec![]
            }
        );
    }

    #[test]
    fn test_read_metadata_from_noninitialized_folder() {
        global_info_logger();
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        let temp_folder = tempfile::TempDir::new().unwrap();
        let folder = GraphFolder::from(temp_folder.path());
        folder.write_graph_data(&graph).unwrap();
        let err = folder.try_read_metadata();
        assert!(err.is_err());
        let result = folder.read_metadata().unwrap();
        assert_eq!(
            result,
            GraphMetadata {
                node_count: 1,
                edge_count: 0,
                properties: vec![]
            }
        );
    }
}
