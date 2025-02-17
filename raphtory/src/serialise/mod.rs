use memmap2::Mmap;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

pub(crate) mod incremental;
pub(crate) mod parquet;
mod proto_ext;
mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub use proto::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode};

use crate::core::utils::errors::GraphError;

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

    pub fn write_graph(&self, buf: &[u8]) -> Result<(), GraphError> {
        if self.prefer_zip_format {
            let file = File::create(&self.root_folder)?;
            let mut zip = ZipWriter::new(file);
            zip.start_file::<_, ()>(GRAPH_FILE_NAME, FileOptions::default())?;
            Ok(zip.write_all(buf)?)
        } else {
            self.ensure_clean_root_dir()?;
            let mut file = File::create(self.get_graph_path())?;
            Ok(file.write_all(buf)?)
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
// the default and is largelly exercised in other places
#[cfg(test)]
mod zip_tests {
    use crate::{
        prelude::{AdditionOps, CacheOps, Graph, GraphViewOps, NO_PROPS},
        serialise::GraphFolder,
    };

    use super::{StableDecode, StableEncode};

    #[test]
    fn test_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        graph.encode(GraphFolder::new_as_zip(&temp_file)).unwrap();
        let graph = Graph::decode(&temp_file).unwrap();
        assert_eq!(graph.count_nodes(), 1);
    }

    #[test]
    fn test_load_cached_from_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        graph.encode(GraphFolder::new_as_zip(&temp_file)).unwrap();
        let result = Graph::load_cached(&temp_file);
        assert!(result.is_err());
    }
}
