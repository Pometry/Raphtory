use memmap2::Mmap;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::api::view::MaterializedGraph,
    errors::GraphError,
    prelude::{GraphViewOps, PropertiesOps},
    serialise::{
        metadata::GraphMetadata,
        serialise::StableDecode,
        serialise::StableEncode,
    },
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};
use tracing::info;

pub const GRAPH_FILE_NAME: &str = "graph";
pub const META_FILE_NAME: &str = ".raph";
const INDEX_PATH: &str = "index";
const VECTORS_PATH: &str = "vectors";

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct GraphFolder {
    pub root_folder: PathBuf,
    pub(crate) write_as_zip_format: bool,
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
            write_as_zip_format: true,
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
        self.root_folder.join(VECTORS_PATH)
    }

    pub fn get_index_path(&self) -> PathBuf {
        self.root_folder.join(INDEX_PATH)
    }

    // TODO: make private once possible
    pub fn get_base_path(&self) -> &Path {
        &self.root_folder
    }

    pub fn is_zip(&self) -> bool {
        self.root_folder.is_file()
    }

    pub fn read_metadata(&self) -> Result<GraphMetadata, GraphError> {
        match self.try_read_metadata() {
            Ok(data) => Ok(data),
            Err(e) => {
                match e.kind() {
                    // In the case that the file is not found or invalid, try creating it then re-reading
                    ErrorKind::NotFound | ErrorKind::InvalidData | ErrorKind::UnexpectedEof => {
                        info!(
                            "Metadata file does not exist or is invalid. Attempting to recreate..."
                        );
                        let graph: MaterializedGraph = MaterializedGraph::decode(self)?;
                        self.write_metadata(&graph)?;
                        Ok(self.try_read_metadata()?)
                    }
                    _ => Err(e.into()),
                }
            }
        }
    }

    pub fn try_read_metadata(&self) -> Result<GraphMetadata, io::Error> {
        if self.is_zip() {
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

    pub fn write_metadata<'graph>(&self, graph: &impl GraphViewOps<'graph>) -> Result<(), GraphError> {
        let node_count = graph.count_nodes();
        let edge_count = graph.count_edges();
        let properties = graph.metadata();
        let metadata = GraphMetadata {
            node_count,
            edge_count,
            metadata: properties.as_vec(),
        };

        if self.write_as_zip_format {
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

    pub(crate) fn get_appendable_graph_file(&self) -> Result<File, io::Error> {
        let path = self.get_graph_path();
        Ok(OpenOptions::new().append(true).open(path)?)
    }

    pub(crate) fn ensure_clean_root_dir(&self) -> Result<(), GraphError> {
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

    fn is_disk_graph(&self) -> bool {
        let path = self.get_graph_path();
        path.is_dir()
    }

    pub fn create_zip<W: Write + Seek>(&self, mut writer: W) -> Result<(), GraphError> {
        let mut buffer = Vec::new();

        if self.is_zip() {
            let mut reader = File::open(&self.root_folder)?;
            reader.read_to_end(&mut buffer)?;
            writer.write_all(&buffer)?;
        } else {
            let mut zip = ZipWriter::new(writer);
            let graph_file = self.get_graph_path();
            {
                // scope for file
                let mut reader = File::open(&graph_file)?;
                reader.read_to_end(&mut buffer)?;
                zip.start_file::<_, ()>(GRAPH_FILE_NAME, FileOptions::default())?;
                zip.write_all(&buffer)?;
            }
            {
                // scope for file
                buffer.clear();
                let mut reader = File::open(self.get_meta_path())?;
                reader.read_to_end(&mut buffer)?;
                zip.start_file::<_, ()>(META_FILE_NAME, FileOptions::default())?;
                zip.write_all(&buffer)?;
            }
        }
        Ok(())
    }
}

impl<P: AsRef<Path>> From<P> for GraphFolder {
    fn from(value: P) -> Self {
        let path: &Path = value.as_ref();
        Self {
            root_folder: path.to_path_buf(),
            write_as_zip_format: false,
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
    use super::*;
    use crate::{
        prelude::{AdditionOps, Graph, NO_PROPS},
        serialise::metadata::GraphMetadata,
    };
    use raphtory_api::core::utils::logging::global_info_logger;

    #[test]
    fn test_load_cached_from_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let zip_path = tmp_dir.path().join("graph.zip");
        graph.encode(GraphFolder::new_as_zip(&zip_path)).unwrap();
        let result = Graph::load_cached(&zip_path);

        assert!(result.is_err());
    }

    /// Verify that the metadata is re-created if it does not exist.
    #[test]
    fn test_read_metadata_from_noninitialized_zip() {
        global_info_logger();

        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let zip_path = tmp_dir.path().join("graph.zip");
        let folder = GraphFolder::new_as_zip(&zip_path);
        graph.encode(&folder).unwrap();

        // Remove the metadata file from the zip to simulate a noninitialized zip
        remove_metadata_from_zip(&zip_path);

        // Should fail because the metadata file is not present
        let err = folder.try_read_metadata();
        assert!(err.is_err());

        // Should re-create the metadata file
        let result = folder.read_metadata().unwrap();
        assert_eq!(
            result,
            GraphMetadata {
                node_count: 1,
                edge_count: 0,
                metadata: vec![]
            }
        );
    }

    /// Helper function to remove the metadata file from a zip
    fn remove_metadata_from_zip(zip_path: &Path) {
        let mut zip_file = std::fs::File::open(&zip_path).unwrap();
        let mut zip_archive = zip::ZipArchive::new(&mut zip_file).unwrap();
        let mut temp_zip = tempfile::NamedTempFile::new().unwrap();

        // Scope for the zip writer
        {
            let mut zip_writer = zip::ZipWriter::new(&mut temp_zip);

            for i in 0..zip_archive.len() {
                let mut file = zip_archive.by_index(i).unwrap();

                // Copy all files except the metadata file
                if file.name() != META_FILE_NAME {
                    zip_writer.start_file::<_, ()>(file.name(), FileOptions::default()).unwrap();
                    std::io::copy(&mut file, &mut zip_writer).unwrap();
                }
            }

            zip_writer.finish().unwrap();
        }

        std::fs::copy(temp_zip.path(), &zip_path).unwrap();
    }

    /// Verify that the metadata is re-created if it does not exist.
    #[test]
    fn test_read_metadata_from_noninitialized_folder() {
        global_info_logger();

        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();

        let temp_folder = tempfile::TempDir::new().unwrap();
        let folder = GraphFolder::from(temp_folder.path());
        graph.encode(&folder).unwrap();

        // Remove the metadata file
        std::fs::remove_file(folder.get_meta_path()).unwrap();

        // Should fail because the metadata file is not present
        let err = folder.try_read_metadata();
        assert!(err.is_err());

        // Should re-create the metadata file
        let result = folder.read_metadata().unwrap();
        assert_eq!(
            result,
            GraphMetadata {
                node_count: 1,
                edge_count: 0,
                metadata: vec![]
            }
        );
    }
}
