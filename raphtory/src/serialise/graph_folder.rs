use crate::{
    db::api::view::MaterializedGraph,
    errors::GraphError,
    prelude::{GraphViewOps, PropertiesOps},
    serialise::{metadata::GraphMetadata, serialise::StableDecode},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};
use tracing::info;
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

/// Stores graph data
pub const GRAPH_PATH: &str = "graph";

/// Stores graph metadata
pub const META_PATH: &str = ".raph";

/// Directory that stores search indexes
pub const INDEX_PATH: &str = "index";

/// Directory that stores vector embeddings of the graph
pub const VECTORS_PATH: &str = "vectors";

/// A container for managing graph data.
/// Folder structure:
///
/// GraphFolder
/// ├── graph/        # Graph data
/// ├── .raph         # Metadata file
/// ├── index/        # Search indexes (optional)
/// └── vectors/      # Vector embeddings (optional)
///
/// If `write_as_zip_format` is true, then the folder is compressed
/// and stored as a zip file.
#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct GraphFolder {
    pub root_folder: PathBuf,
    pub(crate) write_as_zip_format: bool,
}

impl GraphFolder {
    pub fn new_as_zip(path: impl AsRef<Path>) -> Self {
        let folder: GraphFolder = path.into();
        Self {
            write_as_zip_format: true,
            ..folder
        }
    }

    /// Reserve a folder, marking it as occupied by a graph.
    /// Returns an error if `write_as_zip_format` is true or if the folder has data.
    pub fn reserve(&self) -> Result<(), GraphError> {
        if self.write_as_zip_format {
            return Err(GraphError::IOErrorMsg(
                "Cannot reserve a zip folder".to_string(),
            ));
        }

        self.ensure_clean_root_dir()?;

        // Mark as occupied using empty metadata & graph data.
        File::create_new(self.get_meta_path())?;
        fs::create_dir_all(self.get_graph_path())?;

        Ok(())
    }

    /// Returns true if folder is occupied by a graph.
    pub fn is_reserved(&self) -> bool {
        self.get_meta_path().exists()
    }

    /// Clears the folder of any contents.
    pub fn clear(&self) -> Result<(), GraphError> {
        if self.is_zip() {
            return Err(GraphError::IOErrorMsg(
                "Cannot clear a zip folder".to_string(),
            ));
        }

        fs::remove_dir_all(&self.root_folder)?;
        fs::create_dir_all(&self.root_folder)?;
        Ok(())
    }

    pub fn get_graph_path(&self) -> PathBuf {
        self.root_folder.join(GRAPH_PATH)
    }

    pub fn get_meta_path(&self) -> PathBuf {
        self.root_folder.join(META_PATH)
    }

    pub fn get_index_path(&self) -> PathBuf {
        self.root_folder.join(INDEX_PATH)
    }

    pub fn get_vectors_path(&self) -> PathBuf {
        self.root_folder.join(VECTORS_PATH)
    }

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

                        let graph: MaterializedGraph = MaterializedGraph::decode(self, None)?;

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
            let zip_file = archive.by_name(META_PATH)?;
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

    pub fn write_metadata<'graph>(
        &self,
        graph: &impl GraphViewOps<'graph>,
    ) -> Result<(), GraphError> {
        let node_count = graph.count_nodes();
        let edge_count = graph.count_edges();
        let properties = graph.metadata();
        let metadata = GraphMetadata {
            node_count,
            edge_count,
            metadata: properties.as_vec(),
            graph_type: graph.graph_type(),
        };

        if self.write_as_zip_format {
            let file = File::options()
                .read(true)
                .write(true)
                .open(&self.get_base_path())?;
            let mut zip = ZipWriter::new_append(file)?;

            zip.start_file::<_, ()>(META_PATH, FileOptions::default())?;
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

    fn ensure_clean_root_dir(&self) -> Result<(), GraphError> {
        if self.root_folder.exists() {
            let non_empty = self.root_folder.read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphError::NonEmptyGraphFolder(self.root_folder.clone()));
            }
        } else {
            fs::create_dir_all(&self.root_folder)?
        }

        Ok(())
    }

    fn is_disk_graph(&self) -> bool {
        let path = self.get_graph_path();
        path.is_dir()
    }

    /// Creates a zip file from the folder.
    pub fn zip_from_folder<W: Write + Seek>(&self, mut writer: W) -> Result<(), GraphError> {
        let mut buffer = Vec::new();

        if self.is_zip() {
            let mut reader = File::open(&self.root_folder)?;
            reader.read_to_end(&mut buffer)?;
            writer.write_all(&buffer)?;
        } else {
            let mut zip = ZipWriter::new(writer);

            for entry in WalkDir::new(&self.root_folder)
                .into_iter()
                .filter_map(Result::ok)
            {
                let path = entry.path();
                let rel_path = path.strip_prefix(&self.root_folder).map_err(|e| {
                    GraphError::IOErrorMsg(format!("Failed to strip prefix from path: {}", e))
                })?;

                let zip_entry_name = rel_path.to_string_lossy().into_owned();

                if path.is_file() {
                    zip.start_file::<_, ()>(zip_entry_name, FileOptions::default())?;

                    let mut file = File::open(path)?;
                    std::io::copy(&mut file, &mut zip)?;
                } else if path.is_dir() {
                    // Add empty directories to the zip
                    zip.add_directory::<_, ()>(zip_entry_name, FileOptions::default())?;
                }
            }

            zip.finish()?;
        }

        Ok(())
    }

    /// Extracts a zip file to the folder.
    pub fn unzip_to_folder<R: Read + Seek>(&self, reader: R) -> Result<(), GraphError> {
        if self.write_as_zip_format {
            return Err(GraphError::IOErrorMsg(
                "Cannot unzip to a zip format folder".to_string(),
            ));
        }

        self.ensure_clean_root_dir()?;

        let mut zip = ZipArchive::new(reader)?;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let zip_entry_name = match file.enclosed_name() {
                Some(name) => name,
                None => continue,
            };

            let out_path = self.root_folder.join(zip_entry_name);

            if file.is_dir() {
                std::fs::create_dir_all(&out_path)?;
            } else {
                // Create any parent directories
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let mut out_file = std::fs::File::create(&out_path)?;
                std::io::copy(&mut file, &mut out_file)?;
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
        db::graph::graph::assert_graph_equal,
        prelude::{AdditionOps, Graph, Prop, StableEncode, NO_PROPS},
    };
    use raphtory_api::{core::utils::logging::global_info_logger, GraphType};

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
                metadata: vec![],
                graph_type: GraphType::EventGraph,
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
                if file.name() != META_PATH {
                    zip_writer
                        .start_file::<_, ()>(file.name(), FileOptions::default())
                        .unwrap();
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
                metadata: vec![],
                graph_type: GraphType::EventGraph,
            }
        );
    }

    #[test]
    fn test_zip_from_folder() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        // Create a regular folder and encode the graph
        let temp_folder = tempfile::TempDir::new().unwrap();
        let initial_folder = GraphFolder::from(temp_folder.path().join("initial"));
        graph.encode(&initial_folder).unwrap();

        assert!(initial_folder.get_graph_path().exists());
        assert!(initial_folder.get_meta_path().exists());

        // Create a zip file from the folder
        let output_zip_path = temp_folder.path().join("output.zip");
        let output_zip_file = std::fs::File::create(&output_zip_path).unwrap();
        initial_folder.zip_from_folder(output_zip_file).unwrap();

        assert!(output_zip_path.exists());

        // Verify the output zip contains the same graph
        let zip_folder = GraphFolder::new_as_zip(&output_zip_path);
        let decoded_graph = Graph::decode(&zip_folder, None::<&std::path::Path>).unwrap();

        assert_graph_equal(&graph, &decoded_graph);
    }

    #[test]
    fn test_zip_from_zip() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        // Create an initial zip file
        let temp_folder = tempfile::TempDir::new().unwrap();
        let initial_zip_path = temp_folder.path().join("initial.zip");
        let initial_folder = GraphFolder::new_as_zip(&initial_zip_path);
        graph.encode(&initial_folder).unwrap();

        assert!(initial_zip_path.exists());

        // Create a new zip file from the existing zip
        let output_zip_path = temp_folder.path().join("output.zip");
        let output_zip_file = std::fs::File::create(&output_zip_path).unwrap();
        initial_folder.zip_from_folder(output_zip_file).unwrap();

        assert!(output_zip_path.exists());

        // Verify zip file sizes
        let initial_size = std::fs::metadata(&initial_zip_path).unwrap().len();
        let output_size = std::fs::metadata(&output_zip_path).unwrap().len();
        assert_eq!(initial_size, output_size);

        // Verify the output zip contains the same graph
        let zip_folder = GraphFolder::new_as_zip(&output_zip_path);
        let decoded_graph = Graph::decode(&zip_folder, None::<&std::path::Path>).unwrap();

        assert_graph_equal(&graph, &decoded_graph);
    }

    #[test]
    fn test_unzip_to_folder() {
        let graph = Graph::new();

        graph
            .add_edge(0, 0, 1, [("test prop 1", Prop::map(NO_PROPS))], None)
            .unwrap();
        graph
            .add_edge(
                1,
                2,
                3,
                [("test prop 1", Prop::map([("key", "value")]))],
                Some("layer_a"),
            )
            .unwrap();
        graph
            .add_edge(2, 3, 4, [("test prop 2", "value")], Some("layer_b"))
            .unwrap();
        graph
            .add_edge(3, 1, 4, [("test prop 3", 10.0)], None)
            .unwrap();
        graph
            .add_edge(4, 1, 3, [("test prop 4", true)], None)
            .unwrap();

        let temp_folder = tempfile::TempDir::new().unwrap();
        let folder = temp_folder.path().join("graph");
        let graph_folder = GraphFolder::from(&folder);

        graph.encode(&graph_folder).unwrap();
        assert!(graph_folder.get_graph_path().exists());

        // Zip the folder
        let mut zip_bytes = Vec::new();
        let cursor = std::io::Cursor::new(&mut zip_bytes);
        graph_folder.zip_from_folder(cursor).unwrap();

        // Unzip to a new folder
        let folder = temp_folder.path().join("unzip");
        let unzip_folder = GraphFolder::from(&folder);
        let cursor = std::io::Cursor::new(&zip_bytes);
        unzip_folder.unzip_to_folder(cursor).unwrap();

        // Verify the extracted folder has the same structure
        assert!(unzip_folder.get_graph_path().exists());
        assert!(unzip_folder.get_meta_path().exists());

        // Verify the extracted graph is the same as the original
        let extracted_graph = Graph::decode(&unzip_folder, None::<&std::path::Path>).unwrap();
        assert_graph_equal(&graph, &extracted_graph);
    }
}
