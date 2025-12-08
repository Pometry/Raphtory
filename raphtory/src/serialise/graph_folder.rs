use crate::{
    db::api::view::{internal::GraphView, MaterializedGraph},
    errors::GraphError,
    prelude::{Graph, GraphViewOps, ParquetEncoder, PropertiesOps, StableEncode},
    serialise::{metadata::GraphMetadata, serialise::StableDecode},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};
use tracing::info;
use walkdir::WalkDir;
use zip::{
    write::{FileOptions, SimpleFileOptions},
    ZipArchive, ZipWriter,
};

/// Stores graph data
pub const GRAPH_PATH: &str = "graph";

pub const DATA_PATH: &str = "data";

/// Stores graph metadata
pub const META_PATH: &str = ".raph";

/// Temporary metadata for atomic replacement
pub const DIRTY_PATH: &str = ".dirty";

/// Directory that stores search indexes
pub const INDEX_PATH: &str = "index";

/// Directory that stores vector embeddings of the graph
pub const VECTORS_PATH: &str = "vectors";

fn read_path_from_file(mut file: impl Read) -> Result<String, GraphError> {
    let mut value = String::new();
    file.read_to_string(&mut value)?;
    let path: RelativePath = serde_json::from_str(&value)?;
    Ok(path.path)
}

pub fn read_path_pointer(base_path: &Path, file_name: &str) -> Result<Option<String>, io::Error> {
    let file = match File::open(base_path.join(file_name)) {
        Ok(file) => file,
        Err(error) => {
            return match error.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(error),
            }
        }
    };
    let path = read_path_from_file(file)?;
    Ok(Some(path))
}

pub fn read_data_path(base_path: &Path) -> Result<Option<String>, io::Error> {
    read_path_pointer(base_path, META_PATH)
}

pub fn read_dirty_path(base_path: &Path) -> Result<Option<String>, io::Error> {
    read_path_pointer(base_path, DIRTY_PATH)
}

pub fn make_data_path(base_path: &Path, prefix: &str) -> Result<String, io::Error> {
    let mut id = read_data_path(base_path)?
        .and_then(|path| {
            path.strip_prefix(prefix)
                .and_then(|id| id.parse::<usize>().ok())
        })
        .map_or(0, |id| id + 1);

    let mut path = format!("{prefix}{id}");
    while base_path.join(&path).exists() {
        id += 1;
        path = format!("{prefix}{id}");
    }
    Ok(path)
}

fn get_zip_data_path(zip: &mut ZipArchive<File>) -> Result<String, GraphError> {
    let file = zip.by_name(META_PATH)?;
    Ok(read_path_from_file(file)?)
}

fn get_zip_graph_path(zip: &mut ZipArchive<File>) -> Result<String, GraphError> {
    let mut path = get_zip_data_path(zip)?;
    let graph_path = get_zip_graph_path_name(zip, path.clone())?;
    path.push('/');
    path.push_str(&graph_path);
    Ok(graph_path)
}

fn get_zip_graph_path_name(
    zip: &mut ZipArchive<File>,
    mut data_path: String,
) -> Result<String, GraphError> {
    data_path.push('/');
    data_path.push_str(META_PATH);
    let graph_path = read_path_from_file(zip.by_name(&data_path)?)?;
    Ok(graph_path)
}

fn get_zip_meta_path(zip: &mut ZipArchive<File>) -> Result<String, GraphError> {
    let mut path = get_zip_graph_path(zip)?;
    path.push('/');
    path.push_str(META_PATH);
    Ok(path)
}

fn file_opts() -> SimpleFileOptions {
    SimpleFileOptions::default()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelativePath {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub meta: Option<GraphMetadata>,
}

/// A container for managing graph data.
///
/// Folder structure:
///
/// GraphFolder
/// ├── .raph         # Metadata file (json: {path: "data_{id}"})
/// └── data_{id}/    # Data folder (incremental id for atomic replacement)
///     ├── .raph         # Metadata file (json: {path: "graph_{id}", meta: {}})
///     ├── graph_{id}/   # Graph data
///     ├── index/        # Search indexes (optional)
///     └── vectors/      # Vector embeddings (optional)
///
/// If `write_as_zip_format` is true, then the folder is compressed
/// and stored as a zip file.
#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct GraphFolder {
    root_folder: PathBuf,
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

    pub fn root(&self) -> &Path {
        &self.root_folder
    }

    /// Reserve a folder, marking it as occupied by a graph.
    /// Returns an error if the folder has data.
    pub fn init(&self) -> Result<(), GraphError> {
        let relative_data_path = self.get_relative_data_path()?;
        let meta = serde_json::to_string(&RelativePath {
            path: relative_data_path.clone(),
        })?;
        if self.write_as_zip_format {
            let file = File::create_new(&self.root_folder)?;
            let mut zip = ZipWriter::new(BufWriter::new(file));
            zip.start_file(META_PATH, file_opts())?;
            zip.write_all(meta.as_bytes())?;
            zip.add_directory(relative_data_path, file_opts())?;
            zip.flush()?;
        } else {
            self.ensure_clean_root_dir()?;
            let data_path = self.root_folder.join(META_PATH);
            let mut path_file = File::create_new(&data_path)?;
            path_file.write_all(meta.as_bytes())?;

            fs::create_dir_all(&data_path)?;
        }
        Ok(())
    }

    /// Returns true if folder is occupied by a graph.
    pub fn is_reserved(&self) -> bool {
        self.get_meta_path().map_or(false, |path| path.exists())
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

    pub fn get_relative_data_path(&self) -> Result<String, GraphError> {
        let path = if self.is_zip() {
            let mut zip = self.read_zip()?;
            get_zip_data_path(&mut zip)?
        } else {
            read_data_path(&self.root_folder)?.unwrap_or_else(|| DATA_PATH.to_string() + "0")
        };
        Ok(path)
    }

    pub fn get_data_path(&self) -> Result<PathBuf, io::Error> {
        let relative =
            read_data_path(&self.root_folder)?.unwrap_or_else(|| DATA_PATH.to_string() + "0");
        Ok(self.root_folder.join(relative))
    }

    pub fn get_graph_path(&self) -> Result<PathBuf, io::Error> {
        let mut path = self.get_data_path()?;
        let relative = read_data_path(&path)?.unwrap_or_else(|| GRAPH_PATH.to_string() + "0");
        path.push(relative);
        Ok(path)
    }

    pub fn get_relative_graph_path(&self) -> Result<String, GraphError> {
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            let data_path = get_zip_data_path(&mut zip)?;
            get_zip_graph_path_name(&mut zip, data_path)
        } else {
            let data_path = self.get_data_path()?;
            Ok(read_data_path(&data_path)?.unwrap_or_else(|| GRAPH_PATH.to_string() + "0"))
        }
    }

    pub fn get_meta_path(&self) -> Result<PathBuf, io::Error> {
        let mut path = self.get_data_path()?;
        path.push(META_PATH);
        Ok(path)
    }

    pub fn get_index_path(&self) -> Result<PathBuf, io::Error> {
        let mut path = self.get_data_path()?;
        path.push(INDEX_PATH);
        Ok(path)
    }

    pub fn get_vectors_path(&self) -> Result<PathBuf, io::Error> {
        let mut path = self.get_data_path()?;
        path.push(VECTORS_PATH);
        Ok(path)
    }

    pub fn get_base_path(&self) -> &Path {
        &self.root_folder
    }

    pub fn is_zip(&self) -> bool {
        self.root_folder.is_file()
    }

    fn read_zip(&self) -> Result<ZipArchive<File>, GraphError> {
        if self.is_zip() {
            let file = File::open(&self.root_folder)?;
            let archive = ZipArchive::new(file)?;
            Ok(archive)
        } else {
            Err(GraphError::NotAZip)
        }
    }

    pub fn replace_graph(&self, graph: impl ParquetEncoder + GraphView) -> Result<(), GraphError> {
        let data_path = self.get_data_path()?;
        let old_graph_path = self.get_graph_path().ok();
        let new_graph_path = make_data_path(&data_path, GRAPH_PATH)?;
        let meta = Some(GraphMetadata::from_graph(&graph));
        let new_graph_folder = data_path.join(&new_graph_path);
        let dirty_path = data_path.join(DIRTY_PATH);
        fs::write(
            &dirty_path,
            &serde_json::to_vec(&Metadata {
                path: new_graph_path,
                meta,
            })?,
        )?;
        graph.encode_parquet(&new_graph_folder)?;
        fs::rename(&dirty_path, data_path.join(META_PATH))?;
        if let Some(old_graph_path) = old_graph_path {
            fs::remove_dir_all(old_graph_path)?;
        }
        Ok(())
    }

    pub fn read_metadata(&self) -> Result<GraphMetadata, GraphError> {
        let mut json = String::new();
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            let path = get_zip_meta_path(&mut zip)?;
            let mut zip_file = zip.by_name(&path)?;
            zip_file.read_to_string(&mut json)?;
        } else {
            let mut file = File::open(self.get_meta_path()?)?;
            file.read_to_string(&mut json)?;
        }
        let metadata = serde_json::from_str(&json)?;
        Ok(metadata)
    }

    pub fn write_metadata<'graph>(
        &self,
        graph: &impl GraphViewOps<'graph>,
    ) -> Result<(), GraphError> {
        let graph_path = self.get_relative_graph_path()?;
        let data_path = self.get_relative_data_path()?;
        let metadata = GraphMetadata::from_graph(graph);
        let meta = Metadata {
            path: graph_path,
            meta: Some(metadata),
        };

        if self.write_as_zip_format {
            let file = File::options()
                .read(true)
                .write(true)
                .open(&self.get_base_path())?;
            let mut zip = ZipWriter::new_append(file)?;

            zip.start_file::<_, ()>(META_PATH, FileOptions::default())?;
            Ok(serde_json::to_writer(zip, &meta)?)
        } else {
            let path = self.get_meta_path()?;
            let file = File::create(path.clone())?;
            Ok(serde_json::to_writer(file, &meta)?)
        }
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

    fn is_disk_graph(&self) -> Result<bool, GraphError> {
        let meta = self.read_metadata()?;
        Ok(meta.is_diskgraph)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::graph::graph::assert_graph_equal,
        prelude::{AdditionOps, Graph, Prop, StableEncode, NO_PROPS},
    };
    use raphtory_api::{core::utils::logging::global_info_logger, GraphType};

    /// Verify that the metadata is re-created if it does not exist.
    #[test]
    #[ignore = "Need to think about how to deal with reading old format"]
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
                is_diskgraph: false
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
    #[ignore = "Need to think about how to handle reading from old format"]
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
                is_diskgraph: false
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
