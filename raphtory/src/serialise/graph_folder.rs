use crate::{
    db::api::view::internal::GraphView, errors::GraphError, prelude::ParquetEncoder,
    serialise::metadata::GraphMetadata,
};
use raphtory_api::core::input::input_node::parse_u64_strict;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

/// Stores graph data
pub const GRAPH_PATH: &str = "graph";
pub const DEFAULT_GRAPH_PATH: &str = "graph0";

pub const DATA_PATH: &str = "data";
pub const DEFAULT_DATA_PATH: &str = "data0";

/// Stores graph metadata
pub const META_PATH: &str = ".raph";

/// Temporary metadata for atomic replacement
pub const DIRTY_PATH: &str = ".dirty";

/// Directory that stores search indexes
pub const INDEX_PATH: &str = "index";

/// Directory that stores vector embeddings of the graph
pub const VECTORS_PATH: &str = "vectors";

pub(crate) fn valid_relative_graph_path(
    relative_path: &str,
    prefix: &str,
) -> Result<(), GraphError> {
    relative_path
        .strip_prefix(prefix) // should have the prefix
        .and_then(|id| parse_u64_strict(id)) // the remainder should be the id
        .ok_or_else(|| GraphError::InvalidRelativePath(relative_path.to_string()))?;
    Ok(())
}

fn read_path_from_file(mut file: impl Read, prefix: &str) -> Result<String, GraphError> {
    let mut value = String::new();
    file.read_to_string(&mut value)?;
    let path: RelativePath = serde_json::from_str(&value)?;
    valid_relative_graph_path(&path.path, prefix)?;
    Ok(path.path)
}

pub fn read_path_pointer(
    base_path: &Path,
    file_name: &str,
    prefix: &str,
) -> Result<Option<String>, GraphError> {
    let file = match File::open(base_path.join(file_name)) {
        Ok(file) => file,
        Err(error) => {
            return match error.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(error.into()),
            }
        }
    };
    let path = read_path_from_file(file, prefix)?;
    Ok(Some(path))
}

pub fn read_data_path(base_path: &Path, prefix: &str) -> Result<Option<String>, GraphError> {
    read_path_pointer(base_path, META_PATH, prefix)
}

pub fn read_dirty_path(base_path: &Path, prefix: &str) -> Result<Option<String>, GraphError> {
    read_path_pointer(base_path, DIRTY_PATH, prefix)
}

pub fn make_data_path(base_path: &Path, prefix: &str) -> Result<String, io::Error> {
    let mut id = read_data_path(base_path, prefix)?
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

pub fn read_or_default_data_path(base_path: &Path, prefix: &str) -> Result<String, GraphError> {
    Ok(read_data_path(base_path, prefix)?.unwrap_or_else(|| prefix.to_owned() + "0"))
}

pub fn get_zip_data_path<R: Read + Seek>(zip: &mut ZipArchive<R>) -> Result<String, GraphError> {
    let file = zip.by_name(META_PATH)?;
    Ok(read_path_from_file(file, DATA_PATH)?)
}

pub fn get_zip_graph_path<R: Read + Seek>(zip: &mut ZipArchive<R>) -> Result<String, GraphError> {
    let mut path = get_zip_data_path(zip)?;
    let graph_path = get_zip_graph_path_name(zip, path.clone())?;
    path.push('/');
    path.push_str(&graph_path);
    Ok(path)
}

pub fn get_zip_graph_path_name<R: Read + Seek>(
    zip: &mut ZipArchive<R>,
    mut data_path: String,
) -> Result<String, GraphError> {
    data_path.push('/');
    data_path.push_str(META_PATH);
    let graph_path = read_path_from_file(zip.by_name(&data_path)?, GRAPH_PATH)?;
    Ok(graph_path)
}

pub fn get_zip_meta_path<R: Read + Seek>(zip: &mut ZipArchive<R>) -> Result<String, GraphError> {
    let mut path = get_zip_data_path(zip)?;
    path.push('/');
    path.push_str(META_PATH);
    Ok(path)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelativePath {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub meta: GraphMetadata,
}

pub trait GraphPaths {
    fn root(&self) -> &Path;

    fn root_meta_path(&self) -> PathBuf {
        self.root().join(META_PATH)
    }

    fn data_path(&self) -> Result<InnerGraphFolder, GraphError> {
        Ok(InnerGraphFolder {
            path: self.root().join(self.relative_data_path()?),
        })
    }

    fn vectors_path(&self) -> Result<PathBuf, GraphError> {
        let mut path = self.data_path()?.path;
        path.push(VECTORS_PATH);
        Ok(path)
    }

    fn index_path(&self) -> Result<PathBuf, GraphError> {
        let mut path = self.data_path()?.path;
        path.push(INDEX_PATH);
        Ok(path)
    }

    fn graph_path(&self) -> Result<PathBuf, GraphError> {
        let mut path = self.data_path()?.path;
        path.push(self.relative_graph_path()?);
        Ok(path)
    }

    fn meta_path(&self) -> Result<PathBuf, GraphError> {
        let mut path = self.data_path()?.path;
        path.push(META_PATH);
        Ok(path)
    }

    fn is_zip(&self) -> bool {
        self.root().is_file()
    }

    fn read_zip(&self) -> Result<ZipArchive<File>, GraphError> {
        if self.is_zip() {
            let file = File::open(self.root())?;
            let archive = ZipArchive::new(file)?;
            Ok(archive)
        } else {
            Err(GraphError::NotAZip)
        }
    }

    fn relative_data_path(&self) -> Result<String, GraphError> {
        let path = if self.is_zip() {
            let mut zip = self.read_zip()?;
            get_zip_data_path(&mut zip)?
        } else {
            read_or_default_data_path(self.root(), DATA_PATH)?
        };
        Ok(path)
    }

    fn relative_graph_path(&self) -> Result<String, GraphError> {
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            let data_path = get_zip_data_path(&mut zip)?;
            get_zip_graph_path_name(&mut zip, data_path)
        } else {
            let data_path = self.data_path()?;
            read_or_default_data_path(data_path.as_ref(), GRAPH_PATH)
        }
    }

    fn read_metadata(&self) -> Result<GraphMetadata, GraphError> {
        let mut json = String::new();
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            let path = get_zip_meta_path(&mut zip)?;
            let mut zip_file = zip.by_name(&path)?;
            zip_file.read_to_string(&mut json)?;
        } else {
            let mut file = File::open(self.meta_path()?)?;
            file.read_to_string(&mut json)?;
        }
        let metadata: Metadata = serde_json::from_str(&json)?;
        Ok(metadata.meta)
    }

    fn write_metadata(&self, graph: impl GraphView) -> Result<(), GraphError> {
        let graph_path = self.relative_graph_path()?;
        let metadata = GraphMetadata::from_graph(graph);
        let meta = Metadata {
            path: graph_path,
            meta: metadata,
        };
        let path = self.meta_path()?;
        let file = File::create(&path)?;
        Ok(serde_json::to_writer(file, &meta)?)
    }

    /// Returns true if folder is occupied by a graph.
    fn is_reserved(&self) -> bool {
        self.is_zip() || self.meta_path().map_or(false, |path| path.exists())
    }

    /// Initialise the data folder and metadata pointer
    fn init(&self) -> Result<(), GraphError> {
        if self.root().is_dir() {
            let non_empty = self.root().read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphError::NonEmptyGraphFolder(self.root().into()));
            }
        } else {
            fs::create_dir(self.root())?
        }
        let meta_path = self.relative_data_path()?;
        fs::create_dir(self.root().join(&meta_path))?;
        fs::write(
            self.root_meta_path(),
            serde_json::to_string(&RelativePath { path: meta_path })?,
        )?;
        Ok(())
    }
}

impl<P: AsRef<Path> + ?Sized> GraphPaths for P {
    fn root(&self) -> &Path {
        self.as_ref()
    }
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

impl GraphPaths for GraphFolder {
    fn root(&self) -> &Path {
        &self.root_folder
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

    /// Reserve a folder, marking it as occupied by a graph.
    /// Returns an error if the folder has data.
    pub fn init_write(self) -> Result<WriteableGraphFolder, GraphError> {
        if self.write_as_zip_format {
            return Err(GraphError::ZippedGraphCannotBeSwapped);
        }
        let relative_data_path = self.relative_data_path()?;
        let meta = serde_json::to_string(&RelativePath {
            path: relative_data_path.clone(),
        })?;
        self.ensure_clean_root_dir()?;
        let metapath = self.root_folder.join(DIRTY_PATH);
        let mut path_file = File::create_new(&metapath)?;
        path_file.write_all(meta.as_bytes())?;
        fs::create_dir_all(self.root_folder.join(relative_data_path))?;
        Ok(WriteableGraphFolder {
            path: self.root_folder,
        })
    }

    /// Prepare a graph folder for atomically swapping the data contents.
    /// This returns an error if the folder is set to write as Zip.
    ///
    /// If a swap is already in progress (i.e., `.dirty` file exists) it is aborted and
    /// the contents of the corresponding folder are deleted.
    pub fn init_swap(self) -> Result<WriteableGraphFolder, GraphError> {
        if self.write_as_zip_format {
            return Err(GraphError::ZippedGraphCannotBeSwapped);
        }
        let old_swap = match read_dirty_path(self.root(), DATA_PATH) {
            Ok(path) => path,
            Err(_) => {
                fs::remove_file(self.root_folder.join(DIRTY_PATH))?; // dirty file is corrupted, clean it up
                None
            }
        };

        fs::create_dir_all(self.root())?;

        let swap_path = match old_swap {
            Some(relative_path) => {
                let swap_path = self.root_folder.join(relative_path);
                if swap_path.exists() {
                    fs::remove_dir_all(&swap_path)?;
                }
                swap_path
            }
            None => {
                let new_relative_data_path = make_data_path(self.root(), DATA_PATH)?;
                let new_data_path = self.root_folder.join(&new_relative_data_path);
                let meta = serde_json::to_string(&RelativePath {
                    path: new_relative_data_path,
                })?;
                let mut dirty_file = File::create_new(self.root_folder.join(DIRTY_PATH))?;
                dirty_file.write_all(meta.as_bytes())?;
                dirty_file.sync_all()?;
                new_data_path
            }
        };
        fs::create_dir_all(swap_path)?;
        Ok(WriteableGraphFolder {
            path: self.root_folder,
        })
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

    pub fn get_zip_graph_prefix(&self) -> Result<String, GraphError> {
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            Ok([get_zip_data_path(&mut zip)?, get_zip_graph_path(&mut zip)?].join("/"))
        } else {
            let data_path = read_or_default_data_path(self.root(), DATA_PATH)?;
            let graph_path = read_or_default_data_path(&self.root().join(&data_path), GRAPH_PATH)?;
            Ok([data_path, graph_path].join("/"))
        }
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

        Ok(())
    }

    pub fn is_disk_graph(&self) -> Result<bool, GraphError> {
        let meta = self.read_metadata()?;
        Ok(meta.is_diskgraph)
    }

    /// Creates a zip file from the folder.
    pub fn zip_from_folder<W: Write + Seek>(&self, mut writer: W) -> Result<(), GraphError> {
        if self.is_zip() {
            let mut reader = File::open(&self.root_folder)?;
            io::copy(&mut reader, &mut writer)?;
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
                } else if path.is_dir() && !zip_entry_name.is_empty() {
                    // Add empty directories to the zip
                    zip.add_directory::<_, ()>(zip_entry_name, FileOptions::default())?;
                }
            }

            zip.finish()?;
        }
        Ok(())
    }

    pub fn unzip_to_folder<R: Read + Seek>(&self, reader: R) -> Result<(), GraphError> {
        self.ensure_clean_root_dir()?;
        let mut archive = ZipArchive::new(reader)?;
        archive.extract(self.root())?;
        Ok(())
    }
}

#[must_use]
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub struct WriteableGraphFolder {
    path: PathBuf,
}

impl GraphPaths for WriteableGraphFolder {
    fn root(&self) -> &Path {
        &self.path
    }

    fn relative_data_path(&self) -> Result<String, GraphError> {
        let path = read_dirty_path(self.root(), DATA_PATH)?.ok_or(GraphError::NoWriteInProgress)?;
        Ok(path)
    }

    fn relative_graph_path(&self) -> Result<String, GraphError> {
        let path = read_or_default_data_path(&self.data_path()?.as_ref(), GRAPH_PATH)?;
        Ok(path)
    }

    fn init(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

impl WriteableGraphFolder {
    /// Finalise an in-progress write by atomically renaming the '.dirty' file to '.raph'
    /// and cleaning up any old data if it exists.
    ///
    /// This operation returns an error if there is no write in progress.
    pub fn finish(self) -> Result<GraphFolder, GraphError> {
        let old_data = read_data_path(self.root(), DATA_PATH)?;
        fs::rename(self.root().join(DIRTY_PATH), self.root().join(META_PATH))?;
        if let Some(old_data) = old_data {
            let old_data_path = self.root().join(old_data);
            if old_data_path.is_dir() {
                fs::remove_dir_all(old_data_path)?;
            }
        }
        Ok(GraphFolder {
            root_folder: self.path,
            write_as_zip_format: false,
        })
    }
}

#[derive(Clone, Debug)]
pub struct InnerGraphFolder {
    path: PathBuf,
}

impl AsRef<Path> for InnerGraphFolder {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl InnerGraphFolder {
    pub fn write_metadata(&self, graph: impl GraphView) -> Result<(), GraphError> {
        let graph_path = self.relative_graph_path()?;
        let metadata = GraphMetadata::from_graph(graph);
        let meta = Metadata {
            path: graph_path,
            meta: metadata,
        };
        let path = self.meta_path();
        let file = File::create(&path)?;
        Ok(serde_json::to_writer(file, &meta)?)
    }

    pub fn read_metadata(&self) -> Result<GraphMetadata, GraphError> {
        let mut json = String::new();
        let mut file = File::open(self.meta_path())?;
        file.read_to_string(&mut json)?;
        let metadata: Metadata = serde_json::from_str(&json)?;
        Ok(metadata.meta)
    }

    pub fn replace_graph(&self, graph: impl ParquetEncoder + GraphView) -> Result<(), GraphError> {
        let data_path = self.as_ref();
        let old_relative_graph_path = self.relative_graph_path()?;
        let old_graph_path = self.path.join(&old_relative_graph_path);
        let meta = GraphMetadata::from_graph(&graph);
        let new_relative_graph_path = make_data_path(&data_path, GRAPH_PATH)?;
        graph.encode_parquet(&data_path.join(&new_relative_graph_path))?;

        let dirty_path = data_path.join(DIRTY_PATH);
        fs::write(
            &dirty_path,
            &serde_json::to_vec(&Metadata {
                path: new_relative_graph_path.clone(),
                meta,
            })?,
        )?;
        fs::rename(&dirty_path, data_path.join(META_PATH))?;
        if new_relative_graph_path != old_relative_graph_path {
            fs::remove_dir_all(old_graph_path)?;
        }
        Ok(())
    }
    pub fn vectors_path(&self) -> PathBuf {
        self.path.join(VECTORS_PATH)
    }

    pub fn index_path(&self) -> PathBuf {
        self.path.join(INDEX_PATH)
    }

    pub fn meta_path(&self) -> PathBuf {
        self.path.join(META_PATH)
    }

    pub fn relative_graph_path(&self) -> Result<String, GraphError> {
        let relative =
            read_data_path(&self.path, GRAPH_PATH)?.unwrap_or_else(|| GRAPH_PATH.to_owned() + "0");
        Ok(relative)
    }

    pub fn graph_path(&self) -> Result<PathBuf, GraphError> {
        Ok(self.path.join(self.relative_graph_path()?))
    }

    fn ensure_clean_root_dir(&self) -> Result<(), GraphError> {
        if self.as_ref().exists() {
            let non_empty = self.as_ref().read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphError::NonEmptyGraphFolder(self.as_ref().to_path_buf()));
            }
        } else {
            fs::create_dir_all(self)?
        }
        Ok(())
    }

    /// Extracts a zip file to the folder.
    pub fn unzip_to_folder<R: Read + Seek>(&self, reader: R) -> Result<(), GraphError> {
        self.ensure_clean_root_dir()?;

        let mut zip = ZipArchive::new(reader)?;
        let data_dir = get_zip_data_path(&mut zip)?;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let zip_entry_name = match file.enclosed_name() {
                Some(name) => name,
                None => continue,
            };
            if let Ok(inner_path) = zip_entry_name.strip_prefix(&data_dir) {
                let out_path = self.as_ref().join(inner_path);
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
        serialise::serialise::StableDecode,
    };

    // /// Verify that the metadata is re-created if it does not exist.
    // #[test]
    // #[ignore = "Need to think about how to deal with reading old format"]
    // fn test_read_metadata_from_noninitialized_zip() {
    //     global_info_logger();
    //
    //     let graph = Graph::new();
    //     graph.add_node(0, 0, NO_PROPS, None).unwrap();
    //
    //     let tmp_dir = tempfile::TempDir::new().unwrap();
    //     let zip_path = tmp_dir.path().join("graph.zip");
    //     let folder = GraphFolder::new_as_zip(&zip_path);
    //     graph.encode(&folder).unwrap();
    //
    //     // Remove the metadata file from the zip to simulate a noninitialized zip
    //     remove_metadata_from_zip(&zip_path);
    //
    //     // Should fail because the metadata file is not present
    //     let err = folder.try_read_metadata();
    //     assert!(err.is_err());
    //
    //     // Should re-create the metadata file
    //     let result = folder.read_metadata().unwrap();
    //     assert_eq!(
    //         result,
    //         GraphMetadata {
    //             node_count: 1,
    //             edge_count: 0,
    //             metadata: vec![],
    //             graph_type: GraphType::EventGraph,
    //             is_diskgraph: false
    //         }
    //     );
    // }

    // /// Helper function to remove the metadata file from a zip
    // fn remove_metadata_from_zip(zip_path: &Path) {
    //     let mut zip_file = std::fs::File::open(&zip_path).unwrap();
    //     let mut zip_archive = zip::ZipArchive::new(&mut zip_file).unwrap();
    //     let mut temp_zip = tempfile::NamedTempFile::new().unwrap();
    //
    //     // Scope for the zip writer
    //     {
    //         let mut zip_writer = zip::ZipWriter::new(&mut temp_zip);
    //
    //         for i in 0..zip_archive.len() {
    //             let mut file = zip_archive.by_index(i).unwrap();
    //
    //             // Copy all files except the metadata file
    //             if file.name() != META_PATH {
    //                 zip_writer
    //                     .start_file::<_, ()>(file.name(), FileOptions::default())
    //                     .unwrap();
    //                 std::io::copy(&mut file, &mut zip_writer).unwrap();
    //             }
    //         }
    //
    //         zip_writer.finish().unwrap();
    //     }
    //
    //     std::fs::copy(temp_zip.path(), &zip_path).unwrap();
    // }

    // /// Verify that the metadata is re-created if it does not exist.
    // #[test]
    // #[ignore = "Need to think about how to handle reading from old format"]
    // fn test_read_metadata_from_noninitialized_folder() {
    //     global_info_logger();
    //
    //     let graph = Graph::new();
    //     graph.add_node(0, 0, NO_PROPS, None).unwrap();
    //
    //     let temp_folder = tempfile::TempDir::new().unwrap();
    //     let folder = GraphFolder::from(temp_folder.path());
    //     graph.encode(&folder).unwrap();
    //
    //     // Remove the metadata file
    //     std::fs::remove_file(folder.get_meta_path()).unwrap();
    //
    //     // Should fail because the metadata file is not present
    //     let err = folder.try_read_metadata();
    //     assert!(err.is_err());
    //
    //     // Should re-create the metadata file
    //     let result = folder.read_metadata().unwrap();
    //     assert_eq!(
    //         result,
    //         GraphMetadata {
    //             node_count: 1,
    //             edge_count: 0,
    //             metadata: vec![],
    //             graph_type: GraphType::EventGraph,
    //             is_diskgraph: false
    //         }
    //     );
    // }
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

        assert!(initial_folder.graph_path().unwrap().exists());
        assert!(initial_folder.meta_path().unwrap().exists());

        // Create a zip file from the folder
        let output_zip_path = temp_folder.path().join("output.zip");
        let output_zip_file = std::fs::File::create(&output_zip_path).unwrap();
        initial_folder.zip_from_folder(output_zip_file).unwrap();

        assert!(output_zip_path.exists());

        // Verify the output zip contains the same graph
        let zip_folder = GraphFolder::new_as_zip(&output_zip_path);
        let decoded_graph = Graph::decode(&zip_folder).unwrap();

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
        let decoded_graph = Graph::decode(&zip_folder).unwrap();

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
        assert!(graph_folder.graph_path().unwrap().exists());

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
        assert!(unzip_folder.graph_path().unwrap().exists());
        assert!(unzip_folder.meta_path().unwrap().exists());

        // Verify the extracted graph is the same as the original
        let extracted_graph = Graph::decode(&unzip_folder).unwrap();
        assert_graph_equal(&graph, &extracted_graph);
    }
}
