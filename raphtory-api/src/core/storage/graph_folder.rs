//! Raphtory container format for managing graph data.
//!
//! Folder structure:
//!
//! GraphFolder
//! ├── .raph         # Metadata file (json: {path: "data{id}"}) pointing at the current data folder
//! └── data{id}/    # Data folder (incremental id for atomic replacement)
//!     ├── .meta         # Metadata file (json: {path: "graph{id}", meta: {}}) pointing at the current graph folder
//!     ├── graph{id}/   # Graph data (incremental id for atomic replacement)
//!     ├── index/        # Search indexes (optional)
//!     └── vectors/      # Vector embeddings (optional)

use crate::{core::input::input_node::parse_u64_strict, to_millis::ToMillis, GraphType};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io,
    io::{ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
    time::SystemTimeError,
};
use tempfile::NamedTempFile;
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

/// Metadata file that stores path to the data folder.
pub const ROOT_META_PATH: &str = ".raph";
/// Outer most directory containing all data.
pub const DATA_PATH: &str = "data";
pub const DEFAULT_DATA_PATH: &str = "data0";
/// Metadata file that stores path to the graph folder and graph metadata.
pub const GRAPH_META_PATH: &str = ".meta";
/// Directory that stores graph data.
pub const GRAPH_PATH: &str = "graph";
pub const DEFAULT_GRAPH_PATH: &str = "graph0";
/// Directory that stores search indexes.
pub const INDEX_PATH: &str = "index";
/// Directory that stores vector embeddings of the graph.
pub const VECTORS_PATH: &str = "vectors";
/// Temporary metadata file for atomic replacement.
pub const DIRTY_PATH: &str = ".dirty";

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub meta: GraphMetadata,
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub struct GraphMetadata {
    pub node_count: usize,
    pub edge_count: usize,
    pub graph_type: GraphType,
    pub is_diskgraph: bool,
}

impl Metadata {
    /// Atomically write this metadata into the data folder at `data_path`
    pub fn write_atomic(&self, data_path: &Path, meta_path: &Path) -> std::io::Result<()> {
        let mut tmp_file = NamedTempFile::new_in(data_path)?;
        serde_json::to_writer(&mut tmp_file, self).map_err(std::io::Error::other)?;
        tmp_file.as_file().sync_all()?;
        tmp_file.persist(meta_path).map_err(io::Error::from)?;
        Ok(())
    }
}

/// Errors returned by the graph folder path operations (the `GraphPaths` trait and its helpers)
#[derive(thiserror::Error, Debug)]
pub enum GraphFolderError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("zip operation failed: {0}")]
    Zip(#[from] zip::result::ZipError),

    #[error("Path {0} is not a valid relative data path")]
    InvalidRelativePath(String),

    #[error("Not a zip archive")]
    NotAZip,

    #[error("Cannot write graph into non empty folder {0}")]
    NonEmptyGraphFolder(PathBuf),

    #[error("Graph folder is not initialised for writing")]
    NoWriteInProgress,

    #[error("Cannot swap zipped graph data")]
    ZippedGraphCannotBeSwapped,

    #[error("IO operation failed: {0}")]
    IOErrorMsg(String),

    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),

    #[error("Graph path in metadata changed from {recorded:?} to {actual:?}")]
    GraphPathChanged { recorded: String, actual: String },
}

pub fn valid_path_pointer(relative_path: &str, prefix: &str) -> Result<(), GraphFolderError> {
    relative_path
        .strip_prefix(prefix) // should have the prefix
        .and_then(parse_u64_strict) // the remainder should be the id
        .ok_or_else(|| GraphFolderError::InvalidRelativePath(relative_path.to_string()))?;
    Ok(())
}

fn read_path_from_file(mut file: impl Read, prefix: &str) -> Result<String, GraphFolderError> {
    let mut value = String::new();
    file.read_to_string(&mut value)?;
    let path: RelativePath = serde_json::from_str(&value)?;
    valid_path_pointer(&path.path, prefix)?;
    Ok(path.path)
}

pub fn read_path_pointer(
    base_path: &Path,
    file_name: &str,
    prefix: &str,
) -> Result<Option<String>, GraphFolderError> {
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

pub fn make_path_pointer(
    base_path: &Path,
    file_name: &str,
    prefix: &str,
) -> Result<String, GraphFolderError> {
    let mut id = read_path_pointer(base_path, file_name, prefix)?
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

pub fn read_or_default_path_pointer(
    base_path: &Path,
    file_name: &str,
    prefix: &str,
) -> Result<String, GraphFolderError> {
    Ok(read_path_pointer(base_path, file_name, prefix)?.unwrap_or_else(|| prefix.to_owned() + "0"))
}

pub fn get_zip_data_path<R: Read + Seek>(
    zip: &mut ZipArchive<R>,
) -> Result<String, GraphFolderError> {
    let file = zip.by_name(ROOT_META_PATH)?;
    read_path_from_file(file, DATA_PATH)
}

pub fn get_zip_graph_path<R: Read + Seek>(
    zip: &mut ZipArchive<R>,
) -> Result<String, GraphFolderError> {
    let mut path = get_zip_data_path(zip)?;
    let graph_path = get_zip_graph_path_name(zip, path.clone())?;
    path.push('/');
    path.push_str(&graph_path);
    Ok(path)
}

pub fn get_zip_graph_path_name<R: Read + Seek>(
    zip: &mut ZipArchive<R>,
    mut data_path: String,
) -> Result<String, GraphFolderError> {
    data_path.push('/');
    data_path.push_str(GRAPH_META_PATH);
    let graph_path = read_path_from_file(zip.by_name(&data_path)?, GRAPH_PATH)?;
    Ok(graph_path)
}

pub fn get_zip_meta_path<R: Read + Seek>(
    zip: &mut ZipArchive<R>,
) -> Result<String, GraphFolderError> {
    let mut path = get_zip_data_path(zip)?;
    path.push('/');
    path.push_str(GRAPH_META_PATH);
    Ok(path)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelativePath {
    pub path: String,
}

pub trait GraphPaths {
    fn root(&self) -> &Path;

    fn root_meta_path(&self) -> PathBuf {
        self.root().join(ROOT_META_PATH)
    }

    fn data_path(&self) -> Result<InnerGraphFolder, GraphFolderError> {
        Ok(InnerGraphFolder {
            path: self.root().join(self.relative_data_path()?),
        })
    }

    fn vectors_path(&self) -> Result<PathBuf, GraphFolderError> {
        let mut path = self.data_path()?.path;
        path.push(VECTORS_PATH);
        Ok(path)
    }

    fn index_path(&self) -> Result<PathBuf, GraphFolderError> {
        let mut path = self.data_path()?.path;
        path.push(INDEX_PATH);
        Ok(path)
    }

    fn graph_path(&self) -> Result<PathBuf, GraphFolderError> {
        let mut path = self.data_path()?.path;
        path.push(self.relative_graph_path()?);
        Ok(path)
    }

    fn meta_path(&self) -> Result<PathBuf, GraphFolderError> {
        let mut path = self.data_path()?.path;
        path.push(GRAPH_META_PATH);
        Ok(path)
    }

    fn is_zip(&self) -> bool {
        self.root().is_file()
    }

    fn read_zip(&self) -> Result<ZipArchive<File>, GraphFolderError> {
        if self.is_zip() {
            let file = File::open(self.root())?;
            let archive = ZipArchive::new(file)?;
            Ok(archive)
        } else {
            Err(GraphFolderError::NotAZip)
        }
    }

    fn relative_data_path(&self) -> Result<String, GraphFolderError> {
        let path = if self.is_zip() {
            let mut zip = self.read_zip()?;
            get_zip_data_path(&mut zip)?
        } else {
            read_or_default_path_pointer(self.root(), ROOT_META_PATH, DATA_PATH)?
        };
        Ok(path)
    }

    fn relative_graph_path(&self) -> Result<String, GraphFolderError> {
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            let data_path = get_zip_data_path(&mut zip)?;
            get_zip_graph_path_name(&mut zip, data_path)
        } else {
            let data_path = self.data_path()?;
            read_or_default_path_pointer(data_path.as_ref(), GRAPH_META_PATH, GRAPH_PATH)
        }
    }

    fn read_metadata(&self) -> Result<GraphMetadata, GraphFolderError> {
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

    fn write_metadata(&self, meta: Metadata) -> Result<(), GraphFolderError> {
        meta.write_atomic(self.data_path()?.as_ref(), self.meta_path()?.as_ref())?;
        Ok(())
    }

    /// Returns true if folder is occupied by a graph.
    fn is_reserved(&self) -> bool {
        self.meta_path().is_ok_and(|path| path.exists())
    }

    /// Initialise the data folder and metadata pointer
    fn init(&self) -> Result<(), GraphFolderError> {
        if self.root().is_dir() {
            let non_empty = self.root().read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphFolderError::NonEmptyGraphFolder(self.root().into()));
            }
        } else {
            fs::create_dir_all(self.root())?
        }

        // Create the data folder and have the root metadata file point to it.
        let data_path = self.relative_data_path()?;
        fs::create_dir(self.root().join(&data_path))?;
        fs::write(
            self.root_meta_path(),
            serde_json::to_string(&RelativePath { path: data_path })?,
        )?;

        // Create the graph folder inside the data folder.
        let graph_path = self.graph_path()?;
        fs::create_dir(&graph_path)?;

        Ok(())
    }

    fn created(&self) -> Result<i64, GraphFolderError> {
        Ok(self.root_meta_path().metadata()?.created()?.to_millis()?)
    }

    fn last_updated(&self) -> Result<i64, GraphFolderError> {
        Ok(fs::metadata(self.meta_path()?)?.modified()?.to_millis()?)
    }

    fn last_opened(&self) -> Result<i64, GraphFolderError> {
        Ok(fs::metadata(self.meta_path()?)?.accessed()?.to_millis()?)
    }
}

impl<P: AsRef<Path> + ?Sized> GraphPaths for P {
    fn root(&self) -> &Path {
        self.as_ref()
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct GraphFolder {
    root_folder: PathBuf,
    pub write_as_zip_format: bool,
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
    pub fn init_write(self) -> Result<WriteableGraphFolder, GraphFolderError> {
        if self.write_as_zip_format {
            return Err(GraphFolderError::ZippedGraphCannotBeSwapped);
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
    pub fn init_swap(self) -> Result<WriteableGraphFolder, GraphFolderError> {
        if self.write_as_zip_format {
            return Err(GraphFolderError::ZippedGraphCannotBeSwapped);
        }
        let old_swap = match read_path_pointer(self.root(), DIRTY_PATH, DATA_PATH) {
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
                let new_relative_data_path =
                    make_path_pointer(self.root(), ROOT_META_PATH, DATA_PATH)?;
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
    pub fn clear(&self) -> Result<(), GraphFolderError> {
        if self.is_zip() {
            return Err(GraphFolderError::IOErrorMsg(
                "Cannot clear a zip folder".to_string(),
            ));
        }

        fs::remove_dir_all(&self.root_folder)?;
        fs::create_dir_all(&self.root_folder)?;
        Ok(())
    }

    pub fn get_zip_graph_prefix(&self) -> Result<String, GraphFolderError> {
        if self.is_zip() {
            let mut zip = self.read_zip()?;
            Ok([get_zip_data_path(&mut zip)?, get_zip_graph_path(&mut zip)?].join("/"))
        } else {
            let data_path = read_or_default_path_pointer(self.root(), ROOT_META_PATH, DATA_PATH)?;
            let graph_path = read_or_default_path_pointer(
                &self.root().join(&data_path),
                GRAPH_META_PATH,
                GRAPH_PATH,
            )?;
            Ok([data_path, graph_path].join("/"))
        }
    }

    fn ensure_clean_root_dir(&self) -> Result<(), GraphFolderError> {
        if self.root_folder.exists() {
            let non_empty = self.root_folder.read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphFolderError::NonEmptyGraphFolder(
                    self.root_folder.clone(),
                ));
            }
        } else {
            fs::create_dir(&self.root_folder)?
        }

        Ok(())
    }

    pub fn is_disk_graph(&self) -> Result<bool, GraphFolderError> {
        let meta = self.read_metadata()?;
        Ok(meta.is_diskgraph)
    }

    /// Creates a zip file from the folder.
    pub fn zip_from_folder<W: Write + Seek>(&self, mut writer: W) -> Result<(), GraphFolderError> {
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
                    GraphFolderError::IOErrorMsg(format!("Failed to strip prefix from path: {}", e))
                })?;

                let zip_entry_name = rel_path
                    .components()
                    .map(|name| name.as_os_str().to_string_lossy())
                    .join("/");

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

    pub fn unzip_to_folder<R: Read + Seek>(&self, reader: R) -> Result<(), GraphFolderError> {
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

    fn relative_data_path(&self) -> Result<String, GraphFolderError> {
        let path = read_path_pointer(self.root(), DIRTY_PATH, DATA_PATH)?
            .ok_or(GraphFolderError::NoWriteInProgress)?;
        Ok(path)
    }

    fn relative_graph_path(&self) -> Result<String, GraphFolderError> {
        let path =
            read_or_default_path_pointer(self.data_path()?.as_ref(), GRAPH_META_PATH, GRAPH_PATH)?;
        Ok(path)
    }

    fn init(&self) -> Result<(), GraphFolderError> {
        Ok(())
    }
}

impl WriteableGraphFolder {
    /// Finalise an in-progress write by atomically renaming the '.dirty' file to '.raph'
    /// and cleaning up any old data if it exists.
    ///
    /// This operation returns an error if there is no write in progress.
    pub fn finish(self) -> Result<GraphFolder, GraphFolderError> {
        let old_data = read_path_pointer(self.root(), ROOT_META_PATH, DATA_PATH)?;
        fs::rename(
            self.root().join(DIRTY_PATH),
            self.root().join(ROOT_META_PATH),
        )?;
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
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn write_metadata(&self, meta: Metadata) -> Result<(), GraphFolderError> {
        meta.write_atomic(self.as_ref(), &self.meta_path())?;
        Ok(())
    }

    /// Refresh the node/edge counts recorded in the metadata file, preserving the graph type.
    pub fn refresh_metadata(
        &self,
        graph_path: &str,
        node_count: usize,
        edge_count: usize,
    ) -> Result<(), GraphFolderError> {
        // nothing to refresh if there is no metadata file yet
        if !self.meta_path().exists() {
            return Ok(());
        }

        // preserve the existing graph type; a corrupt metadata file surfaces as an error here
        let existing = self.read_metadata()?;

        // the graph data directory must not change between updates
        let recorded_graph_path = self.relative_graph_path()?;
        if recorded_graph_path != graph_path {
            return Err(GraphFolderError::GraphPathChanged {
                recorded: recorded_graph_path,
                actual: graph_path.to_string(),
            });
        }

        self.write_metadata(Metadata {
            path: graph_path.to_string(),
            meta: GraphMetadata {
                node_count,
                edge_count,
                graph_type: existing.graph_type,
                is_diskgraph: true,
            },
        })?;

        Ok(())
    }

    pub fn read_metadata(&self) -> Result<GraphMetadata, GraphFolderError> {
        let mut json = String::new();
        let mut file = File::open(self.meta_path())?;
        file.read_to_string(&mut json)?;
        let metadata: Metadata = serde_json::from_str(&json)?;
        Ok(metadata.meta)
    }

    /// Atomically point the metadata file at the graph data described by `meta`, removing the
    /// previously-referenced graph directory if the path changed.
    ///
    /// NOTE: this does NOT encode the graph data itself. The caller must have already written
    /// the graph data into the directory in `meta.path` (see `replace_graph` in `raphtory`)
    pub fn replace_graph_path(&self, meta: Metadata) -> Result<(), GraphFolderError> {
        let old_relative_graph_path = self.relative_graph_path()?;
        let path_changed = meta.path != old_relative_graph_path;

        self.write_metadata(meta)?;
        if path_changed {
            fs::remove_dir_all(self.as_ref().join(&old_relative_graph_path))?;
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
        self.path.join(GRAPH_META_PATH)
    }

    pub fn relative_graph_path(&self) -> Result<String, GraphFolderError> {
        let relative = read_or_default_path_pointer(&self.path, GRAPH_META_PATH, GRAPH_PATH)?;
        Ok(relative)
    }

    pub fn graph_path(&self) -> Result<PathBuf, GraphFolderError> {
        Ok(self.path.join(self.relative_graph_path()?))
    }

    fn ensure_clean_root_dir(&self) -> Result<(), GraphFolderError> {
        if self.as_ref().exists() {
            let non_empty = self.as_ref().read_dir()?.next().is_some();
            if non_empty {
                return Err(GraphFolderError::NonEmptyGraphFolder(
                    self.as_ref().to_path_buf(),
                ));
            }
        } else {
            fs::create_dir_all(self)?
        }
        Ok(())
    }

    /// Extracts a zip file to the folder.
    pub fn unzip_to_folder<R: Read + Seek>(&self, reader: R) -> Result<(), GraphFolderError> {
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
