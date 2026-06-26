//! const vars for file and directory names regarding exported graphs.

use crate::GraphType;
use serde::{Deserialize, Serialize};
#[cfg(feature = "io")]
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

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

#[cfg(feature = "io")]
impl Metadata {
    /// Atomically write this metadata into the data folder at `data_path`
    pub fn write_atomic(&self, data_path: &Path, meta_path: &Path) -> std::io::Result<()> {
        let tmp_path = data_path.join(".tmp");
        let tmp_file = File::create(&tmp_path)?;
        serde_json::to_writer(tmp_file, self).map_err(std::io::Error::other)?;
        fs::rename(tmp_path, meta_path)?;
        Ok(())
    }
}

/// Errors returned by the graph folder path operations (the `GraphPaths` trait and its helpers)
#[cfg(feature = "io")]
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
}
