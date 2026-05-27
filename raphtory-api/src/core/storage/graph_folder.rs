//! const vars for file and directory names regarding exported graphs.

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
