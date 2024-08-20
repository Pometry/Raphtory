use crate::core::{utils::time::error::ParseTimeError, Prop, PropType};
#[cfg(feature = "arrow")]
use polars_arrow::legacy::error;
use pometry_storage::RAError;
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::path::PathBuf;
#[cfg(feature = "search")]
use tantivy;
#[cfg(feature = "search")]
use tantivy::query::QueryParserError;

#[derive(thiserror::Error, Debug)]
pub enum InvalidPathReason {
    #[error("Backslash not allowed in path: {0}")]
    BackslashError(PathBuf),
    #[error("Double forward slashes are not allowed in path: {0}")]
    DoubleForwardSlash(PathBuf),
    #[error("Only relative paths are allowed to be used within the working_dir: {0}")]
    RootNotAllowed(PathBuf),
    #[error("References to the current dir are not allowed within the path: {0}")]
    CurDirNotAllowed(PathBuf),
    #[error("References to the parent dir are not allowed within the path: {0}")]
    ParentDirNotAllowed(PathBuf),
    #[error("A component of the given path was a symlink: {0}")]
    SymlinkNotAllowed(PathBuf),
    #[error("Could not strip working_dir prefix when getting relative path: {0}")]
    StripPrefixError(PathBuf),
    #[error("The give path does not exist: {0}")]
    PathDoesNotExist(PathBuf),
    #[error("Could not parse Path: {0}")]
    PathNotParsable(PathBuf),
    #[error("Path is not valid UTF8: {0}")]
    PathNotUTF8(PathBuf),
    #[error("The give path is a directory, but should be a file or not exist: {0}")]
    PathIsDirectory(PathBuf),
}

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[cfg(feature = "arrow")]
    #[error("Arrow error: {0}")]
    Arrow(#[from] error::PolarsError),
    #[error("Invalid path: {source:?}")]
    InvalidPath {
        #[from]
        source: InvalidPathReason,
    },
    #[error("Graph error occurred")]
    UnsupportedDataType,
    #[error("Disk graph not found")]
    DiskGraphNotFound,
    #[error("Disk Graph is immutable")]
    ImmutableDiskGraph,
    #[error("Event Graph doesn't support deletions")]
    EventGraphDeletionsNotSupported,
    #[error("Graph not found {0}")]
    GraphNotFound(PathBuf),
    #[error("Graph already exists by name = {0}")]
    GraphNameAlreadyExists(PathBuf),
    #[error("Immutable graph reference already exists. You can access mutable graph apis only exclusively.")]
    IllegalGraphAccess,
    #[error("Incorrect property given.")]
    IncorrectPropertyType,
    #[error("Failed to mutate graph")]
    FailedToMutateGraph {
        #[from]
        source: MutateGraphError,
    },
    #[error("Failed to mutate graph property")]
    FailedToMutateGraphProperty { source: MutateGraphError },

    #[error("Wrong type for property {name}: expected {expected:?} but actual type is {actual:?}")]
    PropertyTypeError {
        name: String,
        expected: PropType,
        actual: PropType,
    },

    #[error("Tried to mutate constant property {name}: old value {old:?}, new value {new:?}")]
    ConstantPropertyMutationError { name: ArcStr, old: Prop, new: Prop },

    #[error("Failed to parse time string")]
    ParseTime {
        #[from]
        source: ParseTimeError,
    },

    #[error("Node already exists with ID {0:?}")]
    NodeExistsError(GID),

    #[error("Edge already exists for nodes {0:?} {1:?}")]
    EdgeExistsError(GID, GID),

    #[error("No Node with ID {0}")]
    NodeIdError(u64),

    #[error("No Node with name {0}")]
    NodeNameError(String),

    #[error("Node Type Error {0}")]
    NodeTypeError(String),

    #[error("No Edge between {src} and {dst}")]
    EdgeIdError { src: u64, dst: u64 },

    #[error("No Edge between {src} and {dst}")]
    EdgeNameError { src: String, dst: String },
    // wasm
    #[error("Node is not String or Number")]
    NodeIdNotStringOrNumber,
    #[error("Invalid layer: {invalid_layer}. Valid layers: {valid_layers}")]
    InvalidLayer {
        invalid_layer: String,
        valid_layers: String,
    },
    #[error("Layer {layer} does not exist for edge ({src}, {dst})")]
    InvalidEdgeLayer {
        layer: String,
        src: String,
        dst: String,
    },
    #[error("Bincode operation failed")]
    BinCodeError {
        #[from]
        source: Box<bincode::ErrorKind>,
    },

    #[error("The loaded graph is of the wrong type. Did you mean Graph / PersistentGraph?")]
    GraphLoadError,

    #[error("IO operation failed")]
    IOError {
        #[from]
        source: std::io::Error,
    },

    #[cfg(feature = "arrow")]
    #[error("Failed to load graph: {0}")]
    LoadFailure(String),

    #[cfg(feature = "arrow")]
    #[error(
        "Failed to load graph as the following columns are not present within the dataframe: {0}"
    )]
    ColumnDoesNotExist(String),

    #[cfg(feature = "storage")]
    #[error("Raphtory Arrow Error: {0}")]
    DiskGraphError(#[from] RAError),

    #[cfg(feature = "search")]
    #[error("Index operation failed")]
    IndexError {
        #[from]
        source: tantivy::TantivyError,
    },

    #[cfg(feature = "search")]
    #[error("Index operation failed")]
    QueryError {
        #[from]
        source: QueryParserError,
    },

    #[error(
        "Failed to load the graph as the bincode version {0} is different to supported version {1}"
    )]
    BincodeVersionError(u32, u32),

    #[error("The layer_name function is only available once an edge has been exploded via .explode_layers() or .explode(). If you want to retrieve the layers for this edge you can use .layer_names")]
    LayerNameAPIError,

    #[error("The time function is only available once an edge has been exploded via .explode(). You may want to retrieve the history for this edge via .history(), or the earliest/latest time via earliest_time or latest_time")]
    TimeAPIError,

    #[error("Illegal set error {0}")]
    IllegalSet(String),

    #[cfg(feature = "proto")]
    #[error("Protobuf encode error{0}")]
    DecodeError(#[from] prost::DecodeError),

    #[cfg(feature = "proto")]
    #[error("Protobuf decode error{0}")]
    EncodeError(#[from] prost::EncodeError),

    #[cfg(feature = "proto")]
    #[error("Failed to deserialise graph: {0}")]
    DeserialisationError(String),

    #[cfg(feature = "proto")]
    #[error("Cache is not initialised")]
    CacheNotInnitialised,

    #[error("Immutable graph is .. immutable!")]
    AttemptToMutateImmutableGraph,
}

impl GraphError {
    pub fn invalid_layer(invalid_layer: String, valid_layers: Vec<String>) -> Self {
        let valid_layers = valid_layers.join(", ");
        GraphError::InvalidLayer {
            invalid_layer,
            valid_layers,
        }
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum MutateGraphError {
    #[error("Create node '{node_id}' first before adding static properties to it")]
    NodeNotFoundError { node_id: u64 },
    #[error("Unable to find layer '{layer_name}' to add property to")]
    LayerNotFoundError { layer_name: String },
    #[error("Tried to change constant graph property {name}, old value: {old_value}, new value: {new_value}")]
    IllegalGraphPropertyChange {
        name: String,
        old_value: Prop,
        new_value: Prop,
    },
    #[error("Create edge '{0}' -> '{1}' first before adding static properties to it")]
    MissingEdge(u64, u64), // src, dst
    #[error("Cannot add properties to edge view with no layers")]
    NoLayersError,
    #[error("Cannot add properties to edge view with more than one layer")]
    AmbiguousLayersError,
    #[error("Invalid Node id {0:?}")]
    InvalidNodeId(GID),
}
