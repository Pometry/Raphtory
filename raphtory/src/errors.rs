use crate::{
    core::storage::lazy_vec::IllegalSet,
    db::graph::views::filter::model::filter_operator::FilterOperator, prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::core::entities::{
    properties::prop::{PropError, PropType},
    GID,
};
use raphtory_core::{
    entities::{
        graph::{logical_to_physical::InvalidNodeId, tgraph::InvalidLayer},
        properties::props::{MetadataError, TPropError},
    },
    utils::time::ParseTimeError,
};
use raphtory_storage::mutation::MutationError;
use std::{
    fmt::Debug,
    io,
    path::{PathBuf, StripPrefixError},
    time::SystemTimeError,
};
use tracing::error;

#[cfg(feature = "io")]
use parquet::errors::ParquetError;

#[cfg(feature = "storage")]
use pometry_storage::RAError;
#[cfg(feature = "arrow")]
use {
    polars_arrow::{datatypes::ArrowDataType, legacy::error},
    raphtory_api::core::entities::{properties::prop::DeserialisationError, GidType, VID},
};

#[cfg(feature = "python")]
use pyo3::PyErr;

#[cfg(feature = "search")]
use {tantivy, tantivy::query::QueryParserError};

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
    #[error("The give path does not exist: {0}")]
    PathDoesNotExist(PathBuf),
    #[error("Could not parse Path: {0}")]
    PathNotParsable(PathBuf),
    #[error("The path to the graph contains a subpath to an existing graph: {0}")]
    ParentIsGraph(PathBuf),
    #[error("The path provided does not exists as a namespace: {0}")]
    NamespaceDoesNotExist(String),
    #[error("The path provided contains non-UTF8 characters.")]
    NonUTFCharacters,
    #[error("Failed to strip prefix")]
    StripPrefix {
        #[from]
        source: StripPrefixError,
    },
}

#[cfg(feature = "arrow")]
#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("Only str columns are supported for layers, got {0:?}")]
    InvalidLayerType(ArrowDataType),
    #[error("Only str columns are supported for node type, got {0:?}")]
    InvalidNodeType(ArrowDataType),
    #[error("{0:?} not supported as property type")]
    InvalidPropertyType(ArrowDataType),
    #[error("{0:?} not supported as node id type")]
    InvalidNodeIdType(ArrowDataType),
    #[error("{0:?} not supported for time column")]
    InvalidTimestamp(ArrowDataType),
    #[error("Missing value for src id")]
    MissingSrcError,
    #[error("Missing value for dst id")]
    MissingDstError,
    #[error("Missing value for node id")]
    MissingNodeError,
    #[error("Missing value for timestamp")]
    MissingTimeError,
    #[error("Missing value for edge id {0:?} -> {1:?}")]
    MissingEdgeError(VID, VID),
    #[error("Node IDs have the wrong type, expected {existing}, got {new}")]
    NodeIdTypeError { existing: GidType, new: GidType },
    #[error("Fatal load error, graph may be in a dirty state.")]
    FatalError,
}

#[cfg(feature = "proto")]
#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[cfg(feature = "proto")]
    #[error("Unrecoverable disk error: {0}, resetting file size failed: {1}")]
    FatalWriteError(io::Error, io::Error),

    #[cfg(feature = "proto")]
    #[error("Failed to write delta to cache: {0}")]
    WriteError(#[from] io::Error),
}

pub type GraphResult<T> = Result<T, GraphError>;

pub fn into_graph_err(err: impl Into<GraphError>) -> GraphError {
    err.into()
}

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error(transparent)]
    MutationError(#[from] MutationError),

    #[error(transparent)]
    PropError(#[from] PropError),

    #[error("You cannot set ‘{0}’ and ‘{1}’ at the same time. Please pick one or the other.")]
    WrongNumOfArgs(String, String),

    #[cfg(feature = "arrow")]
    #[error("Arrow error: {0}")]
    Arrow(#[from] error::PolarsError),

    #[error("Arrow-rs error: {0}")]
    ArrowRs(#[from] arrow_schema::ArrowError),

    #[cfg(feature = "io")]
    #[error("Arrow-rs parquet error: {0}")]
    ParquetError(#[from] ParquetError),

    #[error("Invalid path: {source}")]
    InvalidPath {
        #[from]
        source: InvalidPathReason,
    },

    #[cfg(feature = "arrow")]
    #[error("{source}")]
    LoadError {
        #[from]
        source: LoadError,
    },
    #[error("Storage feature not enabled")]
    DiskGraphNotFound,

    #[error("Missing graph index. You need to create an index first.")]
    IndexNotCreated,

    #[error("Failed to create index.")]
    FailedToCreateIndex,

    #[error("Failed to persist index.")]
    FailedToPersistIndex,

    #[error("Cannot persist RAM index")]
    CannotPersistRamIndex,

    #[error("Failed to remove existing graph index: {0}")]
    FailedToRemoveExistingGraphIndex(PathBuf),

    #[error("Failed to move graph index")]
    FailedToMoveGraphIndex,

    #[error("Valid view is not supported for event graph")]
    EventGraphNoValidView,

    #[error("Graph not found {0}")]
    GraphNotFound(PathBuf),

    #[error("Graph already exists by name = {0}")]
    GraphNameAlreadyExists(PathBuf),

    #[error("{reason}")]
    InvalidProperty { reason: String },

    #[error("Failed to parse time string: {source}")]
    ParseTime {
        #[from]
        source: ParseTimeError,
    },

    #[error("Node already exists with ID {0:?}")]
    NodeExistsError(GID),

    #[error("Nodes already exist with IDs: {0:?}")]
    NodesExistError(Vec<GID>),

    #[error("Edge already exists for nodes {0:?} {1:?}")]
    EdgeExistsError(GID, GID),

    #[error("Edges already exist with IDs: {0:?}")]
    EdgesExistError(Vec<(GID, GID)>),

    #[error("Node {0} does not exist")]
    NodeMissingError(GID),

    #[error("Node Type Error {0}")]
    NodeTypeError(String),

    #[error("No Edge between {src} and {dst}")]
    EdgeMissingError { src: GID, dst: GID },

    #[error("Property {0} does not exist")]
    PropertyMissingError(String),
    // wasm
    #[error(transparent)]
    InvalidLayer(#[from] InvalidLayer),

    #[error("Graph does not have a default layer. Valid layers: {valid_layers}")]
    NoDefaultLayer { valid_layers: String },

    #[error("Layer {layer} does not exist for edge ({src}, {dst})")]
    InvalidEdgeLayer {
        layer: String,
        src: String,
        dst: String,
    },
    #[error("The loaded graph is of the wrong type. Did you mean Graph / PersistentGraph?")]
    GraphLoadError,

    #[error("IO operation failed")]
    IOError {
        #[from]
        source: io::Error,
    },

    #[error("IO operation failed: {0}")]
    IOErrorMsg(String),

    #[cfg(feature = "vectors")]
    #[error("Arroy error: {0}")]
    ArroyError(#[from] arroy::Error),

    #[cfg(feature = "vectors")]
    #[error("Heed error: {0}")]
    HeedError(#[from] heed::Error),

    #[cfg(feature = "vectors")]
    #[error("The path {0} does not contain a vector DB")]
    VectorDbDoesntExist(String),

    #[cfg(feature = "proto")]
    #[error("zip operation failed")]
    ZipError {
        #[from]
        source: zip::result::ZipError,
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
    #[error("Index operation failed: {source}")]
    IndexError {
        #[from]
        source: tantivy::TantivyError,
    },

    #[cfg(feature = "search")]
    #[error("Index operation failed: {0}")]
    IndexErrorMsg(String),

    #[cfg(feature = "vectors")]
    #[error("Embedding operation failed")]
    EmbeddingError {
        #[from]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[cfg(feature = "search")]
    #[error("Index operation failed")]
    QueryError {
        #[from]
        source: QueryParserError,
    },

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
    #[error(
        "Cannot recover from write failure {write_err}, new updates are invalid: {decode_err}"
    )]
    FatalDecodeError {
        write_err: WriteError,
        decode_err: prost::DecodeError,
    },

    #[cfg(feature = "proto")]
    #[error("Cache write error: {0}")]
    CacheWriteError(#[from] WriteError),

    #[cfg(feature = "proto")]
    #[error("Protobuf decode error{0}")]
    EncodeError(#[from] prost::EncodeError),

    #[cfg(feature = "proto")]
    #[error("Cannot write graph into non empty folder {0}")]
    NonEmptyGraphFolder(PathBuf),

    #[cfg(feature = "arrow")]
    #[error(transparent)]
    DeserialisationError(#[from] DeserialisationError),

    #[cfg(feature = "proto")]
    #[error("Cache is not initialised")]
    CacheNotInnitialised,

    #[error("Immutable graph is .. immutable!")]
    AttemptToMutateImmutableGraph,

    #[cfg(feature = "python")]
    #[error("Python error occurred: {0}")]
    PythonError(#[from] PyErr),

    #[error("An error with Tdqm occurred")]
    TqdmError,

    #[error("An error when parsing Jinja query templates: {0}")]
    JinjaError(String),

    #[error("An error when parsing the data to json: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),

    #[error("Property filtering not implemented on PersistentGraph yet")]
    PropertyFilteringNotImplemented,

    #[error("Expected a {0} for {1} operator")]
    ExpectedValueForOperator(String, String),

    #[error("Unsupported: Cannot convert {0} to ArrowDataType ")]
    UnsupportedArrowDataType(PropType),

    #[error("Not supported")]
    NotSupported,

    #[error("Operator {0} requires a property value, but none was provided.")]
    InvalidFilterExpectSingleGotNone(FilterOperator),

    #[error("Operator {0} requires a single value, but a set was provided.")]
    InvalidFilterExpectSingleGotSet(FilterOperator),

    #[error("Comparison not implemented for {0}")]
    InvalidFilterCmp(PropType),

    #[error("Expected a homogeneous map with inner type {0}, got {1}")]
    InvalidHomogeneousMap(PropType, PropType),

    #[error("Operator {0} requires a set of values, but a single value was provided.")]
    InvalidFilterExpectSetGotSingle(FilterOperator),

    #[error("Operator {0} requires a set of values, but none was provided.")]
    InvalidFilterExpectSetGotNone(FilterOperator),

    #[error("Operator {0} is only supported for strings.")]
    InvalidContains(FilterOperator),

    #[error("Invalid filter: {0}")]
    InvalidGqlFilter(String),

    #[error("Property {0} not found in temporal or metadata")]
    PropertyNotFound(String),

    #[error("PropertyIndex not found for property {0}")]
    PropertyIndexNotFound(String),

    #[error("Tokenization is support only for str field type")]
    UnsupportedFieldTypeForTokenization,

    #[error("Not tokens found")]
    NoTokensFound,

    #[error("More than one view set within a ViewCollection object - due to limitations in graphql we cannot tell which order to execute these in. Please add these views as individual objects in the order you want them to execute.")]
    TooManyViewsSet,

    #[error("Invalid Value conversion")]
    InvalidValueConversion,

    #[error("Unsupported Value: {0}")]
    UnsupportedValue(String),

    #[error("Value cannot be empty.")]
    EmptyValue,

    #[error("Filter must contain at least one filter condition.")]
    ParsingError,

    #[error("Node filter is not supported for edge filtering")]
    NodeFilterIsNotEdgeFilter,

    #[error("Only property filters are supported for exploded edge filtering")]
    NotExplodedEdgeFilter,

    #[error("Indexing not supported")]
    IndexingNotSupported,

    #[error("Failed to create index in ram. There already exists an on disk index.")]
    OnDiskIndexAlreadyExists,

    #[error("Your window and step must be of the same type: duration (string) or epoch (int)")]
    MismatchedIntervalTypes,

    #[error("Cannot initialize cache for zipped graph. Unzip the graph to initialize the cache.")]
    ZippedGraphCannotBeCached,
}

impl From<MetadataError> for GraphError {
    fn from(value: MetadataError) -> Self {
        Self::MutationError(value.into())
    }
}

impl From<TPropError> for GraphError {
    fn from(value: TPropError) -> Self {
        Self::MutationError(value.into())
    }
}

impl From<InvalidNodeId> for GraphError {
    fn from(value: InvalidNodeId) -> Self {
        Self::MutationError(value.into())
    }
}

impl GraphError {
    pub fn no_default_layer<'graph>(graph: impl GraphViewOps<'graph>) -> Self {
        let valid_layers = graph.unique_layers().join(", ");
        GraphError::NoDefaultLayer { valid_layers }
    }
}

impl<A: Debug> From<IllegalSet<A>> for GraphError {
    fn from(value: IllegalSet<A>) -> Self {
        Self::IllegalSet(value.to_string())
    }
}

impl From<GraphError> for io::Error {
    fn from(error: GraphError) -> Self {
        io::Error::other(error)
    }
}
