use polars_arrow::legacy::error;
use crate::core::{utils::time::error::ParseTimeError, ArcStr, Prop, PropType};
#[cfg(feature = "search")]
use tantivy;
#[cfg(feature = "search")]
use tantivy::query::QueryParserError;

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] error::PolarsError),
    #[error("Graph error occurred")]
    UnsupportedDataType,
    #[error("Graph already exists by name = {name}")]
    GraphNameAlreadyExists { name: String },
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

    #[error("Node already exists with ID {0}")]
    NodeExistsError(u64),

    #[error("Edge already exists for nodes {0} {1}")]
    EdgeExistsError(u64, u64),

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
    #[error("Invalid layer {0}.")]
    InvalidLayer(String),
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

    #[cfg(feature = "python")]
    #[error("Failed to load graph: {0}")]
    LoadFailure(String),

    #[cfg(feature = "python")]
    #[error(
        "Failed to load graph as the following columns are not present within the dataframe: {0}"
    )]
    ColumnDoesNotExist(String),

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
        "Failed to load the graph as the bincode version {0} is different to installed version {1}"
    )]
    BincodeVersionError(u32, u32),

    #[error("The layer_name function is only available once an edge has been exploded via .explode_layers() or .explode(). If you want to retrieve the layers for this edge you can use .layer_names")]
    LayerNameAPIError,

    #[error("The time function is only available once an edge has been exploded via .explode(). You may want to retrieve the history for this edge via .history(), or the earliest/latest time via earliest_time or latest_time")]
    TimeAPIError,
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
}
