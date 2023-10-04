use crate::core::{
    storage::lazy_vec::IllegalSet, utils::time::error::ParseTimeError, ArcStr, Prop, PropType,
};
#[cfg(feature = "search")]
use tantivy;
#[cfg(feature = "search")]
use tantivy::query::QueryParserError;

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
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

    #[error("No Vertex with ID {0}")]
    VertexIdError(u64),

    #[error("No Vertex with name {0}")]
    VertexNameError(String),

    #[error("No Edge between {src} and {dst}")]
    EdgeIdError { src: u64, dst: u64 },

    #[error("No Edge between {src} and {dst}")]
    EdgeNameError { src: String, dst: String },
    // wasm
    #[error("Vertex is not String or Number")]
    VertexIdNotStringOrNumber,
    #[error("Invalid layer {0}.")]
    InvalidLayer(String),
    #[error("Bincode operation failed")]
    BinCodeError {
        #[from]
        source: Box<bincode::ErrorKind>,
    },

    #[error("The loaded graph is of the wrong kind")]
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
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum MutateGraphError {
    #[error("Create vertex '{vertex_id}' first before adding static properties to it")]
    VertexNotFoundError { vertex_id: u64 },
    #[error("Unable to find layer '{layer_name}' to add property to")]
    LayerNotFoundError { layer_name: String },
    #[error("cannot change property for vertex '{vertex_id}'")]
    IllegalVertexPropertyChange {
        vertex_id: u64,
        source: IllegalMutate,
    },
    #[error("Tried to change constant graph property {name}, old value: {old_value}, new value: {new_value}")]
    IllegalGraphPropertyChange {
        name: String,
        old_value: Prop,
        new_value: Prop,
    },
    #[error("Create edge '{0}' -> '{1}' first before adding static properties to it")]
    MissingEdge(u64, u64), // src, dst
    #[error("cannot change property for edge '{src_id}' -> '{dst_id}'")]
    IllegalEdgePropertyChange {
        src_id: u64,
        dst_id: u64,
        source: IllegalMutate,
    },
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("cannot mutate static property '{name}'")]
pub struct IllegalMutate {
    pub name: String,
    pub source: IllegalSet<Option<Prop>>,
}

impl IllegalMutate {
    pub(crate) fn from_source(source: IllegalSet<Option<Prop>>, prop: &str) -> IllegalMutate {
        IllegalMutate {
            name: prop.to_string(),
            source,
        }
    }
}
