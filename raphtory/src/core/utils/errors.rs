use crate::core::storage::lazy_vec::IllegalSet;
use crate::core::utils::time::error::ParseTimeError;

use crate::core::Prop;

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("Immutable graph reference already exists. You can access mutable graph apis only exclusively.")]
    IllegalGraphAccess,
    #[error("Incorrect property given.")]
    IncorrectPropertyType,
    #[error("Failed to mutate graph")]
    FailedToMutateGraph { source: MutateGraphError },
    #[error("Failed to mutate graph property")]
    FailedToMutateGraphProperty { source: MutateGraphError },
    #[error("Failed to parse time string")]
    ParseTime {
        #[from]
        source: ParseTimeError,
    },
    // wasm
    #[error("Vertex is not String or Number")]
    VertexIdNotStringOrNumber,
    #[error("Invalid layer.")]
    InvalidLayer,
    #[error("Bincode operation failed")]
    BinCodeError { source: Box<bincode::ErrorKind> },
    #[error("IO operation failed")]
    IOError { source: std::io::Error },
}

impl From<bincode::Error> for GraphError {
    fn from(source: bincode::Error) -> Self {
        GraphError::BinCodeError { source }
    }
}

impl From<std::io::Error> for GraphError {
    fn from(source: std::io::Error) -> Self {
        GraphError::IOError { source }
    }
}

impl From<MutateGraphError> for GraphError {
    fn from(source: MutateGraphError) -> Self {
        GraphError::FailedToMutateGraph { source }
    }
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
    #[error("Failed to update graph property")]
    IllegalGraphPropertyChange { source: IllegalMutate },
    #[error("Create edge '{0}' -> '{1}' first before adding static properties to it")]
    MissingEdge(u64, u64), // src, dst
    #[error("cannot change property for edge '{src_id}' -> '{dst_id}'")]
    IllegalEdgePropertyChange {
        src_id: u64,
        dst_id: u64,
        source: IllegalMutate,
    },
    #[error("cannot update property as is '{first_type}' and '{second_type}' given'")]
    PropertyChangedType {
        first_type: &'static str,
        second_type: &'static str,
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
