//! A data structure for representing temporal graphs.

use self::errors::MutateGraphError;

pub(crate) mod errors {
    use crate::core::props::IllegalMutate;

    #[derive(thiserror::Error, Debug, PartialEq)]
    pub enum MutateGraphError {
        #[error("Create vertex '{vertex_id}' first before adding static properties to it")]
        VertexNotFoundError { vertex_id: u64 },
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
}

pub type MutateGraphResult = Result<(), MutateGraphError>;
