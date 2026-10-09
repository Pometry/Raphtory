use crate::embeddings::EmbeddingError;
use raphtory::errors::GraphError;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum VectorError {
    #[error(transparent)]
    GraphError(#[from] GraphError),

    #[error("Heed error: {0}")]
    HeedError(#[from] heed::Error),

    #[error("LanceDB error: {0}")]
    LanceDbError(#[from] lancedb::Error),

    #[error("The path {0} does not contain a vector DB")]
    VectorDbDoesntExist(String),

    #[error("The schema of the vector DB is invalid")]
    InvalidVectorDbSchema,

    #[error("Embedding operation failed")]
    EmbeddingError {
        #[from]
        source: EmbeddingError,
    },

    #[error("Model has not been initialised with a sample, so dimension cannot be inferred. Please provide a sample embedding when initializing the model, or set the dimension explicitly in the model config.")]
    UnresolvedModel,

    #[error(transparent)]
    PersistError(#[from] tempfile::PersistError),

    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),

    #[error(transparent)]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("The stored template or embedding model differs from the one requested, so only entities missing from the index cannot be added; re-vectorise instead")]
    VectorTemplateChanged,
}

pub type VectorResult<T> = Result<T, VectorError>;

impl From<std::io::Error> for VectorError {
    fn from(error: std::io::Error) -> Self {
        GraphError::from(error).into()
    }
}

/// Lets code that works in terms of [`GraphError`] (e.g. the GraphQL server) use `?` on
/// vector operations; non-graph errors are carried as [`GraphError::ExternalError`].
impl From<VectorError> for GraphError {
    fn from(error: VectorError) -> Self {
        match error {
            VectorError::GraphError(error) => error,
            other => GraphError::ExternalError(Arc::new(other)),
        }
    }
}

#[cfg(feature = "python")]
impl From<VectorError> for pyo3::PyErr {
    fn from(error: VectorError) -> Self {
        GraphError::from(error).into()
    }
}
