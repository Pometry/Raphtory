use arrow::datatypes::ArrowPrimitiveType;

pub(crate) mod table_provider;

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("Failed to create thread pool: {0}")]
    TPBuildErr(#[from] rayon::ThreadPoolBuildError),

    #[error("Layer not found: {0}")]
    LayerNotFound(String),

    #[error("Missing node properties : {0}")]
    MissingNodeProperties(String),

    #[error("Failed to execute plan: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("Arrow schema error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("Failed to parse cypher {0}")]
    CypherParseError(#[from] super::parser::ParseError),

    #[error("IO Failure {0}")]
    IOError(#[from] std::io::Error),
}
