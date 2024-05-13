use crate::arrow2::{array::Arrow2Arrow, datatypes::ArrowDataType, types::NativeType};
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

fn arrow2_to_arrow_buf<U: ArrowPrimitiveType>(
    buffer: &crate::arrow2::buffer::Buffer<U::Native>,
) -> arrow::array::PrimitiveArray<U>
where
    U::Native: NativeType,
{
    let dt = ArrowDataType::from(<U::Native as crate::arrow2::types::NativeType>::PRIMITIVE);
    let prim_array = crate::arrow2::array::PrimitiveArray::new(dt, buffer.clone(), None);
    let data = prim_array.to_data();
    arrow::array::PrimitiveArray::from(data)
}
