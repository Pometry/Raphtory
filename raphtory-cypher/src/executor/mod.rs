use arrow::datatypes::ArrowPrimitiveType;
use arrow2::{array::Arrow2Arrow, types::NativeType};
use arrow_array::{GenericStringArray, OffsetSizeTrait};

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
}

fn arrow2_to_arrow_buf<U: ArrowPrimitiveType>(
    buffer: &arrow2::buffer::Buffer<U::Native>,
) -> arrow::array::PrimitiveArray<U>
where
    U::Native: NativeType,
{
    let dt = arrow2::datatypes::DataType::from(<U::Native as arrow2::types::NativeType>::PRIMITIVE);
    let prim_array = arrow2::array::PrimitiveArray::new(dt, buffer.clone(), None);
    prim_array.to_data().into()
}

fn arrow2_to_arrow<U: ArrowPrimitiveType>(
    array: &arrow2::array::PrimitiveArray<U::Native>,
) -> arrow::array::PrimitiveArray<U>
where
    U::Native: NativeType,
{
    array.to_data().into()
}

fn utf8_arrow2_to_arrow<I: arrow2::offset::Offset + OffsetSizeTrait>(
    array: &arrow2::array::Utf8Array<I>,
) -> GenericStringArray<I>
where
    I: arrow2::offset::Offset,
{
    array.to_data().into()
}
