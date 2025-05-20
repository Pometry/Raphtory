use crate::{
    core::entities::properties::prop::{Prop, PropType},
    iter::{BoxedLIter, IntoDynBoxed},
};
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{ArrowError, Field, Schema};
use serde::{Deserialize, Serialize, Serializer};
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};
use thiserror::Error;

#[derive(Default, Debug, Clone)]
pub enum PropArray {
    #[default]
    Empty,
    Array(ArrayRef),
}

#[derive(Error)]
pub enum DeserialisationError {
    #[error("Failed to deserialize ArrayRef")]
    DeserialisationError,
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

impl Hash for PropArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let PropArray::Array(array) = self {
            let data = array.to_data();
            let dtype = array.data_type();
            dtype.hash(state);
            data.offset().hash(state);
            data.len().hash(state);
            for buffer in data.buffers() {
                buffer.hash(state);
            }
        } else {
            PropArray::Empty.hash(state);
        }
    }
}

impl PropArray {
    pub fn len(&self) -> usize {
        match self {
            PropArray::Array(arr) => arr.len(),
            PropArray::Empty => 0,
        }
    }

    pub fn dtype(&self) -> PropType {
        match self {
            PropArray::Empty => PropType::Empty,
            PropArray::Array(a) => PropType::from(a.data_type()),
        }
    }

    pub fn to_vec_u8(&self) -> Vec<u8> {
        // assuming we can allocate this can't fail
        let mut bytes = vec![];
        if let PropArray::Array(value) = self {
            let schema = Schema::new(vec![Field::new("data", value.data_type().clone(), true)]);
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            let rb = RecordBatch::try_new(schema.into(), vec![value.clone()]).unwrap();
            writer.write(&rb).unwrap();
            writer.finish().unwrap();
        }
        bytes
    }

    pub fn from_vec_u8(bytes: &[u8]) -> Result<Self, DeserialisationError> {
        if bytes.is_empty() {
            return Ok(PropArray::Empty);
        }
        let mut reader = StreamReader::try_new(bytes, None)?;
        let rb = reader
            .next()
            .ok_or(DeserialisationError::DeserialisationError)??;
        Ok(PropArray::Array(rb.column(0).clone()))
    }

    pub fn into_array_ref(self) -> Option<ArrayRef> {
        match self {
            PropArray::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn as_array_ref(&self) -> Option<&ArrayRef> {
        match self {
            PropArray::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn iter_prop(&self) -> impl Iterator<Item = Prop> + '_ {
        self.iter_prop_inner().into_iter().flatten()
    }

    fn iter_prop_inner(&self) -> Option<BoxedLIter<Prop>> {
        let arr = self.as_array_ref()?;

        arr.as_any()
            .downcast_ref::<PrimitiveArray<arrow_array::types::Int32Type>>()
            .map(|arr| {
                arr.into_iter()
                    .map(|v| Prop::I32(v.unwrap_or_default()))
                    .into_dyn_boxed()
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::Float64Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::F64(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::Float32Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::F32(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::UInt64Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::U64(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::UInt32Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::U32(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::Int64Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::I64(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::UInt16Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::U16(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
            .or_else(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<arrow_array::types::UInt8Type>>()
                    .map(|arr| {
                        arr.into_iter()
                            .map(|v| Prop::U8(v.unwrap_or_default()))
                            .into_dyn_boxed()
                    })
            })
    }
}

impl Serialize for PropArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_vec_u8();
        bytes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PropArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        PropArray::from_vec_u8(&bytes).map_err(serde::de::Error::custom)
    }
}

impl PartialEq for PropArray {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PropArray::Empty, PropArray::Empty) => true,
            (PropArray::Array(a), PropArray::Array(b)) => a.eq(b),
            _ => false,
        }
    }
}

impl Prop {
    pub fn from_arr<TT: ArrowPrimitiveType>(vals: Vec<TT::Native>) -> Self
    where
        arrow_array::PrimitiveArray<TT>: From<Vec<TT::Native>>,
    {
        let array = arrow_array::PrimitiveArray::<TT>::from(vals);
        Prop::Array(PropArray::Array(Arc::new(array)))
    }
}

pub fn arrow_dtype_from_prop_type(prop_type: &PropType) -> Result<DataType, GraphError> {
    match prop_type {
        PropType::Str => Ok(DataType::LargeUtf8),
        PropType::U8 => Ok(DataType::UInt8),
        PropType::U16 => Ok(DataType::UInt16),
        PropType::I32 => Ok(DataType::Int32),
        PropType::I64 => Ok(DataType::Int64),
        PropType::U32 => Ok(DataType::UInt32),
        PropType::U64 => Ok(DataType::UInt64),
        PropType::F32 => Ok(DataType::Float32),
        PropType::F64 => Ok(DataType::Float64),
        PropType::Bool => Ok(DataType::Boolean),
        PropType::NDTime => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            None,
        )),
        PropType::DTime => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            Some("UTC".into()),
        )),
        PropType::Array(d_type) => Ok(DataType::List(
            Field::new("data", arrow_dtype_from_prop_type(&d_type)?, true).into(),
        )),

        PropType::List(d_type) => Ok(DataType::List(
            Field::new("data", arrow_dtype_from_prop_type(&d_type)?, true).into(),
        )),
        PropType::Map(d_type) => {
            let fields = d_type
                .iter()
                .map(|(k, v)| {
                    Ok::<_, GraphError>(Field::new(
                        k.to_string(),
                        arrow_dtype_from_prop_type(v)?,
                        true,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        // 38 comes from herehttps://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html
        PropType::Decimal { scale } => Ok(DataType::Decimal128(38, (*scale).try_into().unwrap())),
        PropType::Empty => {
            // this is odd, we'll just pick one and hope for the best
            Ok(DataType::Null)
        }
    }
}

pub fn prop_type_from_arrow_dtype(arrow_dtype: &DataType) -> PropType {
    match arrow_dtype {
        DataType::LargeUtf8 | DataType::Utf8 | DataType::Utf8View => PropType::Str,
        DataType::UInt8 => PropType::U8,
        DataType::UInt16 => PropType::U16,
        DataType::Int32 => PropType::I32,
        DataType::Int64 => PropType::I64,
        DataType::UInt32 => PropType::U32,
        DataType::UInt64 => PropType::U64,
        DataType::Float32 => PropType::F32,
        DataType::Float64 => PropType::F64,
        DataType::Boolean => PropType::Bool,
        DataType::Decimal128(_, scale) => PropType::Decimal {
            scale: *scale as i64,
        },
        DataType::List(field) => {
            let d_type = field.data_type();
            PropType::Array(Box::new(prop_type_from_arrow_dtype(&d_type)))
        }
        _ => panic!("{:?} not supported as disk_graph property", arrow_dtype),
    }
}

pub trait PropArrayUnwrap {
    fn into_array(self) -> Option<ArrayRef>;
    fn unwrap_array(self) -> ArrayRef {
        self.into_array().unwrap()
    }
}

impl<P: PropArrayUnwrap> PropArrayUnwrap for Option<P> {
    fn into_array(self) -> Option<ArrayRef> {
        self.and_then(|p| p.into_array())
    }
}

impl PropArrayUnwrap for Prop {
    fn into_array(self) -> Option<ArrayRef> {
        if let Prop::Array(v) = self {
            v.into_array_ref()
        } else {
            None
        }
    }
}
