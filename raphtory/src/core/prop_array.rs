use crate::core::{utils::errors::GraphError, Prop};
use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use raphtory_api::iter::{BoxedLIter, IntoDynBoxed};
use serde::{Deserialize, Serialize, Serializer};
use std::hash::{Hash, Hasher};

#[derive(Default, Debug, Clone)]
pub enum PropArray {
    #[default]
    Empty,
    Array(ArrayRef),
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

    pub fn from_vec_u8(bytes: &[u8]) -> Result<Self, GraphError> {
        if bytes.is_empty() {
            return Ok(PropArray::Empty);
        }
        let mut reader = StreamReader::try_new(bytes, None)?;
        let rb = reader.next().ok_or(GraphError::DeserialisationError(
            "failed to deserialize ArrayRef".to_string(),
        ))??;
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
