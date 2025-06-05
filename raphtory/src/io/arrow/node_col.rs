use crate::{errors::LoadError, io::arrow::dataframe::DFChunk, prelude::AdditionOps};
use arrow_array::{Array as ArrowArray, Int32Array, Int64Array};
use polars_arrow::{
    array::{Array, PrimitiveArray, StaticArray, Utf8Array},
    datatypes::ArrowDataType,
    offset::Offset,
};
use raphtory_api::core::entities::{GidRef, GidType};
use rayon::prelude::{IndexedParallelIterator, *};

trait NodeColOps: Send + Sync {
    fn has_missing_values(&self) -> bool {
        self.null_count() != 0
    }
    fn get(&self, i: usize) -> Option<GidRef>;

    fn dtype(&self) -> GidType;

    fn null_count(&self) -> usize;

    fn len(&self) -> usize;
}

impl NodeColOps for PrimitiveArray<u64> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(GidRef::U64)
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }

    fn null_count(&self) -> usize {
        Array::null_count(self)
    }
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for PrimitiveArray<u32> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(|v| GidRef::U64(v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }

    fn null_count(&self) -> usize {
        Array::null_count(self)
    }
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for PrimitiveArray<i64> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(|v| GidRef::U64(v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
    fn null_count(&self) -> usize {
        Array::null_count(self)
    }
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for PrimitiveArray<i32> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(|v| GidRef::U64(v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
    fn null_count(&self) -> usize {
        Array::null_count(self)
    }
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl<O: Offset> NodeColOps for Utf8Array<O> {
    fn get(&self, i: usize) -> Option<GidRef> {
        if i >= self.len() {
            None
        } else {
            // safety: bounds checked above
            unsafe {
                if self.is_null_unchecked(i) {
                    None
                } else {
                    let value = self.value_unchecked(i);
                    Some(GidRef::Str(value))
                }
            }
        }
    }
    fn dtype(&self) -> GidType {
        GidType::Str
    }

    fn null_count(&self) -> usize {
        Array::null_count(self)
    }

    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for Int32Array {
    fn get(&self, i: usize) -> Option<GidRef> {
        self.values().get(i).map(|v| GidRef::U64(*v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }
    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

impl NodeColOps for Int64Array {
    fn get(&self, i: usize) -> Option<GidRef> {
        self.values().get(i).map(|v| GidRef::U64(*v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }
    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

impl NodeColOps for arrow_array::StringArray {
    fn get(&self, i: usize) -> Option<GidRef> {
        if i >= ArrowArray::len(self) {
            None
        } else {
            // safety: bounds checked above
            unsafe {
                let value = self.value_unchecked(i);
                Some(GidRef::Str(value))
            }
        }
    }

    fn dtype(&self) -> GidType {
        GidType::Str
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }
    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

impl NodeColOps for arrow_array::LargeStringArray {
    fn get(&self, i: usize) -> Option<GidRef> {
        if i >= ArrowArray::len(self) {
            None
        } else {
            // safety: bounds checked above
            unsafe {
                let value = self.value_unchecked(i);
                Some(GidRef::Str(value))
            }
        }
    }

    fn dtype(&self) -> GidType {
        GidType::Str
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }

    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

impl NodeColOps for arrow_array::StringViewArray {
    fn get(&self, i: usize) -> Option<GidRef> {
        if i >= ArrowArray::len(self) {
            None
        } else {
            // safety: bounds checked above
            unsafe {
                let value = self.value_unchecked(i);
                Some(GidRef::Str(value))
            }
        }
    }

    fn dtype(&self) -> GidType {
        GidType::Str
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }
    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

impl NodeColOps for arrow_array::UInt32Array {
    fn get(&self, i: usize) -> Option<GidRef> {
        self.values().get(i).map(|v| GidRef::U64(*v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }
    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

impl NodeColOps for arrow_array::UInt64Array {
    fn get(&self, i: usize) -> Option<GidRef> {
        self.values().get(i).map(|v| GidRef::U64(*v))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
    fn null_count(&self) -> usize {
        ArrowArray::null_count(self)
    }
    fn len(&self) -> usize {
        ArrowArray::len(self)
    }
}

pub struct NodeCol(Box<dyn NodeColOps>);

impl<'a> TryFrom<&'a dyn Array> for NodeCol {
    type Error = LoadError;

    fn try_from(value: &'a dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            ArrowDataType::Int32 => {
                let col = value
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            ArrowDataType::Int64 => {
                let col = value
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            ArrowDataType::UInt32 => {
                let col = value
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u32>>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            ArrowDataType::UInt64 => {
                let col = value
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            ArrowDataType::Utf8 => {
                let col = value
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            ArrowDataType::LargeUtf8 => {
                let col = value
                    .as_any()
                    .downcast_ref::<Utf8Array<i64>>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            dtype => Err(LoadError::InvalidNodeIdType(dtype.clone())),
        }
    }
}

impl<'a> TryFrom<&'a dyn arrow_array::Array> for NodeCol {
    type Error = LoadError;

    fn try_from(value: &'a dyn arrow_array::Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            arrow_schema::DataType::Int32 => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            arrow_schema::DataType::Int64 => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            arrow_schema::DataType::UInt32 => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::UInt32Array>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            arrow_schema::DataType::UInt64 => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            arrow_schema::DataType::Utf8 => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            arrow_schema::DataType::LargeUtf8 => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            arrow_schema::DataType::Utf8View => {
                let col = value
                    .as_any()
                    .downcast_ref::<arrow_array::StringViewArray>()
                    .unwrap()
                    .clone();
                Ok(NodeCol(Box::new(col)))
            }
            dtype => Err(LoadError::ArrowDataType(dtype.clone())),
        }
    }
}

impl NodeCol {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = Option<GidRef<'_>>> + '_ {
        (0..self.0.len()).into_par_iter().map(|i| self.0.get(i))
    }

    pub fn iter(&self) -> impl Iterator<Item = GidRef> + '_ {
        (0..self.0.len()).map(|i| self.0.get(i).unwrap())
    }

    pub fn validate(
        &self,
        graph: &impl AdditionOps,
        node_missing_error: LoadError,
    ) -> Result<(), LoadError> {
        if let Some(existing) = graph.id_type().filter(|&id_type| id_type != self.0.dtype()) {
            return Err(LoadError::NodeIdTypeError {
                existing,
                new: self.0.dtype(),
            });
        }
        if self.0.has_missing_values() {
            return Err(node_missing_error);
        }
        Ok(())
    }
}

pub fn lift_node_col(index: usize, df: &DFChunk) -> Result<NodeCol, LoadError> {
    (df.chunk[index].as_ref()).try_into()
}
