use crate::{errors::LoadError, io::arrow::dataframe::DFChunk, prelude::AdditionOps};
use arrow::{
    array::{
        Array, AsArray, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Int32Type, Int64Type, UInt32Type, UInt64Type},
};
use raphtory_api::core::entities::{GidRef, GidType};
use rayon::prelude::{IndexedParallelIterator, *};

trait NodeColOps: Send + Sync {
    fn has_missing_values(&self) -> bool {
        self.null_count() != 0
    }
    fn get(&self, i: usize) -> Option<GidRef<'_>>;

    fn dtype(&self) -> GidType;

    fn null_count(&self) -> usize;

    fn len(&self) -> usize;
}

impl NodeColOps for Int32Array {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        self.values().get(i).map(|v| GidRef::U64(*v as u64))
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

impl NodeColOps for Int64Array {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        self.values().get(i).map(|v| GidRef::U64(*v as u64))
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

impl NodeColOps for StringArray {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        if i >= Array::len(self) {
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
        Array::null_count(self)
    }
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for LargeStringArray {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        if i >= Array::len(self) {
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
        Array::null_count(self)
    }

    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for StringViewArray {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        if i >= Array::len(self) {
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
        Array::null_count(self)
    }
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl NodeColOps for UInt32Array {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        self.values().get(i).map(|v| GidRef::U64(*v as u64))
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

impl NodeColOps for UInt64Array {
    fn get(&self, i: usize) -> Option<GidRef<'_>> {
        self.values().get(i).map(|v| GidRef::U64(*v))
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

pub struct NodeCol(Box<dyn NodeColOps>);

impl<'a> TryFrom<&'a dyn Array> for NodeCol {
    type Error = LoadError;

    fn try_from(value: &'a dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Int32 => {
                let col = value.as_primitive::<Int32Type>().clone();
                Ok(NodeCol(Box::new(col)))
            }
            DataType::Int64 => {
                let col = value.as_primitive::<Int64Type>().clone();
                Ok(NodeCol(Box::new(col)))
            }
            DataType::UInt32 => {
                let col = value.as_primitive::<UInt32Type>().clone();
                Ok(NodeCol(Box::new(col)))
            }
            DataType::UInt64 => {
                let col = value.as_primitive::<UInt64Type>().clone();
                Ok(NodeCol(Box::new(col)))
            }
            DataType::Utf8 => {
                let col = value.as_string::<i32>().clone();
                Ok(NodeCol(Box::new(col)))
            }
            DataType::LargeUtf8 => {
                let col = value.as_string::<i64>().clone();
                Ok(NodeCol(Box::new(col)))
            }
            DataType::Utf8View => {
                let col = value.as_string_view().clone();
                Ok(NodeCol(Box::new(col)))
            }
            dtype => Err(LoadError::InvalidNodeIdType(dtype.clone())),
        }
    }
}

impl NodeCol {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = Option<GidRef<'_>>> + '_ {
        (0..self.0.len()).into_par_iter().map(|i| self.0.get(i))
    }

    pub fn iter(&self) -> impl Iterator<Item = GidRef<'_>> + '_ {
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

    pub fn dtype(&self) -> GidType {
        self.0.dtype()
    }
}

pub fn lift_node_col(index: usize, df: &DFChunk) -> Result<NodeCol, LoadError> {
    (df.chunk[index].as_ref()).try_into()
}
