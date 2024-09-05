use crate::{
    core::utils::errors::LoadError,
    db::api::{
        mutation::internal::InternalAdditionOps,
        view::{BoxedLIter, IntoDynBoxed},
    },
    io::arrow::dataframe::DFChunk,
};
use polars_arrow::{
    array::{Array, PrimitiveArray, StaticArray, Utf8Array},
    datatypes::ArrowDataType,
    offset::Offset,
};
use raphtory_api::core::entities::{GidRef, GidType};
use rayon::prelude::{IndexedParallelIterator, *};

trait NodeColOps: Array + Send + Sync {
    fn has_missing_values(&self) -> bool {
        self.null_count() != 0
    }
    fn get(&self, i: usize) -> Option<GidRef>;

    fn dtype(&self) -> GidType;
}

impl NodeColOps for PrimitiveArray<u64> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(GidRef::U64)
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
}

impl NodeColOps for PrimitiveArray<u32> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(|v| GidRef::U64(v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
}

impl NodeColOps for PrimitiveArray<i64> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(|v| GidRef::U64(v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
    }
}

impl NodeColOps for PrimitiveArray<i32> {
    fn get(&self, i: usize) -> Option<GidRef> {
        StaticArray::get(self, i).map(|v| GidRef::U64(v as u64))
    }

    fn dtype(&self) -> GidType {
        GidType::U64
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

impl NodeCol {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = Option<GidRef<'_>>> + '_ {
        (0..self.0.len()).into_par_iter().map(|i| self.0.get(i))
    }

    pub fn iter(&self) -> impl Iterator<Item = GidRef> + '_ {
        (0..self.0.len()).map(|i| self.0.get(i).unwrap())
    }

    pub fn validate(
        &self,
        graph: &impl InternalAdditionOps,
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
